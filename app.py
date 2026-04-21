"""
COMP5 Drawing Verification — Web Application
Flask backend for Render.com deployment
Wraps the core verification logic from comp5_verifier.py
"""

import os
import re
import json
import uuid
import zipfile
import tempfile
import threading
from io import BytesIO
from pathlib import Path
from datetime import datetime

from flask import (Flask, request, jsonify, render_template,
                   send_file, session)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "comp5-verify-2026")

# ── In-memory job store (resets on dyno restart — fine for free tier) ─────────
jobs = {}   # job_id → { status, progress, results, error }
jobs_lock = threading.Lock()

ALLOWED_EXTENSIONS = {".zip", ".xlsx", ".xls"}
MAX_CONTENT_LENGTH = 500 * 1024 * 1024   # 500 MB
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

# ── Import core verification functions ────────────────────────────────────────
# We import just the functions we need — not the PyQt5 GUI parts
import sys
sys.path.insert(0, os.path.dirname(__file__))

try:
    # Import only the pure-Python verification functions
    from verifier_core import (
        verify_pdf, collect_pdfs_from_zip, load_transmittal_excel
    )
    VERIFIER_OK = True
except ImportError as e:
    VERIFIER_OK = False
    IMPORT_ERROR = str(e)
except Exception as e:
    VERIFIER_OK = False
    IMPORT_ERROR = str(e)

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    info = {
        "status": "ok" if VERIFIER_OK else "degraded",
        "verifier": VERIFIER_OK,
        "time": datetime.utcnow().isoformat(),
    }
    if not VERIFIER_OK:
        info["error"] = IMPORT_ERROR
    return jsonify(info)


@app.route("/api/verify", methods=["POST"])
def start_verification():
    """
    Accept ZIP + Excel upload, start background verification.
    Returns job_id immediately.
    """
    if not VERIFIER_OK:
        return jsonify({"error": f"Verifier not loaded: {IMPORT_ERROR}"}), 500

    if "zip" not in request.files or "excel" not in request.files:
        return jsonify({"error": "Both ZIP and Excel files are required"}), 400

    zip_file   = request.files["zip"]
    excel_file = request.files["excel"]

    # Read into memory (no temp files on disk for security)
    zip_bytes   = zip_file.read()
    excel_bytes = excel_file.read()
    zip_name    = secure_filename(zip_file.filename)
    excel_name  = secure_filename(excel_file.filename)

    # Create job
    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status":   "running",
            "progress": 0,
            "total":    0,
            "log":      [],
            "results":  [],
            "zip_name": zip_name,
            "excel_name": excel_name,
            "started":  datetime.utcnow().isoformat(),
        }

    # Run verification in background thread
    thread = threading.Thread(
        target=_run_verification,
        args=(job_id, zip_bytes, excel_bytes),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id})


def _run_verification(job_id, zip_bytes, excel_bytes):
    """Background worker — runs verification and updates jobs dict."""

    def log(msg):
        with jobs_lock:
            jobs[job_id]["log"].append(msg)

    try:
        log("Parsing Excel transmittal...")
        transmittal = load_transmittal_excel(excel_bytes)
        log(f"Transmittal loaded: {len(transmittal)} drawings")

        log("Reading ZIP file (including nested ZIPs)...")
        pdf_entries = collect_pdfs_from_zip(zip_bytes)
        log(f"Found {len(pdf_entries)} PDF(s) in ZIP")

        # CPY-based matching
        _CPY_RE = re.compile(r"(\d{3}-\d{2}-[A-Z]+-[A-Z]+-\d{4,5})", re.IGNORECASE)
        def _cpy(fn):
            m = _CPY_RE.search(fn)
            return re.sub(r"[\s\-_]", "", m.group(1) if m else fn).upper()

        tmap = {re.sub(r"[\s\-_]","",r["cpyNo"]).upper(): r for r in transmittal if r.get("cpyNo")}
        matched = [(s, b, tmap[_cpy(s)]) for s, b in pdf_entries if _cpy(s) in tmap]
        missing = [r for r in transmittal if re.sub(r"[\s\-_]","",r["cpyNo"]).upper() not in {_cpy(s) for s,_ in pdf_entries}]

        total = len(matched) + len(missing)
        with jobs_lock:
            jobs[job_id]["total"] = total

        results = []
        for idx, (short, pdf_bytes_item, row) in enumerate(matched):
            log(f"[{idx+1}/{total}] Verifying: {short}")
            try:
                res = verify_pdf(pdf_bytes_item, short, row)
            except Exception as e:
                res = {
                    "filename": short, "overallResult": "FAIL",
                    "issues": f"Error: {e}",
                    **{k: row.get(k,"") for k in ["srNo","docNo","cpyNo","revision","title"]},
                    **{k: "FAIL" for k in ["docNoMatch","cpyNoMatch","revMatch",
                                            "sigsResult","commentsResult",
                                            "classificationResult","prevRevResult","titleMatch"]},
                    "sigCount": 0, "commentsCount": 0,
                    "classificationMissingPages": [], "docNoFromPdf": "",
                    "cpyNoFromPdf": "", "revFromPdf": "",
                }
            results.append(res)
            ov = res.get("overallResult","FAIL")
            log(f"  → {ov}")
            with jobs_lock:
                jobs[job_id]["progress"] = idx + 1
                jobs[job_id]["results"] = results[:]

        for row in missing:
            res = {
                "filename": row.get("cpyNo","?") + "_B.pdf",
                "overallResult": "FAIL",
                "issues": "PDF NOT SUBMITTED",
                **{k: row.get(k,"") for k in ["srNo","docNo","cpyNo","revision","title"]},
                **{k: "FAIL" for k in ["docNoMatch","cpyNoMatch","revMatch",
                                        "sigsResult","commentsResult",
                                        "classificationResult","prevRevResult","titleMatch"]},
                "sigCount": 0, "commentsCount": 0,
                "classificationMissingPages": [], "docNoFromPdf": "",
                "cpyNoFromPdf": "", "revFromPdf": "",
            }
            results.append(res)
            log(f"[MISSING] {row.get('cpyNo','?')} — not submitted")

        summary = {
            "total":   len(results),
            "passed":  sum(1 for r in results if r["overallResult"]=="PASS"),
            "failed":  sum(1 for r in results if r["overallResult"]=="FAIL"),
            "warned":  sum(1 for r in results if r["overallResult"]=="WARN"),
        }
        log(f"Complete: {summary['passed']} PASS | {summary['failed']} FAIL | {summary['warned']} WARN")

        with jobs_lock:
            jobs[job_id].update({
                "status":  "done",
                "results": results,
                "summary": summary,
            })

    except Exception as e:
        log(f"FATAL ERROR: {e}")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(e)


@app.route("/api/job/<job_id>")
def job_status(job_id):
    """Poll job status + incremental results."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/job/<job_id>/log")
def job_log(job_id):
    """Return log lines for a job (for live log panel)."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"log": job.get("log", [])})


@app.route("/api/job/<job_id>/download")
def download_report(job_id):
    """Download Excel verification report for a completed job."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Job not ready"}), 400

    try:
        from verifier_core import generate_excel_report
        excel_bytes = generate_excel_report(
            job["results"], job.get("excel_name", "transmittal")
        )
        buf = BytesIO(excel_bytes)
        buf.seek(0)
        name = f"Verification_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(buf, download_name=name,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
