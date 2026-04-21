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

# ── Job store — file-based so jobs survive server restarts ────────────────────
import pickle, hashlib
JOBS_DIR = "/tmp/comp5_jobs"
os.makedirs(JOBS_DIR, exist_ok=True)
jobs_lock = threading.Lock()

def _job_path(job_id):
    return os.path.join(JOBS_DIR, f"{job_id}.pkl")

def _save_job(job_id, job):
    try:
        with open(_job_path(job_id), "wb") as f:
            pickle.dump(job, f)
    except Exception:
        pass

def _load_job(job_id):
    try:
        p = _job_path(job_id)
        if os.path.exists(p):
            with open(p, "rb") as f:
                return pickle.load(f)
    except Exception:
        pass
    return None

def _get_job(job_id):
    return _load_job(job_id)

def _update_job(job_id, updates):
    with jobs_lock:
        job = _load_job(job_id) or {}
        job.update(updates)
        _save_job(job_id, job)
    return job

def _append_log(job_id, msg):
    with jobs_lock:
        job = _load_job(job_id) or {}
        job.setdefault("log", []).append(msg)
        _save_job(job_id, job)

def _append_result(job_id, res):
    with jobs_lock:
        job = _load_job(job_id) or {}
        job.setdefault("results", []).append(res)
        job["progress"] = len(job["results"])
        _save_job(job_id, job)

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

    # Create job — saved to disk immediately so it survives restarts
    job_id = str(uuid.uuid4())
    _update_job(job_id, {
        "status":     "running",
        "progress":   0,
        "total":      0,
        "log":        [],
        "results":    [],
        "zip_name":   zip_name,
        "excel_name": excel_name,
        "started":    datetime.utcnow().isoformat(),
    })

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
        _append_log(job_id, msg)

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
        _update_job(job_id, {"total": total})

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
            _append_result(job_id, res)
            ov = res.get("overallResult","FAIL")
            log(f"  → {ov}")
            results.append(res)

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

        _update_job(job_id, {
            "status":  "done",
            "summary": summary,
        })

    except Exception as e:
        log(f"FATAL ERROR: {e}")
        _update_job(job_id, {"status": "error", "error": str(e)})


@app.route("/api/job/<job_id>")
def job_status(job_id):
    """Poll job status + incremental results."""
    job = _get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found — server may have restarted. Please run a new verification."}), 404
    return jsonify(job)


@app.route("/api/job/<job_id>/log")
def job_log(job_id):
    """Return log lines for a job (for live log panel)."""
    job = _get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"log": job.get("log", [])})


@app.route("/api/job/<job_id>/download")
def download_report(job_id):
    """Download Excel verification report for a completed job."""
    job = _get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found — server restarted. Please run verification again."}), 404
    # Allow download if status is done OR if results exist (handles timing edge cases)
    has_results = len(job.get("results", [])) > 0
    if job["status"] not in ("done",) and not has_results:
        return jsonify({"error": "Job not ready — no results yet"}), 400

    try:
        from verifier_core import generate_excel_report
        excel_bytes = generate_excel_report(
            job["results"], job.get("excel_name", "transmittal")
        )
        buf = BytesIO(excel_bytes)
        buf.seek(0)
        name = f"Verification_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
        response = send_file(
            buf,
            download_name=name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True
        )
        # Ensure CORS headers allow download from browser
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Content-Disposition"] = f'attachment; filename="{name}"'
        return response
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
