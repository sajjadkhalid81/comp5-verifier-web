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

# ── Job store — in-memory dict (fast) ────────────────────────────────────────
jobs      = {}   # job_id → job dict
jobs_lock = threading.Lock()

def _get_job(job_id):
    with jobs_lock:
        return jobs.get(job_id)

def _update_job(job_id, updates):
    with jobs_lock:
        if job_id not in jobs:
            jobs[job_id] = {}
        jobs[job_id].update(updates)

def _append_log(job_id, msg):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].setdefault("log", []).append(msg)

def _append_result(job_id, res):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].setdefault("results", []).append(res)
            jobs[job_id]["progress"] = len(jobs[job_id]["results"])

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


@app.route("/ping")
def ping():
    """Keepalive endpoint — called by frontend every 10s during verification."""
    return jsonify({"pong": True})


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

        # CPY lookup
        _CPY_RE = re.compile(r"(\d{3}-\d{2}-[A-Z]+-[A-Z]+-\d{4,5})", re.IGNORECASE)
        def _cpy(fn):
            m = _CPY_RE.search(fn)
            return re.sub(r"[\s\-_]", "", m.group(1) if m else fn).upper()

        tmap = {re.sub(r"[\s\-_]","",r["cpyNo"]).upper(): r
                for r in transmittal if r.get("cpyNo")}

        # ── Scan ZIP to count PDFs (no extraction) ─────────────────────────
        import zipfile as _zipfile
        def _scan_zip_names(zdata, depth=0):
            """Return list of PDF short names without extracting bytes."""
            names = []
            if depth > 5: return names
            try:
                with _zipfile.ZipFile(BytesIO(zdata)) as z:
                    for n in sorted(z.namelist()):
                        short = n.split("/")[-1]
                        if n.lower().endswith(".pdf"):
                            names.append(short)
                        elif n.lower().endswith(".zip"):
                            names.extend(_scan_zip_names(z.read(n), depth+1))
            except Exception:
                pass
            return names

        all_pdf_names = _scan_zip_names(zip_bytes)
        matched_cpys  = {_cpy(n) for n in all_pdf_names if _cpy(n) in tmap}
        missing_rows  = [r for r in transmittal
                         if r.get("cpyNo") and
                         re.sub(r"[\s\-_]","",r["cpyNo"]).upper() not in matched_cpys]
        total = len(matched_cpys) + len(missing_rows)
        log(f"Found {len(all_pdf_names)} PDF(s) — {len(missing_rows)} missing — {total} total")
        _update_job(job_id, {"total": total})

        # ── Stream PDFs one-at-a-time — never all in memory ─────────────────
        def _stream_pdfs(zdata, depth=0):
            """Yield (short_name, pdf_bytes) one at a time, then free memory."""
            if depth > 5: return
            try:
                with _zipfile.ZipFile(BytesIO(zdata)) as z:
                    for n in sorted(z.namelist()):
                        short = n.split("/")[-1]
                        if n.lower().endswith(".pdf"):
                            yield short, z.read(n)   # read ONE PDF
                        elif n.lower().endswith(".zip"):
                            yield from _stream_pdfs(z.read(n), depth+1)
            except Exception:
                pass

        results = []
        processed = set()
        idx = 0
        for short, pdf_bytes_item in _stream_pdfs(zip_bytes):
            key = _cpy(short)
            if key not in tmap: continue
            processed.add(key)
            row = tmap[key]
            idx += 1
            log(f"[{idx}/{total}] {short}")
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
            del pdf_bytes_item   # free immediately after use
            _append_result(job_id, res)
            log(f"  → {res.get('overallResult','?')}")
            results.append(res)

        for row in missing_rows:
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
            _append_result(job_id, res)
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


# ── Report routes ──────────────────────────────────────────────────────────

@app.route("/reports")
def reports_page():
    return render_template("reports.html")


@app.route("/api/tq-sdr-report", methods=["POST"])
def tq_sdr_report():
    """Generate TQ & SDR Excel reports from uploaded log file."""
    try:
        from report_core import generate_tq_sdr_report
    except ImportError as e:
        return jsonify({"error": f"report_core not available: {e}"}), 500

    if "excel" not in request.files:
        return jsonify({"error": "Excel file required"}), 400
    try:
        excel_bytes = request.files["excel"].read()
        result = generate_tq_sdr_report(excel_bytes)
        # Store in jobs store temporarily
        job_id = str(uuid.uuid4())
        with jobs_lock:
            jobs[job_id] = {
                "status": "done",
                "type": "tq_sdr",
                "tqy": result["tqy"],
                "sdr": result["sdr"],
                "summary": result["summary"],
            }
        return jsonify({"job_id": job_id, "summary": result["summary"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tq-sdr-report/<job_id>/<report_type>")
def download_tq_sdr(job_id, report_type):
    """Download TQY or SDR report."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job.get("type") != "tq_sdr":
        return jsonify({"error": "Report not found"}), 404
    data = job.get(report_type)
    if not data:
        return jsonify({"error": "Invalid report type"}), 400
    buf  = BytesIO(data); buf.seek(0)
    name = f"COMP5_{report_type.upper()}_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    resp = send_file(buf, download_name=name,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True)
    resp.headers["Content-Disposition"] = f'attachment; filename="{name}"'
    return resp


@app.route("/api/comp5-weekly-report", methods=["POST"])
def comp5_weekly_report():
    """Generate Weekly COMP5 Issued Documents report."""
    try:
        from report_core import generate_comp5_weekly_report
    except ImportError as e:
        return jsonify({"error": f"report_core not available: {e}"}), 500

    if "excel" not in request.files:
        return jsonify({"error": "Excel file required"}), 400
    try:
        excel_bytes = request.files["excel"].read()
        report_bytes = generate_comp5_weekly_report(excel_bytes)
        job_id = str(uuid.uuid4())
        with jobs_lock:
            jobs[job_id] = {
                "status": "done",
                "type": "comp5_weekly",
                "data": report_bytes,
            }
        return jsonify({"job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/comp5-weekly-report/<job_id>")
def download_comp5_weekly(job_id):
    """Download the weekly COMP5 report."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job.get("type") != "comp5_weekly":
        return jsonify({"error": "Report not found"}), 404
    buf  = BytesIO(job["data"]); buf.seek(0)
    name = f"COMP5_Weekly_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    resp = send_file(buf, download_name=name,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True)
    resp.headers["Content-Disposition"] = f'attachment; filename="{name}"'
    return resp


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
