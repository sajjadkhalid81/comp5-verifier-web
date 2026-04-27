"""
COMP5 Drawing Verification — Web Application
Flask backend for Render.com deployment
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

from flask import (Flask, request, jsonify, render_template, send_file)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "comp5-verify-2026")

# ── In-memory job store ────────────────────────────────────────────────────────
# NOTE: gunicorn MUST run with --workers 1 so all requests share this dict
jobs      = {}
jobs_lock = threading.Lock()

MAX_CONTENT_LENGTH = 500 * 1024 * 1024
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

import sys
sys.path.insert(0, os.path.dirname(__file__))

try:
    from verifier_core import (
        verify_pdf, collect_pdfs_from_zip, load_transmittal_excel
    )
    VERIFIER_OK  = True
    IMPORT_ERROR = ""
except Exception as e:
    VERIFIER_OK  = False
    IMPORT_ERROR = str(e)

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ping")
def ping():
    return jsonify({"pong": True})


@app.route("/health")
def health():
    info = {
        "status":   "ok" if VERIFIER_OK else "degraded",
        "verifier": VERIFIER_OK,
        "time":     datetime.utcnow().isoformat(),
    }
    if not VERIFIER_OK:
        info["error"] = IMPORT_ERROR
    return jsonify(info)


@app.route("/api/verify", methods=["POST"])
def start_verification():
    if not VERIFIER_OK:
        return jsonify({"error": f"Verifier not loaded: {IMPORT_ERROR}"}), 500

    if "zip" not in request.files or "excel" not in request.files:
        return jsonify({"error": "Both ZIP and Excel files are required"}), 400

    zip_file   = request.files["zip"]
    excel_file = request.files["excel"]
    zip_bytes   = zip_file.read()
    excel_bytes = excel_file.read()
    zip_name    = secure_filename(zip_file.filename)
    excel_name  = secure_filename(excel_file.filename)

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status":     "running",
            "progress":   0,
            "total":      0,
            "log":        [],
            "results":    [],
            "zip_name":   zip_name,
            "excel_name": excel_name,
            "started":    datetime.utcnow().isoformat(),
        }

    thread = threading.Thread(
        target=_run_verification,
        args=(job_id, zip_bytes, excel_bytes),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id})


def _run_verification(job_id, zip_bytes, excel_bytes):
    def log(msg):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id].setdefault("log", []).append(msg)

    try:
        log("Parsing Excel transmittal...")
        transmittal = load_transmittal_excel(excel_bytes)
        del excel_bytes
        log(f"Transmittal loaded: {len(transmittal)} drawings")

        log("Reading ZIP file...")
        pdf_entries = collect_pdfs_from_zip(zip_bytes)
        del zip_bytes
        log(f"Found {len(pdf_entries)} PDF(s) in ZIP")

        _CPY_RE = re.compile(r"(\d{3}-\d{2}-[A-Z]+-[A-Z]+-\d{4,5})", re.IGNORECASE)
        def _cpy(fn):
            m = _CPY_RE.search(fn)
            return re.sub(r"[\s\-_]", "", m.group(1) if m else fn).upper()

        tmap = {re.sub(r"[\s\-_]","",r["cpyNo"]).upper(): r
                for r in transmittal if r.get("cpyNo")}
        matched = [(s, b, tmap[_cpy(s)]) for s, b in pdf_entries if _cpy(s) in tmap]
        missing = [r for r in transmittal
                   if r.get("cpyNo") and
                   re.sub(r"[\s\-_]","",r["cpyNo"]).upper() not in {_cpy(s) for s,_ in pdf_entries}]

        total = len(matched) + len(missing)
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["total"] = total

        results = []
        for idx, (short, pdf_bytes_item, row) in enumerate(matched):
            log(f"[{idx+1}/{total}] {short}")
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
                    "classificationMissingPages": [],
                    "docNoFromPdf": "", "cpyNoFromPdf": "", "revFromPdf": "",
                }
            results.append(res)
            log(f"  → {res.get('overallResult','?')}")
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id]["results"] = results[:]
                    jobs[job_id]["progress"] = idx + 1

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
                "classificationMissingPages": [],
                "docNoFromPdf": "", "cpyNoFromPdf": "", "revFromPdf": "",
            }
            results.append(res)
            log(f"[MISSING] {row.get('cpyNo','?')} — not submitted")

        summary = {
            "total":  len(results),
            "passed": sum(1 for r in results if r["overallResult"]=="PASS"),
            "failed": sum(1 for r in results if r["overallResult"]=="FAIL"),
            "warned": sum(1 for r in results if r["overallResult"]=="WARN"),
        }
        log(f"Complete: {summary['passed']} PASS | {summary['failed']} FAIL | {summary['warned']} WARN")
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["status"]  = "done"
                jobs[job_id]["summary"] = summary

    except Exception as e:
        log(f"FATAL ERROR: {e}")
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"]  = str(e)


@app.route("/api/job/<job_id>")
def job_status(job_id):
    with jobs_lock:
        job = dict(jobs.get(job_id, {}))
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/job/<job_id>/log")
def job_log(job_id):
    with jobs_lock:
        job = dict(jobs.get(job_id, {}))
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"log": job.get("log", [])})


@app.route("/api/job/<job_id>/download")
def download_report(job_id):
    with jobs_lock:
        job = dict(jobs.get(job_id, {}))
    if not job:
        return jsonify({"error": "Job not found"}), 404
    results = job.get("results", [])
    if not results:
        return jsonify({"error": "No results yet"}), 400
    try:
        from verifier_core import generate_excel_report
        excel_bytes = generate_excel_report(results, job.get("excel_name", "transmittal"))
        buf  = BytesIO(excel_bytes)
        buf.seek(0)
        name = f"Verification_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
        resp = send_file(buf, download_name=name,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True)
        resp.headers["Content-Disposition"] = f'attachment; filename="{name}"'
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Report routes ──────────────────────────────────────────────────────────────

@app.route("/reports")
def reports_page():
    return render_template("reports.html")


@app.route("/api/tq-sdr-report", methods=["POST"])
def tq_sdr_report():
    try:
        from report_core import generate_tq_sdr_report
    except ImportError as e:
        return jsonify({"error": f"report_core not available: {e}"}), 500
    if "excel" not in request.files:
        return jsonify({"error": "Excel file required"}), 400
    try:
        excel_bytes = request.files["excel"].read()
        result  = generate_tq_sdr_report(excel_bytes)
        job_id  = str(uuid.uuid4())
        with jobs_lock:
            jobs[job_id] = {
                "status": "done", "type": "tq_sdr",
                "tqy": result["tqy"], "sdr": result["sdr"],
                "summary": result["summary"],
            }
        return jsonify({"job_id": job_id, "summary": result["summary"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tq-sdr-report/<job_id>/<report_type>")
def download_tq_sdr(job_id, report_type):
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
    try:
        from report_core import generate_comp5_weekly_report
    except ImportError as e:
        return jsonify({"error": f"report_core not available: {e}"}), 500
    if "excel" not in request.files:
        return jsonify({"error": "Excel file required"}), 400
    try:
        excel_bytes  = request.files["excel"].read()
        report_bytes = generate_comp5_weekly_report(excel_bytes)
        job_id = str(uuid.uuid4())
        with jobs_lock:
            jobs[job_id] = {"status": "done", "type": "comp5_weekly", "data": report_bytes}
        return jsonify({"job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/comp5-weekly-report/<job_id>")
def download_comp5_weekly(job_id):
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
