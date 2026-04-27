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

from flask import (Flask, request, jsonify, render_template,
                   send_file)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "comp5-verify-2026")

# ── Job store — in-memory dict (fast, no disk overhead) ───────────────────────
jobs      = {}
jobs_lock = threading.Lock()

def _get_job(job_id):
    with jobs_lock:
        return dict(jobs.get(job_id, {}))

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

# ── Upload size ────────────────────────────────────────────────────────────────
MAX_CONTENT_LENGTH = 500 * 1024 * 1024
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

# ── Temp directory for uploaded files (disk, not RAM) ─────────────────────────
UPLOAD_DIR = os.path.join(tempfile.gettempdir(), "comp5_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ── Import verifier ────────────────────────────────────────────────────────────
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
    """Keepalive — called by frontend every 10s to prevent free-tier sleep."""
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
    """
    Save uploaded files to DISK immediately — never hold in RAM.
    Start background verification thread that reads from disk.
    """
    if not VERIFIER_OK:
        return jsonify({"error": f"Verifier not loaded: {IMPORT_ERROR}"}), 500

    if "zip" not in request.files or "excel" not in request.files:
        return jsonify({"error": "Both ZIP and Excel files are required"}), 400

    job_id   = str(uuid.uuid4())
    job_dir  = os.path.join(UPLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    # Save directly to disk — DO NOT read into RAM
    zip_path   = os.path.join(job_dir, "upload.zip")
    excel_path = os.path.join(job_dir, "transmittal.xlsx")

    try:
        request.files["zip"].save(zip_path)
        request.files["excel"].save(excel_path)
    except Exception as e:
        return jsonify({"error": f"Upload failed: {e}"}), 500

    zip_name   = secure_filename(request.files["zip"].filename)
    excel_name = secure_filename(request.files["excel"].filename)

    # Create job record
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

    # Start background thread — passes file PATHS, not bytes
    thread = threading.Thread(
        target=_run_verification,
        args=(job_id, zip_path, excel_path),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id})


def _make_error_result(short, row, error_msg):
    """Standard error result dict."""
    return {
        "filename":     short,
        "overallResult": "FAIL",
        "issues":       f"Error: {error_msg}",
        **{k: row.get(k, "") for k in ["srNo", "docNo", "cpyNo", "revision", "title"]},
        **{k: "FAIL" for k in ["docNoMatch", "cpyNoMatch", "revMatch",
                                "sigsResult", "commentsResult",
                                "classificationResult", "prevRevResult", "titleMatch"]},
        "sigCount": 0, "commentsCount": 0,
        "classificationMissingPages": [],
        "docNoFromPdf": "", "cpyNoFromPdf": "", "revFromPdf": "",
    }


def _run_verification(job_id, zip_path, excel_path):
    """
    Background worker.
    Reads files from DISK paths — never holds entire ZIP in RAM.
    Streams one PDF at a time from zipfile.
    """
    def log(msg):
        _append_log(job_id, msg)

    try:
        # ── Load Excel transmittal from disk ──────────────────────────────
        log("Parsing Excel transmittal...")
        with open(excel_path, "rb") as f:
            excel_bytes = f.read()
        transmittal = load_transmittal_excel(excel_bytes)
        del excel_bytes  # free immediately
        log(f"Transmittal loaded: {len(transmittal)} drawings")

        # ── CPY lookup map ────────────────────────────────────────────────
        _CPY_RE = re.compile(r"(\d{3}-\d{2}-[A-Z]+-[A-Z]+-\d{4,5})", re.IGNORECASE)
        def _cpy(fn):
            m = _CPY_RE.search(fn)
            return re.sub(r"[\s\-_]", "", m.group(1) if m else fn).upper()

        tmap = {
            re.sub(r"[\s\-_]", "", r["cpyNo"]).upper(): r
            for r in transmittal if r.get("cpyNo")
        }

        # ── Count total without loading all PDFs ──────────────────────────
        def _count_and_list(zpath, depth=0):
            """Recursively list PDF names from ZIP on disk. No bytes in RAM."""
            names = []
            if depth > 5: return names
            try:
                with zipfile.ZipFile(zpath, "r") as z:
                    for n in sorted(z.namelist()):
                        if n.lower().endswith(".pdf"):
                            names.append(n.split("/")[-1])
                        elif n.lower().endswith(".zip"):
                            # For nested ZIPs: extract to temp, recurse, delete
                            tmp = os.path.join(
                                tempfile.gettempdir(),
                                f"comp5_inner_{job_id}_{depth}_{uuid.uuid4().hex[:6]}.zip"
                            )
                            try:
                                with open(tmp, "wb") as f:
                                    f.write(z.read(n))
                                names.extend(_count_and_list(tmp, depth + 1))
                            finally:
                                try: os.remove(tmp)
                                except: pass
            except Exception as e:
                log(f"ZIP read warning: {e}")
            return names

        log("Scanning ZIP structure...")
        all_pdf_names = _count_and_list(zip_path)
        matched_cpys  = {_cpy(n) for n in all_pdf_names if _cpy(n) in tmap}
        missing_rows  = [
            r for r in transmittal
            if r.get("cpyNo") and
            re.sub(r"[\s\-_]", "", r["cpyNo"]).upper() not in matched_cpys
        ]
        total = len(matched_cpys) + len(missing_rows)
        log(f"Found {len(all_pdf_names)} PDF(s) | {len(missing_rows)} missing | {total} total")
        _update_job(job_id, {"total": total})

        # ── Stream PDFs one at a time from disk ───────────────────────────
        def _stream_from_disk(zpath, depth=0):
            """
            Yield (short_name, pdf_bytes) reading ONE PDF at a time.
            For nested ZIPs: extract inner ZIP to temp file, recurse, delete.
            Never holds more than ONE PDF in RAM at once.
            """
            if depth > 5: return
            try:
                with zipfile.ZipFile(zpath, "r") as z:
                    for n in sorted(z.namelist()):
                        short = n.split("/")[-1]
                        if n.lower().endswith(".pdf"):
                            yield short, z.read(n)   # ONE PDF at a time
                        elif n.lower().endswith(".zip"):
                            tmp = os.path.join(
                                tempfile.gettempdir(),
                                f"comp5_inner_{job_id}_{depth}_{uuid.uuid4().hex[:6]}.zip"
                            )
                            try:
                                with open(tmp, "wb") as f:
                                    f.write(z.read(n))
                                yield from _stream_from_disk(tmp, depth + 1)
                            finally:
                                try: os.remove(tmp)
                                except: pass
            except Exception as e:
                log(f"Stream warning: {e}")

        results   = []
        processed = set()
        idx       = 0

        for short, pdf_bytes in _stream_from_disk(zip_path):
            key = _cpy(short)
            if key not in tmap:
                continue
            processed.add(key)
            row = tmap[key]
            idx += 1
            log(f"[{idx}/{total}] {short}")

            try:
                res = verify_pdf(pdf_bytes, short, row)
            except Exception as e:
                res = _make_error_result(short, row, str(e))

            del pdf_bytes   # free PDF bytes immediately

            results.append(res)
            _append_result(job_id, res)
            log(f"  → {res.get('overallResult', '?')}")

        # ── Missing PDFs ──────────────────────────────────────────────────
        for row in missing_rows:
            res = {
                "filename":      row.get("cpyNo", "?") + "_B.pdf",
                "overallResult": "FAIL",
                "issues":        "PDF NOT SUBMITTED",
                **{k: row.get(k, "") for k in ["srNo", "docNo", "cpyNo", "revision", "title"]},
                **{k: "FAIL" for k in ["docNoMatch", "cpyNoMatch", "revMatch",
                                        "sigsResult", "commentsResult",
                                        "classificationResult", "prevRevResult", "titleMatch"]},
                "sigCount": 0, "commentsCount": 0,
                "classificationMissingPages": [],
                "docNoFromPdf": "", "cpyNoFromPdf": "", "revFromPdf": "",
            }
            results.append(res)
            _append_result(job_id, res)
            log(f"[MISSING] {row.get('cpyNo', '?')} — not submitted")

        summary = {
            "total":  len(results),
            "passed": sum(1 for r in results if r["overallResult"] == "PASS"),
            "failed": sum(1 for r in results if r["overallResult"] == "FAIL"),
            "warned": sum(1 for r in results if r["overallResult"] == "WARN"),
        }
        log(f"Complete: {summary['passed']} PASS | {summary['failed']} FAIL | {summary['warned']} WARN")
        _update_job(job_id, {"status": "done", "summary": summary})

    except Exception as e:
        log(f"FATAL ERROR: {e}")
        _update_job(job_id, {"status": "error", "error": str(e)})

    finally:
        # Clean up uploaded files from disk
        try:
            import shutil
            shutil.rmtree(os.path.join(UPLOAD_DIR, job_id), ignore_errors=True)
        except Exception:
            pass


@app.route("/api/job/<job_id>")
def job_status(job_id):
    job = _get_job(job_id)
    if not job:
        return jsonify({
            "error": "Job not found — server may have restarted.",
            "hint":  "Please run a new verification."
        }), 404
    return jsonify(job)


@app.route("/api/job/<job_id>/log")
def job_log(job_id):
    job = _get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"log": job.get("log", [])})


@app.route("/api/job/<job_id>/download")
def download_report(job_id):
    job = _get_job(job_id)
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
        resp = send_file(
            buf,
            download_name=name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True
        )
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
        result = generate_tq_sdr_report(excel_bytes)
        job_id = str(uuid.uuid4())
        with jobs_lock:
            jobs[job_id] = {
                "status":  "done",
                "type":    "tq_sdr",
                "tqy":     result["tqy"],
                "sdr":     result["sdr"],
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
    buf  = BytesIO(data)
    buf.seek(0)
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
        excel_bytes = request.files["excel"].read()
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
    buf  = BytesIO(job["data"])
    buf.seek(0)
    name = f"COMP5_Weekly_Report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    resp = send_file(buf, download_name=name,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True)
    resp.headers["Content-Disposition"] = f'attachment; filename="{name}"'
    return resp


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
