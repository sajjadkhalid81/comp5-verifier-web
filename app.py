"""
COMP5 Reports Web App — Standalone Flask application
Fix: Files are streamed directly in the API response (no in-memory store).
     This works correctly on Render free tier where the server can spin down
     between requests, wiping any in-memory data.

Routes:
  GET  /                       -> reports page
  POST /api/tq-sdr/summary     -> return JSON summary only (KPI display)
  POST /api/tq-sdr/tqy         -> generate & download TQY Excel immediately
  POST /api/tq-sdr/sdr         -> generate & download SDR Excel immediately
  POST /api/comp5/summary      -> return JSON summary only
  POST /api/comp5/download     -> generate & download COMP5 Excel immediately
"""

import os
from io import BytesIO
from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@app.route("/")
def index():
    return render_template("reports.html")


# ── TQ & SDR ───────────────────────────────────────────────────────────────

@app.route("/api/tq-sdr/summary", methods=["POST"])
def api_tq_sdr_summary():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    try:
        from report_core import generate_tq_sdr
        result = generate_tq_sdr(request.files["file"].read())
        return jsonify({"summary": result["summary"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tq-sdr/tqy", methods=["POST"])
def api_download_tqy():
    if "file" not in request.files:
        return "No file uploaded", 400
    try:
        from report_core import generate_tq_sdr
        result = generate_tq_sdr(request.files["file"].read())
        return send_file(BytesIO(result["tqy_bytes"]),
                         download_name=result["tqy_name"],
                         as_attachment=True, mimetype=XLSX_MIME)
    except Exception as e:
        return str(e), 500


@app.route("/api/tq-sdr/sdr", methods=["POST"])
def api_download_sdr():
    if "file" not in request.files:
        return "No file uploaded", 400
    try:
        from report_core import generate_tq_sdr
        result = generate_tq_sdr(request.files["file"].read())
        return send_file(BytesIO(result["sdr_bytes"]),
                         download_name=result["sdr_name"],
                         as_attachment=True, mimetype=XLSX_MIME)
    except Exception as e:
        return str(e), 500


# ── COMP5 ──────────────────────────────────────────────────────────────────

@app.route("/api/comp5/summary", methods=["POST"])
def api_comp5_summary():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    try:
        from report_core import generate_comp5
        result = generate_comp5(request.files["file"].read())
        return jsonify({"summary": result["summary"], "filename": result["filename"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/comp5/download", methods=["POST"])
def api_download_comp5():
    if "file" not in request.files:
        return "No file uploaded", 400
    try:
        from report_core import generate_comp5
        result = generate_comp5(request.files["file"].read())
        return send_file(BytesIO(result["bytes"]),
                         download_name=result["filename"],
                         as_attachment=True, mimetype=XLSX_MIME)
    except Exception as e:
        return str(e), 500


# ── Run ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
