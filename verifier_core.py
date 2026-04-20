"""
verifier_core.py — Pure Python verification functions
Extracted from comp5_verifier.py — NO PyQt5 imports.
Used by both the web app (app.py) and can be used for testing.
"""

import os, re, io, zipfile, math, tempfile, subprocess
from pathlib import Path
from io import BytesIO

# ── Optional imports (graceful fallback) ─────────────────────────────────────
try:
    import fitz        # PyMuPDF
    PYMUPDF_OK = True
except ImportError:
    PYMUPDF_OK = False

try:
    from PIL import Image, ImageDraw, ImageFont, ImageEnhance
    PIL_OK = True
except ImportError:
    PIL_OK = False

try:
    import openpyxl
    OPENPYXL_OK = True
except ImportError:
    OPENPYXL_OK = False

try:
    from pypdf import PdfReader
    PYPDF_OK = True
except ImportError:
    PYPDF_OK = False

try:
    import numpy as np
    NUMPY_OK = True
except ImportError:
    NUMPY_OK = False

# ── Copy core functions from comp5_verifier.py ───────────────────────────────
# These are the pure-Python functions that do not depend on PyQt5

REQUIRED_REVISION = "B"
CLASSIF_VARIANTS  = [
    "Classification: Internal", "Classification:Internal",
    "CLASSIFICATION: INTERNAL", "classification: internal",
]

COORDS = {
    "A1": {
        "doc_no":   (2110, 1463, 2350, 1483),
        "cpy_no":   (2053, 1598, 2225, 1618),
        "revision": (2290, 1598, 2340, 1618),
        "sigs": {
            "DRN":   (2195, 1130, 2232, 1185),
            "DSGND": (2232, 1130, 2264, 1185),
            "CHKD":  (2264, 1130, 2295, 1185),
            "APVD1": (2295, 1130, 2325, 1185),
            "APVD2": (2325, 1130, 2362, 1185),
        },
    }
}

CLASSIF_REGION = (0, 0, 500, 80)
DOC_NO_PATTERN = re.compile(r"\d{4,6}-[A-Z]-[A-Z0-9]+-\d+-[A-Z]+-[A-Z]+-[A-Z]+-\d+-\d+")
CPY_RE         = re.compile(r"(\d{3}-\d{2}-[A-Z]+-[A-Z]+-\d{4,5})", re.IGNORECASE)


def normalize(s):
    return re.sub(r"[\s\-_]", "", str(s)).upper()


def normalize_cpy_seq(s):
    s = str(s).strip()
    m = re.match(r"^(\d{3}-\d{2}-[A-Za-z]+-[A-Za-z]+-)(\d{4,5})$", s)
    if m:
        return m.group(1) + m.group(2).zfill(5)
    return s


def doc_no_match(pdf_val, excel_val):
    if not pdf_val or not excel_val:
        return False
    p = normalize(pdf_val)
    e = normalize(excel_val)
    if p == e:
        return True
    def split_prefix(s):
        m = re.match(r"^(\d+)([A-Z].+)$", s)
        return (m.group(1), m.group(2)) if m else (s, "")
    p_num, p_rest = split_prefix(p)
    e_num, e_rest = split_prefix(e)
    if p_rest and e_rest and p_rest == e_rest:
        if e_num.endswith(p_num) or p_num.endswith(e_num):
            return True
    return False


def cpy_no_match(pdf_val, excel_val):
    if not pdf_val or not excel_val:
        return False
    n_pdf   = re.sub(r"[\s\-_]", "", pdf_val).upper()
    n_excel = re.sub(r"[\s\-_]", "", excel_val).upper()
    if n_pdf == n_excel:
        return True
    return (re.sub(r"[\s\-_]", "", normalize_cpy_seq(pdf_val)).upper() ==
            re.sub(r"[\s\-_]", "", normalize_cpy_seq(excel_val)).upper())


def collect_pdfs_from_zip(zip_source, depth=0):
    """Recursively collect PDFs from ZIP (any nesting depth)."""
    results = []
    if depth > 5:
        return results
    try:
        if isinstance(zip_source, (str, Path)):
            zf_obj = zipfile.ZipFile(zip_source, "r")
        else:
            zf_obj = zipfile.ZipFile(BytesIO(zip_source), "r")
        with zf_obj as zf:
            for name in sorted(n for n in zf.namelist()
                               if not n.startswith("__") and not n.endswith("/")):
                short = name.split("/")[-1]
                low   = name.lower()
                if low.endswith(".pdf"):
                    results.append((short, zf.read(name)))
                elif low.endswith(".zip"):
                    results.extend(collect_pdfs_from_zip(zf.read(name), depth + 1))
    except Exception:
        pass
    seen = set()
    unique = []
    for fname, data in results:
        if fname not in seen:
            seen.add(fname)
            unique.append((fname, data))
    return unique


def load_transmittal_excel(excel_source):
    """
    Load transmittal from Excel file (path or bytes).
    Returns list of dicts with keys: srNo, docNo, cpyNo, revision, title.
    Reads by Document No. — not by Sr. No. — so blank rows never stop reading.
    """
    if not OPENPYXL_OK:
        raise ImportError("openpyxl not installed")

    if isinstance(excel_source, (str, Path)):
        wb = openpyxl.load_workbook(excel_source, data_only=True)
    else:
        wb = openpyxl.load_workbook(BytesIO(excel_source), data_only=True)

    sheet_name = next(
        (n for n in wb.sheetnames if "QG-NFPS" in n.upper()),
        wb.sheetnames[0]
    )
    ws = wb[sheet_name]

    transmittal = []
    sr_counter  = 0
    for row in ws.iter_rows(min_row=25, values_only=True):
        doc_no = str(row[1] or "").strip()
        cpy_no = str(row[2] or "").strip()
        if not doc_no and not cpy_no:
            continue
        if doc_no.lower() in ("document no.", "doc no") or \
           cpy_no.lower() in ("client doc. no.", "cpy no"):
            continue
        sr_counter += 1
        try:
            sr = int(float(str(row[0]).strip())) if row[0] else sr_counter
        except (ValueError, TypeError):
            sr = sr_counter
        transmittal.append({
            "srNo":     sr,
            "docNo":    doc_no,
            "cpyNo":    cpy_no,
            "revision": str(row[3] or "").strip().replace("\xa0", "").strip(),
            "title":    str(row[4] or "").strip(),
        })
    return transmittal


def get_page_type(page):
    w, h = page.rect.width, page.rect.height
    if abs(w - 2384) < 200 and abs(h - 1684) < 200:
        return "A1"
    if abs(w - 2551) < 200 and abs(h - 1772) < 200:
        return "A1"  # HVAC variant
    if abs(w - 792) < 50 and abs(h - 612) < 50:
        return "AB"
    if abs(w - 595) < 50 and abs(h - 842) < 50:
        return "A4"
    return "A1"  # default


def extract_text_at(page, coords):
    x0, top, x1, bot = coords
    return page.get_text("text", clip=fitz.Rect(x0, top, x1, bot)).strip()


def verify_pdf(pdf_bytes, filename, row):
    """
    Full 8-check verification of a single PDF.
    Returns result dict compatible with web and desktop versions.
    """
    if not PYMUPDF_OK:
        return _error_result(filename, row, "PyMuPDF not installed")

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        return _error_result(filename, row, f"Cannot open PDF: {e}")

    try:
        page      = doc[0]
        page_type = get_page_type(page)
        excel_doc = str(row.get("docNo", "")).strip()
        excel_cpy = str(row.get("cpyNo", "")).strip()
        excel_rev = str(row.get("revision", "")).strip()
        excel_ttl = str(row.get("title", "")).strip()

        # 1 ── Doc No
        doc_status, doc_display, doc_from_pdf = _check_doc_no(page, page_type, excel_doc)

        # 2 ── CPY No
        cpy_status, cpy_display, cpy_from_pdf = _check_cpy_no(page, page_type, filename, excel_cpy)

        # 3 ── Revision
        rev_status, rev_display, rev_from_pdf = _check_revision(page, page_type, filename, excel_rev)

        # 4 ── Signatures
        sig_status, sig_count = _check_signatures(doc, page_type)

        # 5 ── Comments
        com_status, com_count = _check_comments(pdf_bytes)

        # 6 ── Classification
        cls_status, cls_missing = _check_classification(doc)

        # 7 ── Prev Rev
        prev_status = _check_prev_rev(page)

        # 8 ── Title
        ttl_status, ttl_display = _check_title(page, excel_ttl)

        doc.close()

        hard_fail = any(s == "FAIL" for s in [
            rev_status, sig_status, com_status, cls_status, doc_status, cpy_status
        ])
        all_pass = all(s == "PASS" for s in [
            doc_status, cpy_status, rev_status, sig_status, com_status,
            cls_status, prev_status, ttl_status
        ])
        overall = "FAIL" if hard_fail else ("PASS" if all_pass else "WARN")

        issues_parts = []
        if doc_status == "FAIL": issues_parts.append(f"Doc No mismatch (PDF: {doc_from_pdf} | Excel: {excel_doc})")
        if cpy_status == "FAIL": issues_parts.append(f"CPY No mismatch (PDF: {cpy_from_pdf} | Excel: {excel_cpy})")
        if rev_status == "FAIL": issues_parts.append(f"Rev mismatch (PDF: {rev_from_pdf})")
        if sig_status == "FAIL": issues_parts.append(f"Insufficient signatures ({sig_count}/5)")
        if com_status == "FAIL": issues_parts.append(f"{com_count} comment(s)")
        if cls_status == "FAIL": issues_parts.append(f"Classification missing on pages: {cls_missing}")
        if prev_status == "WARN": issues_parts.append("Prev rev not confirmed")
        if ttl_status == "WARN": issues_parts.append("Title not confirmed")

        return {
            "filename":    filename,
            "srNo":        row.get("srNo", ""),
            "docNo":       excel_doc,
            "cpyNo":       excel_cpy,
            "revision":    excel_rev,
            "title":       excel_ttl,
            "docNoFromPdf":  doc_from_pdf,
            "cpyNoFromPdf":  cpy_from_pdf,
            "revFromPdf":    rev_from_pdf,
            "titleFromPdf":  ttl_display,
            "docNoMatch":    doc_status,
            "cpyNoMatch":    cpy_status,
            "revMatch":      rev_status,
            "sigsResult":    sig_status,
            "sigCount":      sig_count,
            "commentsResult":      com_status,
            "commentsCount":       com_count,
            "classificationResult":      cls_status,
            "classificationMissingPages": cls_missing,
            "prevRevResult": prev_status,
            "titleMatch":    ttl_status,
            "overallResult": overall,
            "issues":        "; ".join(issues_parts) if issues_parts else "None",
        }

    except Exception as e:
        try:
            doc.close()
        except Exception:
            pass
        return _error_result(filename, row, str(e))


def _error_result(filename, row, error_msg):
    return {
        "filename": filename, "srNo": row.get("srNo",""),
        "docNo": row.get("docNo",""), "cpyNo": row.get("cpyNo",""),
        "revision": row.get("revision",""), "title": row.get("title",""),
        "docNoFromPdf": "", "cpyNoFromPdf": "", "revFromPdf": "", "titleFromPdf": "",
        "docNoMatch": "FAIL", "cpyNoMatch": "FAIL", "revMatch": "FAIL",
        "sigsResult": "FAIL", "sigCount": 0,
        "commentsResult": "FAIL", "commentsCount": 0,
        "classificationResult": "FAIL", "classificationMissingPages": [],
        "prevRevResult": "FAIL", "titleMatch": "FAIL",
        "overallResult": "FAIL",
        "issues": f"Error: {error_msg}",
    }


def _check_doc_no(page, page_type, excel_val):
    raw = extract_text_at(page, COORDS["A1"]["doc_no"])
    m = DOC_NO_PATTERN.search(raw) if raw else None
    pdf_val = m.group(0) if m else (raw.strip() if raw else "")
    if pdf_val:
        if not excel_val:
            return "PASS", f"✓ {pdf_val}", pdf_val
        if doc_no_match(pdf_val, excel_val):
            return "PASS", f"✓ {pdf_val}", pdf_val
        return "FAIL", pdf_val, pdf_val
    return "PASS", (f"✓ {excel_val}  |  Remark: vector font"), excel_val


def _check_cpy_no(page, page_type, filename, excel_val):
    fname_cpy = re.sub(r"_[A-Z]\.pdf$", "", filename, flags=re.IGNORECASE)
    fname_cpy = re.sub(r"\.pdf$", "", fname_cpy, flags=re.IGNORECASE)
    raw = extract_text_at(page, COORDS["A1"]["cpy_no"])
    if raw:
        m = CPY_RE.search(raw.replace("\n", " "))
        pdf_val = m.group(1) if m else raw.split("\n")[0].strip()
        if cpy_no_match(pdf_val, excel_val):
            return "PASS", f"✓ {pdf_val}", pdf_val
        return "FAIL", pdf_val, pdf_val
    if fname_cpy and cpy_no_match(fname_cpy, excel_val):
        return "PASS", f"✓ {fname_cpy} (filename)", fname_cpy
    return "PASS", f"✓ {excel_val}  |  Remark: vector font", excel_val


def _check_revision(page, page_type, filename, excel_val):
    m = re.search(r"_([A-Za-z])\.pdf$", filename)
    rev = m.group(1).upper() if m else ""
    if rev == REQUIRED_REVISION:
        return "PASS", f"✓ {rev}", rev
    if rev:
        return "FAIL", rev, rev
    raw = extract_text_at(page, COORDS["A1"]["revision"]).strip()
    if raw and raw.upper() == REQUIRED_REVISION:
        return "PASS", f"✓ {raw}", raw
    return "WARN", raw or "?", raw


def _check_signatures(doc, page_type):
    if not PIL_OK or not NUMPY_OK:
        return "WARN", 0
    page  = doc[0]
    scale = 150 / 72
    pix   = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csRGB)
    img   = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    arr   = np.array(img)
    count = 0
    for cx0, cy0, cx1, cy1 in COORDS["A1"]["sigs"].values():
        crop = arr[int(cy0*scale):int(cy1*scale), int(cx0*scale):int(cx1*scale)]
        if crop.size > 0:
            pct = np.sum(np.any(crop < 200, axis=2)) / (crop.shape[0] * crop.shape[1])
            if pct > 0.02:
                count += 1
    return ("PASS" if count >= 3 else "FAIL"), count


def _check_comments(pdf_bytes):
    if not PYPDF_OK:
        return "WARN", 0
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        count  = 0
        for pg in reader.pages:
            annots = pg.get("/Annots")
            if annots:
                for ann in annots:
                    ao = ann.get_object()
                    if str(ao.get("/Subtype", "")).replace(" ", "") not in ("/Widget", "/Link"):
                        count += 1
        return ("PASS" if count == 0 else "FAIL"), count
    except Exception:
        return "WARN", 0


def _check_classification(doc):
    missing = []
    scale   = 150 / 72
    for i, page in enumerate(doc):
        full_text = page.get_text("text")
        if any(v in full_text for v in CLASSIF_VARIANTS):
            continue
        clip = fitz.Rect(*CLASSIF_REGION)
        pix  = page.get_pixmap(matrix=fitz.Matrix(scale, scale),
                                colorspace=fitz.csRGB, clip=clip)
        if pix.width > 0 and pix.height > 0:
            img   = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            raw   = img.tobytes()
            total = len(raw) // 3
            nw    = sum(1 for j in range(0, len(raw), 3)
                       if not (raw[j]>230 and raw[j+1]>230 and raw[j+2]>230))
            if total > 0 and (nw / total) > 0.008:
                continue
        missing.append(i + 1)
    return ("PASS" if not missing else "FAIL"), missing


def _check_prev_rev(page):
    text = page.get_text("text").upper()
    phrases = ["INTER-DISCIPLINE CHECK", "INTER DISCIPLINE CHECK", "ISSUED FOR INTER"]
    if any(p in text for p in phrases):
        return "PASS"
    return "WARN"


def _check_title(page, expected):
    if not expected:
        return "WARN", "(no title in Excel)"
    text  = page.get_text("text")
    words = {w for w in re.sub(r"[^A-Za-z0-9 ]", " ", expected.upper()).split() if len(w) > 2}
    found = {w for w in re.sub(r"[^A-Za-z0-9 ]", " ", text.upper()).split()       if len(w) > 2}
    if not words:
        return "WARN", "(empty title)"
    pct = len(words & found) / len(words)
    if pct >= 0.70:
        return "PASS", expected
    if pct >= 0.50:
        return "PASS", f"{expected}  |  Remark: partial {int(pct*100)}% match"
    return "WARN", expected


def generate_excel_report(results, transmittal_name=""):
    """Generate Excel report bytes from results list."""
    if not OPENPYXL_OK:
        raise ImportError("openpyxl not installed")
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Verification"
    headers = ["Sr.", "Filename", "Doc No (Excel)", "Doc No Match",
               "CPY Match", "Rev Match", "Signatures", "Comments",
               "Classification", "Prev Rev", "Title", "RESULT", "Issues"]
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.fill = PatternFill("solid", fgColor="1F2937")
        cell.font = Font(bold=True, color="FFFFFF")
    for ri, r in enumerate(results, 2):
        ov = r.get("overallResult", "")
        ws.cell(ri, 1, r.get("srNo", ri-1))
        ws.cell(ri, 2, r.get("filename", ""))
        ws.cell(ri, 3, r.get("docNo", ""))
        ws.cell(ri, 4, r.get("docNoMatch", ""))
        ws.cell(ri, 5, r.get("cpyNoMatch", ""))
        ws.cell(ri, 6, r.get("revMatch", ""))
        ws.cell(ri, 7, f"{r.get('sigCount',0)}/5  {r.get('sigsResult','')}")
        ws.cell(ri, 8, r.get("commentsResult", ""))
        ws.cell(ri, 9, r.get("classificationResult", ""))
        ws.cell(ri, 10, r.get("prevRevResult", ""))
        ws.cell(ri, 11, r.get("titleMatch", ""))
        ws.cell(ri, 12, ov)
        ws.cell(ri, 13, r.get("issues", ""))
        color = "1E8449" if ov=="PASS" else "C0392B" if ov=="FAIL" else "D68910"
        ws.cell(ri, 12).fill = PatternFill("solid", fgColor=color)
        ws.cell(ri, 12).font = Font(bold=True, color="FFFFFF")
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()
