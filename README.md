# COMP5 Drawing Verification — Web App
**QatarEnergy LNG / Saipem JV**

## Deploy to Render.com (Free)

### Step 1 — Push to GitHub
```bash
git init
git add .
git commit -m "COMP5 Web Verifier v2.0"
git remote add origin https://github.com/YOUR_USERNAME/comp5-verifier-web.git
git push -u origin main
```

### Step 2 — Create Render service
1. Go to render.com → Sign up free
2. Click "New +" → "Web Service"
3. Connect your GitHub repo
4. Settings:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn app:app --workers 2 --timeout 120 --bind 0.0.0.0:$PORT`
   - **Plan:** Free
5. Click "Deploy"

### Step 3 — Point your domain (optional)
In your domain registrar (where sksoftech.com is hosted):
Add CNAME record:
- Host: `app`
- Value: `comp5-verifier.onrender.com`

This makes `app.sksoftech.com` → your verifier app.

## Files
| File | Purpose |
|---|---|
| `app.py` | Flask web server |
| `verifier_core.py` | Pure Python verification (no PyQt5) |
| `templates/index.html` | Web UI |
| `requirements.txt` | Python dependencies |
| `render.yaml` | Render.com config |

## Features
- Upload ZIP + Excel → live progress bar
- Supports nested ZIPs (ST drawings: ZIP→ZIP→PDF)
- CPY-based smart matching (order independent)
- Download Excel report
- Works on any browser — no installation needed
