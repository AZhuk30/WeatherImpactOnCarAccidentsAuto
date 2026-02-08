# 🚀 Complete Automation Setup Guide
## GitHub Actions + Streamlit Cloud Deployment

This guide will help you automate your entire NYC Traffic Safety pipeline and deploy a public dashboard.

---

## 📋 What You're Building

```
┌─────────────────────────────────────────────────────────┐
│                  AUTOMATED SYSTEM                        │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  GitHub Actions (Runs Daily at 2 AM)                    │
│  └─> python run_pipeline.py                             │
│  └─> Fetches weather + collision data                   │
│  └─> Creates/updates weather_master.csv                 │
│  └─> Creates/updates collisions_master.csv              │
│  └─> Commits files back to GitHub                       │
│                     ↓                                    │
│  Streamlit Cloud (Always Online)                        │
│  └─> Reads master CSV files from GitHub                 │
│  └─> Shows dashboard at public URL                      │
│  └─> Updates automatically when data changes            │
│                                                          │
│  Result: Fully automated, publicly accessible dashboard │
│         that updates daily without your laptop!         │
└─────────────────────────────────────────────────────────┘
```

**Time to complete:** 2-3 hours  
**Cost:** $0 (completely free!)

---

## 📦 PART 1: Update Your Code Files

### Step 1.1: Update run_pipeline.py

**Replace your current `run_pipeline.py` with:**
- File: `run_pipeline_UPDATED.py` (provided)

**Key changes:**
- Supports `--days 30` argument (pulls 30 days instead of 7)
- Checks for `SKIP_DATABASE=true` environment variable
- Works in GitHub Actions without MySQL

**Test locally:**
```bash
python run_pipeline.py --days 30
```

---

### Step 1.2: Update src/transform.py

**Replace your current `src/transform.py` with:**
- File: `transform_UPDATED.py` (provided)

**Key changes:**
- Creates `weather_master.csv` and `collisions_master.csv`
- Appends new data instead of overwriting
- Removes duplicates automatically
- Accumulates data over time

**Place as:** `src/transform.py`

---

### Step 1.3: Update dashboard/app.py

**Replace your current `dashboard/app.py` with:**
- File: `app_STREAMLIT_READY.py` (provided)

**Key changes:**
- Reads from master CSV files
- Works on Streamlit Cloud
- Shows data range in sidebar
- Better error handling

**Place as:** `dashboard/app.py`

---

### Step 1.4: Create GitHub Actions Workflow

**Create this folder structure:**
```bash
mkdir -p .github/workflows
```

**Create file:** `.github/workflows/daily_update.yml`

**Copy content from:** `daily_update.yml` (provided)

This file tells GitHub to:
- Run your pipeline every day at 2 AM UTC
- Install Python and dependencies
- Run `python run_pipeline.py --days 30`
- Commit updated CSV files back to GitHub

---

### Step 1.5: Update .gitignore

**Add to your `.gitignore`:**
```
# Python
__pycache__/
*.pyc
*.pyo
venv/
.venv/

# Environment
.env
.env.local

# Data - ONLY ignore raw and logs
data/raw/*.csv
data/logs/*.log
data/logs/*.txt

# OS
.DS_Store
.idea/

# IMPORTANT: Do NOT ignore these (needed for Streamlit Cloud)
# data/processed/weather_master.csv
# data/processed/collisions_master.csv
```

**Note:** We WANT to commit the master CSV files so Streamlit Cloud can read them!

---

## 🔧 PART 2: Set Up GitHub Actions

### Step 2.1: Commit All Changes

```bash
# Make sure you're in your project directory
cd ~/Desktop/WeatherImpactOnCarAccidents

# Stage all new/updated files
git add .github/workflows/daily_update.yml
git add run_pipeline.py
git add src/transform.py
git add dashboard/app.py
git add .gitignore

# Commit
git commit -m "Add GitHub Actions automation and Streamlit Cloud support"

# Push to GitHub
git push origin main
```

### Step 2.2: Run Pipeline Locally Once

Before setting up automation, create the master CSV files:

```bash
# Run with 30 days of data
python run_pipeline.py --days 30

# Check that master files were created
ls -lh data/processed/weather_master.csv
ls -lh data/processed/collisions_master.csv
```

**You should see:**
```
weather_master.csv     (several MB)
collisions_master.csv  (several MB)
```

### Step 2.3: Commit Initial Data Files

```bash
# Add the master CSV files
git add data/processed/weather_master.csv
git add data/processed/collisions_master.csv

# Commit
git commit -m "Add initial master data files"

# Push
git push origin main
```

### Step 2.4: Test GitHub Actions

1. Go to GitHub.com
2. Navigate to your repository
3. Click **Actions** tab (top menu)
4. You should see "Daily Data Update Pipeline"
5. Click **Run workflow** (right side)
6. Click green **Run workflow** button
7. Watch it run (takes 2-3 minutes)

**Expected output:**
```
✅ Checkout repository
✅ Set up Python 3.10
✅ Install dependencies
✅ Create data directories
✅ Run ETL Pipeline
✅ Verify data files
✅ Commit updated data
```

If successful, you'll see green checkmarks! ✅

---

## 🌐 PART 3: Deploy to Streamlit Cloud

### Step 3.1: Sign Up for Streamlit Cloud

1. Go to https://share.streamlit.io/
2. Click **Sign up**
3. Choose **Continue with GitHub**
4. Authorize Streamlit to access your GitHub repos

**This is FREE!** No credit card needed.

### Step 3.2: Deploy Your App

1. Click **New app** (top right)
2. Fill in the form:
   - **Repository:** `AZhuk30/WeatherImpactOnCarAccidents`
   - **Branch:** `main`
   - **Main file path:** `dashboard/app.py`
   - **App URL:** (optional) `weatherimpact-nyc` or leave blank

3. Click **Deploy!**

4. Wait 2-5 minutes for deployment

**You'll get a URL like:**
```
https://weatherimpact-nyc.streamlit.app
```

or

```
https://weatherimpactoncaraccidents-xyz123.streamlit.app
```

### Step 3.3: Test Your Dashboard

1. Open the Streamlit URL
2. You should see your dashboard with data!
3. Try the filters (Borough, Date Range, Weather)
4. Check that visualizations work
5. Try downloading data

---

## ✅ PART 4: Verify Everything Works

### Test 1: Dashboard Shows Data

**On your Streamlit URL:**
- ✅ Dashboard loads without errors
- ✅ Metrics show numbers
- ✅ Charts display
- ✅ Filters work
- ✅ Data exports work

### Test 2: GitHub Actions Runs Daily

**In GitHub:**
1. Go to Actions tab
2. You should see scheduled runs
3. Next run shows as "Scheduled for..."

### Test 3: Data Updates Automatically

**Tomorrow:**
1. Check GitHub Actions ran at 2 AM UTC
2. Look for new commit: "🤖 Auto-update: Data refresh..."
3. Visit your Streamlit dashboard
4. Data should be updated (check date range in sidebar)

---

## 📊 PART 5: Customize Your Deployment

### Change Update Schedule

Edit `.github/workflows/daily_update.yml`:

```yaml
schedule:
  # Every day at 2 AM UTC
  - cron: '0 2 * * *'
  
  # Every 6 hours:
  # - cron: '0 */6 * * *'
  
  # Every Monday at 8 AM:
  # - cron: '0 8 * * 1'
```

### Change Data Window

In GitHub Actions, set the `days` input:

```yaml
workflow_dispatch:
  inputs:
    date_range:
      default: '30'  # Change to 60, 90, etc.
```

Or run manually:
```bash
python run_pipeline.py --days 60
```

---

## 🔍 PART 6: Monitoring & Troubleshooting

### View Pipeline Logs

**In GitHub:**
1. Actions tab
2. Click any run
3. Click "Run ETL Pipeline" step
4. See full output

**Download logs:**
1. Scroll to bottom of run page
2. "Artifacts" section
3. Download `pipeline-logs-XXX`

### Common Issues & Fixes

#### Issue 1: "Data Files Not Found"

**Problem:** Dashboard shows error about missing files

**Solution:**
```bash
# Run pipeline locally
python run_pipeline.py --days 30

# Commit the files
git add data/processed/*.csv
git commit -m "Update master data files"
git push
```

#### Issue 2: GitHub Actions Fails

**Problem:** Red X in Actions tab

**Solution:**
1. Click the failed run
2. Check which step failed
3. Common fixes:
   - API timeout → Run again (it retries)
   - Python error → Check requirements.txt
   - Git conflict → Pull latest changes first

#### Issue 3: Dashboard Shows Old Data

**Problem:** Streamlit shows yesterday's data

**Solution:**
```bash
# On Streamlit Cloud:
# Click hamburger menu (☰) → Reboot app
```

Or wait 1 hour (cache expires automatically)

---

## 📝 PART 7: Update Your Project Documentation

### Add to README.md

```markdown
## 🌐 Live Dashboard

View the live dashboard: **[https://weatherimpact-nyc.streamlit.app](https://your-url-here.streamlit.app)**

- Updates automatically daily at 2 AM UTC
- Shows 30+ days of historical data
- Interactive filters and visualizations
- Free public access

## 🤖 Automation

This project uses GitHub Actions for automated data updates:

- **Schedule:** Daily at 2 AM UTC
- **Process:** Fetches latest weather and collision data from APIs
- **Output:** Updates master CSV files in repository
- **Hosting:** Dashboard hosted on Streamlit Cloud (free tier)

### Manual Run

To run the pipeline manually:

```bash
# Default: 30 days
python run_pipeline.py

# Custom date range
python run_pipeline.py --days 60

# Specific dates
python run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-31
```

## 📊 Data Accumulation

The pipeline maintains master CSV files that grow over time:
- `data/processed/weather_master.csv` - All weather data
- `data/processed/collisions_master.csv` - All collision data

Each run adds new data and removes duplicates automatically.
```

---

## 🎓 PART 8: For Your Project Submission

### What to Include in Design Document

**Architecture Diagram:**
```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   APIs      │ ───> │ GitHub       │ ───> │ Streamlit   │
│ (Data       │      │ Actions      │      │ Cloud       │
│  Sources)   │      │ (Daily ETL)  │      │ (Dashboard) │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ↓
                     ┌──────────────┐
                     │ GitHub Repo  │
                     │ (CSV Files)  │
                     └──────────────┘
```

**Deployment Section:**
```markdown
## Deployment Architecture

Our system uses a cloud-native, serverless architecture:

1. **Data Collection (GitHub Actions)**
   - Serverless compute via GitHub Actions
   - Runs daily at 2 AM UTC
   - No infrastructure to maintain
   - Automatic retry on failure

2. **Data Storage (GitHub Repository)**
   - Master CSV files stored in Git
   - Version controlled
   - Accessible to dashboard
   - Free unlimited storage for CSV files

3. **Visualization (Streamlit Cloud)**
   - Public dashboard at [URL]
   - Auto-updates when data changes
   - Free tier (1 app, unlimited viewers)
   - Built-in caching for performance

4. **Local Development (MySQL)**
   - Full database for SQL demonstrations
   - Star schema implementation
   - Used for testing and development
```

### What to Demo

**Live Demo Flow (5 minutes):**

1. **Show Dashboard** (1 min)
   - Open public URL
   - Show it works in real-time
   - Interact with filters

2. **Show GitHub Actions** (1 min)
   - GitHub → Actions tab
   - "This runs automatically every day"
   - Show recent successful runs

3. **Show Code** (1 min)
   - Show `.github/workflows/daily_update.yml`
   - Explain the automation

4. **Show Data Files** (1 min)
   - Show `weather_master.csv`
   - Show it grows over time
   - Show commit history

5. **Show Local Database** (1 min)
   - MySQL schema
   - Run SQL query
   - Prove you know databases

---

## 📈 Success Metrics

After setup, you should have:

✅ **Automation**
- GitHub Actions runs daily
- No manual intervention needed
- Logs available for debugging

✅ **Public Dashboard**
- Accessible via URL
- Shows real data
- Updates automatically

✅ **Growing Dataset**
- Master files accumulate data
- 30+ days after first run
- 60+ days after a month

✅ **Professional Delivery**
- Production-ready deployment
- Scalable architecture
- Industry-standard tools

---

## 🎯 Next Steps

### After Initial Setup:

1. **Monitor for 3 days**
   - Check GitHub Actions runs successfully
   - Verify data is updating
   - Ensure dashboard stays current

2. **Add to Design Document**
   - Include Streamlit URL
   - Screenshot of dashboard
   - Explain deployment architecture

3. **Prepare Demo**
   - Practice showing live dashboard
   - Prepare to explain automation
   - Show logs and data growth

4. **Final Checks Before Submission**
   - Dashboard is public and working
   - GitHub Actions shows green checkmarks
   - All code is committed and pushed
   - README has deployment instructions

---

## 🆘 Getting Help

### GitHub Actions Not Running?

Check:
1. Workflow file is in `.github/workflows/`
2. File is named `daily_update.yml`
3. You have GitHub Actions enabled (Settings → Actions)
4. Workflow permissions are set (Settings → Actions → General → Read and write permissions)

### Streamlit Cloud Issues?

Check:
1. Files are committed to GitHub
2. `requirements.txt` is complete
3. `dashboard/app.py` exists
4. Master CSV files exist in repo

### Data Not Updating?

Check:
1. GitHub Actions completed successfully (green checkmark)
2. New commit shows in repository
3. Streamlit Cloud has rebooted (or wait 1 hour for cache)

---

## 🎉 Congratulations!

You now have a **production-ready, automated, cloud-deployed** data pipeline!

This demonstrates:
- ✅ ETL pipeline development
- ✅ Cloud deployment (Streamlit)
- ✅ CI/CD automation (GitHub Actions)
- ✅ Data engineering best practices
- ✅ Public web application
- ✅ Scalable architecture

**Share your dashboard URL with:**
- Your instructor
- Your team
- Future employers (on your resume!)

---

**Questions?** Check the troubleshooting section or review the code comments.

**Ready to submit?** Make sure all files are pushed to GitHub and your dashboard is live!
