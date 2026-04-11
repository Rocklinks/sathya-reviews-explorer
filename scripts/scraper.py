"""
Sathya Mobiles Review Scraper
Exact same pattern as the working Sathya Agency scraper.
Stores aggregate data in Hugging Face as sm.json
"""

import re
import json
import os
import asyncio
import traceback
import sys
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from huggingface_hub import HfApi

# ====================== CONFIG ======================
HF_REPO_ID = os.environ.get("HF_REPO", "RocklinKS/sathya-reviews")
HF_FILE = "sm.json"
MAX_CONCURRENT = 7
# ====================================================

BRANCHES = [
    {"id": 1, "name": "Tuticorin1", "place_id": "ChIJuwNfBb7vAzsR1Gk8166QIVE", "agm": "Tamilselvan J"},
    {"id": 2, "name": "Tuticorin2", "place_id": "ChIJUfzbg4L7AzsR4ikUKtp_sx4", "agm": "Tamilselvan J"},
    {"id": 3, "name": "Thisayanvilai1", "place_id": "ChIJJfTo4pN_BDsR7pbTj8_dhEU", "agm": "Tamilselvan J"},
    {"id": 4, "name": "Eral1", "place_id": "ChIJkyXwiO6NAzsR6Wmmcpg5axg", "agm": "Tamilselvan J"},
    {"id": 5, "name": "Sattur2", "place_id": "ChIJFbxGS_XLBjsRPyxhjRSDW1A", "agm": "Tamilselvan J"},
    {"id": 6, "name": "Villathikullam1", "place_id": "ChIJueDIMftbATsR5FHkWT0DMtY", "agm": "Tamilselvan J"},
    {"id": 7, "name": "Tenkasi1", "place_id": "ChIJX-SiDHopBDsR9WQZBK9_y-Q", "agm": "Ashok Kumar"},
    {"id": 8, "name": "Surandai1", "place_id": "ChIJhXjnmVqdBjsRYdhg7Z2Use0", "agm": "Ashok Kumar"},
    {"id": 9, "name": "Ambasamudram1", "place_id": "ChIJLReO2yI5BDsRJUI3MdjudKU", "agm": "Ashok Kumar"},
    {"id": 10, "name": "Rajapalayam1", "place_id": "ChIJM6i7syvoBjsROzyHWZO4iDw", "agm": "Ashok Kumar"},
    {"id": 11, "name": "Virudunagar1", "place_id": "ChIJpVZPddUtATsRNNu8qXIS6eQ", "agm": "Ashok Kumar"},
    {"id": 12, "name": "Puliyangudi1", "place_id": "ChIJPWqGUIKRBjsR3pR0lzk8zk4", "agm": "Ashok Kumar"},
    {"id": 13, "name": "Sankarankovil1", "place_id": "ChIJ9wmKdpGXBjsRhtEpPmbpYys", "agm": "Ashok Kumar"},
    {"id": 14, "name": "Sivakasi1", "place_id": "ChIJwdC-rYvPBjsRx0PfQwzW3hw", "agm": "Ashok Kumar"},
    {"id": 15, "name": "Sivakasi2", "place_id": "ChIJZ2o0g9nPBjsRgCcmzN1Colk", "agm": "Ashok Kumar"},
    {"id": 16, "name": "Tirunelveli1", "place_id": "ChIJhbSc2X_3AzsR9HvY0PLuBlo", "agm": "Senthil"},
    {"id": 17, "name": "Tirunelveli2", "place_id": "ChIJkdCXuEsRBDsR9A-LXevyGx0", "agm": "Senthil"},
    {"id": 18, "name": "Valliyur1", "place_id": "ChIJqa9AFoNnBDsR8pKyv1BnCK4", "agm": "Senthil"},
    {"id": 19, "name": "Nagercoil1", "place_id": "ChIJqZLlE__xBDsRADMABwteyfA", "agm": "Senthil"},
    {"id": 20, "name": "Nagercoil2", "place_id": "ChIJOwGck17xBDsRQOFyQQvObdg", "agm": "Senthil"},
    {"id": 21, "name": "Marthandam", "place_id": "ChIJqQL4BARVBDsRCIedlksC1fg", "agm": "Senthil"},
]

# ─────────────────────────────────────────────
# HF: load existing sm.json
# ─────────────────────────────────────────────
def load_data():
    token = os.environ.get("HF_TOKEN", "")
    if not token:
        print(" [Data] No HF_TOKEN — starting fresh.")
        return {"branches": {}, "daily": {}, "logs": []}

    try:
        import urllib.request
        url = f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/main/{HF_FILE}?download=true"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Cache-Control": "no-cache",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            days = len(data.get("daily", {}))
            print(f" [Data] Loaded from HF: {days} days of history")
            return data
    except Exception as e:
        code = getattr(e, "code", None)
        if code in (404, 401):
            print(f" [Data] HF file not found ({code}) — starting fresh.")
        else:
            print(f" [Data] HF GET error: {e} — starting fresh.")
        return {"branches": {}, "daily": {}, "logs": []}


# ─────────────────────────────────────────────
# HF: save sm.json  ← FIXED VERSION
# ─────────────────────────────────────────────
def save_data(data):
    token = os.environ.get("HF_TOKEN", "").strip()
    if not token:
        print(" [Save] ❌ No HF_TOKEN found in environment variables — skipping upload.")
        return

    try:
        api = HfApi(token=token)

        # Step 1: Create repo if it doesn't exist
        print(f" [Save] Checking/creating repo: {HF_REPO_ID} (dataset)")
        repo_url = api.create_repo(
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            private=False,      # Change to True if you want it private
            exist_ok=True
        )
        print(f" [Save] Repository ready → {repo_url}")

        # Step 2: Upload the file
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            tmp_path = f.name

        print(f" [Save] Uploading sm.json ...")
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo=HF_FILE,   # "sm.json"
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            commit_message=f"Update sm.json - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        )
        os.unlink(tmp_path)
        print(f" [Save] ✅ SUCCESS: sm.json uploaded to https://huggingface.co/datasets/{HF_REPO_ID}")

    except Exception as e:
        print(f" [Save] ❌ FAILED to save to Hugging Face: {type(e).__name__}: {e}")
        traceback.print_exc()
        print("\nPossible fixes:")
        print("1. Make sure HF_TOKEN has 'Write' access (not just Read)")
        print("2. Create the repo manually at https://huggingface.co/new-dataset")
        print(f"3. Repo name must be exactly: {HF_REPO_ID}")


# ─────────────────────────────────────────────
# SCRAPE CORE
# ─────────────────────────────────────────────
async def _try_scrape(page, place_id, wait_ms=3000):
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    count = None
    stars = None
    await page.goto(url, wait_until="domcontentloaded", timeout=35000)
    await page.wait_for_timeout(wait_ms)
    content = await page.content()

    # Review count
    for sel in ['[aria-label*="reviews"]', '[aria-label*="Reviews"]', 'button[jsaction*="review"]']:
        els = await page.locator(sel).all()
        for el in els:
            label = await el.get_attribute("aria-label") or ""
            m = re.search(r"([\d,]+)", label)
            if m:
                count = int(m.group(1).replace(",", ""))
                break
        if count:
            break

    # Star rating
    for sel in ['[aria-label*="stars"]', 'span[aria-label*="stars"]', '[aria-label*="star rating"]']:
        els = await page.locator(sel).all()
        for el in els:
            label = await el.get_attribute("aria-label") or ""
            m = re.search(r"(\d\.\d)", label)
            if m:
                stars = float(m.group(1))
                break
        if stars:
            break

    # Fallbacks...
    if not count:
        for pat in [r'([\d,]+)\s*reviews?', r'"reviewCount"["\s:]+(\d+)', r'(\d[\d,]{2,})\s*Google review']:
            m = re.search(pat, content, re.IGNORECASE)
            if m:
                v = int(m.group(1).replace(",", ""))
                if v > 10:
                    count = v
                    break

    if not stars:
        for pat in [r'"ratingValue":"([\d.]+)"', r'(\d\.\d)\s*(?:stars|out of 5)']:
            m = re.search(pat, content, re.IGNORECASE)
            if m:
                try:
                    v = float(m.group(1))
                    if 1.0 <= v <= 5.0:
                        stars = v
                        break
                except ValueError:
                    pass

    return count, stars


async def scrape_place(context, place_id, name, max_retries=3):
    wait_times = [3000, 5000, 8000]
    pause_times = [0, 3, 5]
    for attempt in range(1, max_retries + 1):
        page = None
        try:
            if attempt > 1:
                print(f" ↺ Retry {attempt} for {name}...", flush=True)
                await asyncio.sleep(pause_times[attempt - 1])
            page = await context.new_page()
            count, stars = await _try_scrape(page, place_id, wait_ms=wait_times[attempt - 1])
            if count is not None:
                await page.close()
                return count, stars
            print(f" ⚠ Attempt {attempt}: no count for {name}", flush=True)
        except Exception as e:
            print(f" ⚠ Attempt {attempt} error for {name}: {e}", flush=True)
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
    return None, None


def compute_monthly(data, bid, snap_date, daily_count):
    month_prefix = snap_date[:7]
    same_month_dates = sorted(
        [d for d in data.get("daily", {}) if d.startswith(month_prefix) and d < snap_date],
        reverse=True
    )
    prev_monthly = data["daily"][same_month_dates[0]].get(bid, {}).get("monthly", 0) if same_month_dates else 0
    return prev_monthly + daily_count


# ─────────────────────────────────────────────
# MAIN RUN
# ─────────────────────────────────────────────
async def run():
    IST_OFFSET = timedelta(hours=5, minutes=30)
    now_ist = datetime.utcnow() + IST_OFFSET
    snap_date = now_ist.strftime("%Y-%m-%d")
    run_time = datetime.utcnow().isoformat()

    print("=" * 58)
    print(" SATHYA MOBILES — Review Scraper")
    print(f" Snap date : {snap_date} (IST)")
    print(f" Branches  : {len(BRANCHES)} | Concurrency: {MAX_CONCURRENT}")
    print("=" * 58)

    data = load_data()

    # Build baseline
    all_dates_before = sorted([d for d in data.get("daily", {}) if d < snap_date], reverse=True)
    baseline_date = all_dates_before[0] if all_dates_before else None
    baseline_snap = data["daily"].get(baseline_date, {}) if baseline_date else {}
    baseline = {str(b["id"]): baseline_snap.get(str(b["id"]), {}).get("total_snap", 
                data.get("branches", {}).get(str(b["id"]), {}).get("overall", 0)) 
                for b in BRANCHES}

    print(f"\n Baseline date : {baseline_date or 'none (first run)'}\n")

    results = {}
    success = 0
    failed = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            locale="en-IN",
            viewport={"width": 1280, "height": 800},
        )

        # Warm-up
        try:
            page = await context.new_page()
            await page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            await page.close()
            print(" [warm-up] Browser ready ✓\n")
        except Exception:
            print(" [warm-up] Skipped\n")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async def bounded_scrape(branch):
            nonlocal success
            async with semaphore:
                bid = str(branch["id"])
                name = branch["name"]
                print(f" [{branch['id']:02d}/{len(BRANCHES)}] {name:<22}", end=" ", flush=True)
                
                live, stars = await scrape_place(context, branch["place_id"], name)
                
                if live is not None:
                    prev = baseline.get(bid, 0)
                    daily = live - prev
                    delta_str = f"+{daily}" if daily >= 0 else str(daily)
                    stars_str = f"{stars}★" if stars else "—"
                    print(f"→ {live:,} total {delta_str} new {stars_str} ✓")
                    results[bid] = {"live": live, "stars": stars, "daily_count": daily}
                    success += 1
                else:
                    failed.append(name)
                    print("→ FAILED ✗")
                await asyncio.sleep(0.6)

        await asyncio.gather(*[bounded_scrape(b) for b in BRANCHES])
        await browser.close()

    # Process results
    if snap_date not in data["daily"]:
        data["daily"][snap_date] = {}

    for b in BRANCHES:
        bid = str(b["id"])
        if bid not in results:
            prev_snap = baseline_snap.get(bid, {})
            data["daily"][snap_date][bid] = {
                "total_snap": prev_snap.get("total_snap", 0),
                "daily_count": 0,
                "monthly": compute_monthly(data, bid, snap_date, 0),
                "star_rating": prev_snap.get("star_rating", 0),
            }
            continue

        r = results[bid]
        live = r["live"]
        stars = r["stars"]
        daily = r["daily_count"]
        final_stars = stars if stars else data.get("branches", {}).get(bid, {}).get("star_rating", 0)
        monthly = compute_monthly(data, bid, snap_date, daily)

        data["daily"][snap_date][bid] = {
            "total_snap": live,
            "daily_count": daily,
            "monthly": monthly,
            "star_rating": final_stars,
        }
        data["branches"][bid] = {
            "id": b["id"],
            "name": b["name"],
            "agm": b["agm"],
            "overall": live,
            "star_rating": final_stars,
            "monthly": monthly,
        }

    # Log
    data.setdefault("logs", []).insert(0, {
        "ran_at": run_time,
        "snap_date": snap_date,
        "baseline_date": baseline_date,
        "success": success,
        "failed": len(failed),
        "failed_names": failed,
    })
    data["logs"] = data["logs"][:60]
    data["last_updated"] = run_time

    # Summary
    total_reviews = sum(b.get("overall", 0) for b in data["branches"].values())
    daily_total = sum(data["daily"].get(snap_date, {}).get(str(b["id"]), {}).get("daily_count", 0) for b in BRANCHES)

    print(f"\n{'─' * 58}")
    print(f" ✅ Scraped : {success}/{len(BRANCHES)} branches")
    if failed:
        print(f" ❌ Failed  : {', '.join(failed)}")
    print(f" Total     : {total_reviews:,} reviews")
    print(f" Today     : +{daily_total} new reviews")
    print(f"{'─' * 58}\n")

    # Upload to HF
    save_data(data)

    print(f"\n✅ Done: {success}/{len(BRANCHES)} branches saved for {snap_date}")

    if success < len(BRANCHES) * 0.5:
        print("❌ Over 50% failed — marking as failed.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print(f"\n[FATAL] Scraper crashed: {e}")
        traceback.print_exc()
        sys.exit(1)
