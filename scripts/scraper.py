"""
╔══════════════════════════════════════════════════════════════════╗
║   SATHYA MOBILES — Nightly Review Scraper (Python Async)        ║
║   Same proven pattern as Sathya Agency scraper                  ║
║   Stores data in Hugging Face as sm.json                        ║
╠══════════════════════════════════════════════════════════════════╣
║   pip install playwright huggingface_hub                        ║
║   python -m playwright install chromium                         ║
╚══════════════════════════════════════════════════════════════════╝
"""

import re
import os
import sys
import json
import asyncio
from datetime import datetime, timezone, timedelta

from playwright.async_api import async_playwright

try:
    from huggingface_hub import HfApi
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
HF_TOKEN   = os.environ.get("HF_TOKEN", "")
HF_REPO    = os.environ.get("HF_REPO",  "RocklinKS/sathya-reviews")
HF_FILE    = "sm.json"

WORKERS    = 4      # parallel browser contexts
TIMEOUT_MS = 25000  # page navigation timeout
WAIT_MS    = 12000  # waitForSelector timeout
DELAY_S    = 0.8    # small delay between branches per worker

# ─────────────────────────────────────────────
# BRANCHES: (id, name, agm, place_id)
# ─────────────────────────────────────────────
BRANCHES = [
    (1,  "Tuticorin1",        "Tamilselvan J",  "ChIJuwNfBb7vAzsR1Gk8166QIVE"),
    (2,  "Tuticorin2",        "Tamilselvan J",  "ChIJUfzbg4L7AzsR4ikUKtp_sx4"),
    (3,  "Thisayanvilai1",    "Tamilselvan J",  "ChIJJfTo4pN_BDsR7pbTj8_dhEU"),
    (4,  "Eral1",             "Tamilselvan J",  "ChIJkyXwiO6NAzsR6Wmmcpg5axg"),
    (5,  "Sattur2",           "Tamilselvan J",  "ChIJFbxGS_XLBjsRPyxhjRSDW1A"),
    (6,  "Villathikullam1",   "Tamilselvan J",  "ChIJueDIMftbATsR5FHkWT0DMtY"),
    (7,  "Tenkasi1",          "Ashok Kumar",    "ChIJX-SiDHopBDsR9WQZBK9_y-Q"),
    (8,  "Surandai1",         "Ashok Kumar",    "ChIJhXjnmVqdBjsRYdhg7Z2Use0"),
    (9,  "Ambasamudram1",     "Ashok Kumar",    "ChIJLReO2yI5BDsRJUI3MdjudKU"),
    (10, "Rajapalayam1",      "Ashok Kumar",    "ChIJM6i7syvoBjsROzyHWZO4iDw"),
    (11, "Virudunagar1",      "Ashok Kumar",    "ChIJpVZPddUtATsRNNu8qXIS6eQ"),
    (12, "Puliyangudi1",      "Ashok Kumar",    "ChIJPWqGUIKRBjsR3pR0lzk8zk4"),
    (13, "Sankarankovil1",    "Ashok Kumar",    "ChIJ9wmKdpGXBjsRhtEpPmbpYys"),
    (14, "Sivakasi1",         "Ashok Kumar",    "ChIJwdC-rYvPBjsRx0PfQwzW3hw"),
    (15, "Sivakasi2",         "Ashok Kumar",    "ChIJZ2o0g9nPBjsRgCcmzN1Colk"),
    (16, "Tirunelveli1",      "Senthil",        "ChIJhbSc2X_3AzsR9HvY0PLuBlo"),
    (17, "Tirunelveli2",      "Senthil",        "ChIJkdCXuEsRBDsR9A-LXevyGx0"),
    (18, "Valliyur1",         "Senthil",        "ChIJqa9AFoNnBDsR8pKyv1BnCK4"),
    (19, "Nagercoil1",        "Senthil",        "ChIJqZLlE__xBDsRADMABwteyfA"),
    (20, "Nagercoil2",        "Senthil",        "ChIJOwGck17xBDsRQOFyQQvObdg"),
    (21, "Marthandam",        "Senthil",        "ChIJqQL4BARVBDsRCIedlksC1fg"),
]

# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────
def get_ist_date():
    """Return today's date string in IST (UTC+5:30)."""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")

def iso_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# ─────────────────────────────────────────────
# SCRAPE ONE PLACE — async, waits for real DOM elements
# ─────────────────────────────────────────────
async def scrape_place(page, place_id: str):
    """Returns (review_count, star_rating). Either may be None on failure."""
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    review_count = None
    star_rating  = None

    try:
        # Navigate — wait for network to be idle so JS has rendered
        await page.goto(url, wait_until="networkidle", timeout=TIMEOUT_MS)

        # Wait for the reviews element to actually appear in DOM
        # This is the key — we don't sleep blindly, we wait for the element
        try:
            await page.wait_for_selector(
                '[aria-label*="reviews"], [aria-label*="Reviews"]',
                timeout=WAIT_MS
            )
        except Exception:
            # Fallback: wait for star rating element
            try:
                await page.wait_for_selector(
                    '[aria-label*="stars"], [aria-label*="star"]',
                    timeout=WAIT_MS
                )
            except Exception:
                pass  # will try content fallback below

        # ── Review count via aria-label ──
        for sel in [
            '[aria-label*="reviews"]',
            '[aria-label*="Reviews"]',
            'button[jsaction*="review"]',
        ]:
            if review_count:
                break
            for el in await page.locator(sel).all():
                label = await el.get_attribute("aria-label") or ""
                m = re.search(r"([\d,]+)", label)
                if m:
                    val = int(m.group(1).replace(",", ""))
                    if val > 0:
                        review_count = val
                        break

        # ── Star rating via aria-label ──
        for sel in [
            '[aria-label*="stars"]',
            '[aria-label*="star"]',
            'span[aria-label*="stars"]',
        ]:
            if star_rating:
                break
            for el in await page.locator(sel).all():
                label = await el.get_attribute("aria-label") or ""
                m = re.search(r"(\d\.\d)", label)
                if m:
                    star_rating = float(m.group(1))
                    break

        # ── Fallback: parse raw HTML ──
        content = await page.content()

        if not review_count:
            for pat in [
                r"([\d,]+)\s*reviews?",
                r'"reviewCount"["\s:]+(\d+)',
                r"(\d[\d,]{2,})\s*Google review",
            ]:
                m = re.search(pat, content, re.IGNORECASE)
                if m:
                    val = int(m.group(1).replace(",", ""))
                    if val > 10:
                        review_count = val
                        break

        if not star_rating:
            for pat in [
                r'"aggregateRating".*?"ratingValue":\s*"?([\d.]+)',
                r"(\d\.\d)\s*(?:out of 5|stars)",
                r'"ratingValue":"([\d.]+)"',
            ]:
                m = re.search(pat, content, re.IGNORECASE)
                if m:
                    try:
                        val = float(m.group(1))
                        if 1.0 <= val <= 5.0:
                            star_rating = val
                            break
                    except ValueError:
                        pass

    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:100]}")

    return review_count, star_rating


# ─────────────────────────────────────────────
# WORKER — one browser context, processes its chunk
# Retries each branch immediately on failure (no sleep)
# ─────────────────────────────────────────────
async def run_worker(browser, chunk, results: dict):
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="en-IN",
        viewport={"width": 1280, "height": 800},
    )
    page = await context.new_page()

    for (bid, name, agm, place_id) in chunk:
        label = f"  [{bid:02d}] {name:<24} → "
        print(label, end="", flush=True)

        review_count = None
        star_rating  = None

        # Immediate retry on failure — no sleep between attempts
        for attempt in range(1, 3):
            review_count, star_rating = await scrape_place(page, place_id)
            if review_count is not None:
                break
            if attempt == 1:
                print("[retry] ", end="", flush=True)

        stars_str = f"{star_rating}⭐" if star_rating else "—"
        if review_count is not None:
            print(f"{review_count:,} reviews  {stars_str}  ✓")
            results[bid] = {"review_count": review_count, "star_rating": star_rating}
        else:
            print(f"FAILED ✗")
            results[bid] = {"review_count": None, "star_rating": star_rating}

        await asyncio.sleep(DELAY_S)

    await context.close()


# ─────────────────────────────────────────────
# HF: fetch existing sm.json
# ─────────────────────────────────────────────
def hf_get_existing():
    if not HF_AVAILABLE or not HF_TOKEN:
        return None
    try:
        import urllib.request
        url = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main/{HF_FILE}?download=true"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {HF_TOKEN}",
            "Cache-Control": "no-cache",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        code = getattr(e, "code", None)
        if code in (404, 401):
            print(f"  ℹ️  HF: file not found ({code}) — starting fresh.")
        else:
            print(f"  ⚠️  HF GET error: {e}")
        return None


# ─────────────────────────────────────────────
# HF: upload sm.json
# ─────────────────────────────────────────────
def hf_put(json_str: str):
    if not HF_AVAILABLE or not HF_TOKEN:
        print("  ⚠️  huggingface_hub not available or HF_TOKEN missing — skipping upload.")
        return False
    try:
        import tempfile
        api = HfApi(token=HF_TOKEN)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
            f.write(json_str)
            tmp_path = f.name
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo=HF_FILE,
            repo_id=HF_REPO,
            repo_type="dataset",
            commit_message=f"data: update {HF_FILE} {iso_now()}",
        )
        os.unlink(tmp_path)
        return True
    except Exception as e:
        print(f"  ❌ HF upload error: {e}")
        return False


# ─────────────────────────────────────────────
# COMPUTE MONTHLY
# ─────────────────────────────────────────────
def compute_monthly(data: dict, bid: str, today: str, today_daily: int) -> int:
    month_prefix = today[:7]
    total = today_daily
    for date_str, day_snap in (data.get("daily") or {}).items():
        if date_str.startswith(month_prefix) and date_str != today:
            total += (day_snap.get(bid) or {}).get("daily_count", 0)
    return total


# ─────────────────────────────────────────────
# BUILD / UPDATE JSON
# ─────────────────────────────────────────────
def build_json(existing, scrape_results: dict, today: str):
    data = existing or {
        "last_updated": iso_now(),
        "branches": {},
        "daily": {},
        "logs": [],
    }

    # Ensure all branches exist in master map
    for (bid, name, agm, _) in BRANCHES:
        key = str(bid)
        if key not in data["branches"]:
            data["branches"][key] = {"id": bid, "name": name, "agm": agm, "overall": 0, "star_rating": 0}

    # Find previous date (exclude today)
    known_dates = sorted(d for d in (data.get("daily") or {}).keys() if d != today)
    prev_date   = known_dates[-1] if known_dates else None

    if today not in data["daily"]:
        data["daily"][today] = {}

    success_count = 0
    fail_count    = 0
    failed_names  = []

    for (bid, name, agm, _) in BRANCHES:
        key = str(bid)
        res = scrape_results.get(bid, {})
        live_total = res.get("review_count")
        star       = res.get("star_rating")

        if live_total is None:
            fail_count += 1
            failed_names.append(name)
            prev_snap = (data["daily"].get(prev_date) or {}).get(key, {}) if prev_date else {}
            data["daily"][today][key] = {
                "total_snap":  prev_snap.get("total_snap", 0),
                "daily_count": 0,
                "monthly":     compute_monthly(data, key, today, 0),
                "star_rating": star or prev_snap.get("star_rating", 0),
            }
            continue

        success_count += 1
        prev_total = 0
        if prev_date:
            prev_total = (data["daily"].get(prev_date) or {}).get(key, {}).get("total_snap", 0)
        else:
            prev_total = data["branches"].get(key, {}).get("overall", 0)

        daily_count = max(0, live_total - prev_total)
        monthly     = compute_monthly(data, key, today, daily_count)

        data["daily"][today][key] = {
            "total_snap":  live_total,
            "daily_count": daily_count,
            "monthly":     monthly,
            "star_rating": star or 0,
        }

        data["branches"][key]["overall"]     = live_total
        data["branches"][key]["star_rating"] = star or data["branches"][key].get("star_rating", 0)
        data["branches"][key]["monthly"]     = monthly

    log_entry = {
        "ran_at":        iso_now(),
        "snap_date":     today,
        "baseline_date": prev_date or "none",
        "success":       success_count,
        "failed":        fail_count,
        "failed_names":  failed_names,
    }
    data["logs"]         = ([log_entry] + (data.get("logs") or []))[:60]
    data["last_updated"] = iso_now()

    return data, success_count, fail_count, failed_names


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    today   = get_ist_date()
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 62)
    print("  SATHYA MOBILES — Python Async Review Scraper")
    print(f"  Running : {now_str} UTC")
    print(f"  Date    : {today} (IST)")
    print(f"  Branches: {len(BRANCHES)}  |  Workers: {WORKERS}")
    print("=" * 62)

    # ── Fetch existing data from HF ──
    print("\n📥 Fetching existing data from Hugging Face...")
    existing = hf_get_existing()
    if existing:
        days = len(existing.get("daily") or {})
        print(f"  ✅ Found: {days} days of history")
    else:
        print("  ℹ️  Starting fresh")

    # ── Split branches into worker chunks ──
    chunks = [[] for _ in range(WORKERS)]
    for i, branch in enumerate(BRANCHES):
        chunks[i % WORKERS].append(branch)

    print(f"\n🌐 Launching {WORKERS} parallel Chromium contexts...\n")

    scrape_results = {}
    start_time = asyncio.get_event_loop().time()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )

        # Run all workers in parallel
        await asyncio.gather(*[
            run_worker(browser, chunk, scrape_results)
            for chunk in chunks
            if chunk
        ])

        await browser.close()

    elapsed   = asyncio.get_event_loop().time() - start_time
    success_n = sum(1 for r in scrape_results.values() if r["review_count"] is not None)
    fail_n    = len(BRANCHES) - success_n

    print("\n" + "─" * 62)
    print(f"  ✅ Done: {success_n}/{len(BRANCHES)} in {elapsed:.1f}s")
    if fail_n:
        failed = [name for (bid, name, _, __) in BRANCHES if scrape_results.get(bid, {}).get("review_count") is None]
        print(f"  ❌ Failed: {', '.join(failed)}")

    # ── Build JSON ──
    print("\n📊 Building JSON...")
    data, success_count, fail_count, failed_names = build_json(existing, scrape_results, today)

    total_reviews = sum(b.get("overall", 0) for b in data["branches"].values())
    daily_total   = sum(
        (data["daily"].get(today) or {}).get(str(bid), {}).get("daily_count", 0)
        for (bid, _, __, ___) in BRANCHES
    )
    print(f"  Total reviews : {total_reviews:,}")
    print(f"  New today     : +{daily_total}")

    # ── Upload to HF ──
    if not HF_TOKEN:
        print("\n⚠️  HF_TOKEN not set — skipping upload.")
    else:
        print("\n📤 Uploading to Hugging Face...")
        ok = hf_put(json.dumps(data, indent=2))
        if ok:
            print(f"  ✅ Uploaded → hf://datasets/{HF_REPO}/{HF_FILE}")
        else:
            print("  ❌ Upload failed.")
            sys.exit(1)

    print("\n" + "=" * 62)
    print(f"  🎉 DONE  |  ✅ {success_count}  ❌ {fail_count}  ⏱ {elapsed:.1f}s")
    print("=" * 62 + "\n")

    if fail_count >= len(BRANCHES) * 0.5:
        print("  ❌ Over 50% branches failed.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
