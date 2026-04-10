/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║   SATHYA MOBILES — Nightly Review Scraper (Node.js)         ║
 * ║   Uses Playwright with parallel workers for max speed        ║
 * ║   Stores data in Hugging Face as sm.json                     ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const { chromium } = require("playwright");
const https = require("https");
const http = require("http");

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const HF_TOKEN   = process.env.HF_TOKEN || "";
const HF_REPO    = process.env.HF_REPO  || "RocklinKS/sathya-reviews";
const HF_FILE    = "sm.json";
const WORKERS    = 4;   // parallel browser contexts
const TIMEOUT_MS = 22000;
const DELAY_MS   = 900; // between requests per worker

// ─────────────────────────────────────────────
// BRANCHES: [id, name, agm, placeId]
// ─────────────────────────────────────────────
const BRANCHES = [
  [1,  "Tuticorin1",        "Tamilselvan J",  "ChIJuwNfBb7vAzsR1Gk8166QIVE"],
  [2,  "Tuticorin2",        "Tamilselvan J",  "ChIJUfzbg4L7AzsR4ikUKtp_sx4"],
  [3,  "Thisayanvilai1",    "Tamilselvan J",  "ChIJJfTo4pN_BDsR7pbTj8_dhEU"],
  [4,  "Eral1",             "Tamilselvan J",  "ChIJkyXwiO6NAzsR6Wmmcpg5axg"],
  [5,  "Sattur2",           "Tamilselvan J",  "ChIJFbxGS_XLBjsRPyxhjRSDW1A"],
  [6,  "Villathikullam1",   "Tamilselvan J",  "ChIJueDIMftbATsR5FHkWT0DMtY"],
  [7,  "Tenkasi1",          "Ashok Kumar",    "ChIJX-SiDHopBDsR9WQZBK9_y-Q"],
  [8,  "Surandai1",         "Ashok Kumar",    "ChIJhXjnmVqdBjsRYdhg7Z2Use0"],
  [9,  "Ambasamudram1",     "Ashok Kumar",    "ChIJLReO2yI5BDsRJUI3MdjudKU"],
  [10, "Rajapalayam1",      "Ashok Kumar",    "ChIJM6i7syvoBjsROzyHWZO4iDw"],
  [11, "Virudunagar1",      "Ashok Kumar",    "ChIJpVZPddUtATsRNNu8qXIS6eQ"],
  [12, "Puliyangudi1",      "Ashok Kumar",    "ChIJPWqGUIKRBjsR3pR0lzk8zk4"],
  [13, "Sankarankovil1",    "Ashok Kumar",    "ChIJ9wmKdpGXBjsRhtEpPmbpYys"],
  [14, "Sivakasi1",         "Ashok Kumar",    "ChIJwdC-rYvPBjsRx0PfQwzW3hw"],
  [15, "Sivakasi2",         "Ashok Kumar",    "ChIJZ2o0g9nPBjsRgCcmzN1Colk"],
  [16, "Tirunelveli1",      "Senthil",        "ChIJhbSc2X_3AzsR9HvY0PLuBlo"],
  [17, "Tirunelveli2",      "Senthil",        "ChIJkdCXuEsRBDsR9A-LXevyGx0"],
  [18, "Valliyur1",         "Senthil",        "ChIJqa9AFoNnBDsR8pKyv1BnCK4"],
  [19, "Nagercoil1",        "Senthil",        "ChIJqZLlE__xBDsRADMABwteyfA"],
  [20, "Nagercoil2",        "Senthil",        "ChIJOwGck17xBDsRQOFyQQvObdg"],
  [21, "Marthandam",        "Senthil",        "ChIJqQL4BARVBDsRCIedlksC1fg"],
];

// ─────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────
function getISTDateString() {
  const now = new Date();
  // IST = UTC+5:30
  const istOffset = 5.5 * 60 * 60 * 1000;
  const istDate = new Date(now.getTime() + istOffset);
  return istDate.toISOString().slice(0, 10); // "YYYY-MM-DD"
}

function isoNow() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function monthStart(dateStr) {
  return dateStr.slice(0, 7) + "-01";
}

// ─────────────────────────────────────────────
// SCRAPE SINGLE PLACE
// ─────────────────────────────────────────────
async function scrapePlace(page, placeId) {
  const url = `https://www.google.com/maps/place/?q=place_id:${placeId}`;
  let reviewCount = null;
  let starRating  = null;

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
    await page.waitForTimeout(2800);

    // ── Review count via aria-label ──
    const countSelectors = [
      '[aria-label*="reviews"]',
      '[aria-label*="Reviews"]',
      'button[jsaction*="review"]'
    ];
    for (const sel of countSelectors) {
      if (reviewCount) break;
      const els = await page.locator(sel).all();
      for (const el of els) {
        const label = await el.getAttribute("aria-label") || "";
        const m = label.match(/([\d,]+)/);
        if (m) {
          reviewCount = parseInt(m[1].replace(/,/g, ""), 10);
          break;
        }
      }
    }

    // ── Star rating via aria-label ──
    const starSelectors = [
      '[aria-label*="stars"]',
      '[aria-label*="star"]',
      'span[aria-label*="stars"]'
    ];
    for (const sel of starSelectors) {
      if (starRating) break;
      const els = await page.locator(sel).all();
      for (const el of els) {
        const label = await el.getAttribute("aria-label") || "";
        const m = label.match(/(\d\.\d)/);
        if (m) {
          starRating = parseFloat(m[1]);
          break;
        }
      }
    }

    // ── Fallback: parse page content ──
    const content = await page.content();

    if (!reviewCount) {
      const patterns = [
        /([\d,]+)\s*reviews?/i,
        /"reviewCount"["\s:]+(\d+)/i,
        /(\d[\d,]{2,})\s*Google review/i
      ];
      for (const pat of patterns) {
        const m = content.match(pat);
        if (m) {
          const val = parseInt(m[1].replace(/,/g, ""), 10);
          if (val > 10) { reviewCount = val; break; }
        }
      }
    }

    if (!starRating) {
      const patterns = [
        /"aggregateRating".*?"ratingValue":\s*"?([\d.]+)/i,
        /(\d\.\d)\s*(?:out of 5|stars)/i,
        /"ratingValue":"([\d.]+)"/i
      ];
      for (const pat of patterns) {
        const m = content.match(pat);
        if (m) {
          const val = parseFloat(m[1]);
          if (val >= 1.0 && val <= 5.0) { starRating = val; break; }
        }
      }
    }
  } catch (e) {
    console.error(`      ⚠️  Error scraping ${placeId}: ${e.message}`);
  }

  return { reviewCount, starRating };
}

// ─────────────────────────────────────────────
// WORKER — processes a chunk of branches
// ─────────────────────────────────────────────
async function runWorker(browser, chunk, results) {
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
      "AppleWebKit/537.36 (KHTML, like Gecko) " +
      "Chrome/122.0.0.0 Safari/537.36",
    locale: "en-IN",
    viewport: { width: 1280, height: 800 },
  });
  const page = await context.newPage();

  for (const [id, name, agm, placeId] of chunk) {
    process.stdout.write(`  [${String(id).padStart(2, "0")}] ${name.padEnd(24)} → `);
    const { reviewCount, starRating } = await scrapePlace(page, placeId);
    if (reviewCount !== null) {
      console.log(`${reviewCount.toLocaleString("en-IN")} reviews  ${starRating ? starRating + "⭐" : "—"}  ✓`);
      results[id] = { reviewCount, starRating };
    } else {
      console.log(`FAILED ✗  (stars: ${starRating ? starRating + "⭐" : "—"})`);
      results[id] = { reviewCount: null, starRating };
    }
    await sleep(DELAY_MS);
  }

  await context.close();
}

// ─────────────────────────────────────────────
// HF: fetch existing sm.json
// ─────────────────────────────────────────────
function hfGet() {
  return new Promise((resolve) => {
    const url = `https://huggingface.co/datasets/${HF_REPO}/resolve/main/${HF_FILE}?download=true`;
    const options = {
      headers: {
        Authorization: `Bearer ${HF_TOKEN}`,
        "Cache-Control": "no-cache"
      }
    };
    https.get(url, options, (res) => {
      if (res.statusCode === 404 || res.statusCode === 401) {
        console.log(`  ℹ️  HF file not found (${res.statusCode}), starting fresh.`);
        resolve(null);
        return;
      }
      let data = "";
      res.on("data", c => data += c);
      res.on("end", () => {
        try { resolve(JSON.parse(data)); }
        catch { resolve(null); }
      });
    }).on("error", (e) => {
      console.error("  ⚠️  HF GET error:", e.message);
      resolve(null);
    });
  });
}

// ─────────────────────────────────────────────
// HF: upload sm.json via commit API
// ─────────────────────────────────────────────
function hfPut(jsonStr) {
  return new Promise((resolve, reject) => {
    const content = Buffer.from(jsonStr, "utf8").toString("base64");
    const body = JSON.stringify({
      summary: `data: update ${HF_FILE} ${isoNow()}`,
      files: [{ path: HF_FILE, content }]
    });
    const options = {
      hostname: "huggingface.co",
      path: `/api/datasets/${HF_REPO}/commit/main`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${HF_TOKEN}`,
        "Content-Length": Buffer.byteLength(body)
      }
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", c => data += c);
      res.on("end", () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(true);
        } else {
          console.error("  HF PUT error:", res.statusCode, data.slice(0, 300));
          reject(new Error(`HF PUT failed: ${res.statusCode}`));
        }
      });
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

// ─────────────────────────────────────────────
// BUILD / UPDATE reviews JSON structure
// ─────────────────────────────────────────────
function buildJson(existing, scrapeResults, today) {
  // Initialise structure
  const data = existing || {
    last_updated: isoNow(),
    branches: {},
    daily: {},
    logs: []
  };

  // Ensure branches map exists
  for (const [id, name, agm] of BRANCHES) {
    if (!data.branches[String(id)]) {
      data.branches[String(id)] = { id, name, agm, overall: 0, star_rating: 0 };
    }
  }

  // Find yesterday's date in data for delta
  const knownDates = Object.keys(data.daily || {}).sort();
  const prevDate   = knownDates.length > 0 ? knownDates[knownDates.length - 1] : null;

  // Monthly sum: sum of daily_counts from 1st of month up to today
  // We accumulate from existing data
  const monthPrefix = today.slice(0, 7); // "YYYY-MM"

  if (!data.daily[today]) data.daily[today] = {};

  let successCount = 0;
  let failCount    = 0;
  const failedNames = [];

  for (const [id, name, agm] of BRANCHES) {
    const bid = String(id);
    const res = scrapeResults[id];

    if (!res || res.reviewCount === null) {
      failCount++;
      failedNames.push(name);
      // Keep previous snap if available
      const prevSnap = prevDate ? (data.daily[prevDate]?.[bid] || {}) : {};
      data.daily[today][bid] = {
        total_snap:  prevSnap.total_snap  || 0,
        daily_count: 0,
        monthly:     computeMonthly(data, bid, today, 0),
        star_rating: res?.starRating || prevSnap.star_rating || 0
      };
      continue;
    }

    successCount++;
    const liveTotal = res.reviewCount;
    const prevTotal = prevDate
      ? (data.daily[prevDate]?.[bid]?.total_snap || 0)
      : (data.branches[bid]?.overall || 0);

    const dailyCount = Math.max(0, liveTotal - prevTotal);
    const monthly    = computeMonthly(data, bid, today, dailyCount);

    data.daily[today][bid] = {
      total_snap:  liveTotal,
      daily_count: dailyCount,
      monthly:     monthly,
      star_rating: res.starRating || 0
    };

    // Update branches master record
    data.branches[bid].overall    = liveTotal;
    data.branches[bid].star_rating = res.starRating || data.branches[bid].star_rating || 0;
    data.branches[bid].monthly    = monthly;
  }

  // Prepend log entry
  const logEntry = {
    ran_at:       isoNow(),
    snap_date:    today,
    baseline_date: prevDate || "none",
    success:      successCount,
    failed:       failCount,
    failed_names: failedNames
  };
  data.logs = [logEntry, ...(data.logs || [])].slice(0, 60);
  data.last_updated = isoNow();

  return { data, successCount, failCount, failedNames };
}

// ─────────────────────────────────────────────
// COMPUTE MONTHLY: sum all daily_counts for the month + today's new count
// ─────────────────────────────────────────────
function computeMonthly(data, bid, today, todayDailyCount) {
  const monthPrefix = today.slice(0, 7);
  let sum = todayDailyCount;
  for (const [dateStr, daySnap] of Object.entries(data.daily || {})) {
    if (dateStr.startsWith(monthPrefix) && dateStr !== today) {
      sum += (daySnap[bid]?.daily_count || 0);
    }
  }
  return sum;
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function main() {
  const today  = getISTDateString();
  const nowStr = new Date().toISOString().replace("T", " ").slice(0, 19);

  console.log("=".repeat(62));
  console.log("  SATHYA MOBILES — Node.js Review Scraper");
  console.log(`  Running: ${nowStr} UTC`);
  console.log(`  Date:    ${today} (IST)`);
  console.log(`  Branches: ${BRANCHES.length}  |  Workers: ${WORKERS}`);
  console.log("=".repeat(62));

  // ── Fetch existing data from HF ──
  console.log("\n📥 Fetching existing data from Hugging Face...");
  const existing = await hfGet();
  if (existing) {
    const days = Object.keys(existing.daily || {}).length;
    console.log(`  ✅ Found existing data: ${days} days of history`);
  } else {
    console.log("  ℹ️  Starting fresh (no existing data)");
  }

  // ── Scrape all branches in parallel ──
  console.log(`\n🌐 Launching ${WORKERS} parallel browser contexts...\n`);

  // Split branches into chunks for workers
  const chunks = Array.from({ length: WORKERS }, () => []);
  BRANCHES.forEach((b, i) => chunks[i % WORKERS].push(b));

  const browser = await chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--disable-web-security",
      "--disable-features=IsolateOrigins,site-per-process"
    ]
  });

  const scrapeResults = {};
  const startTime = Date.now();

  // Run all workers in parallel
  await Promise.all(
    chunks.filter(c => c.length > 0).map(chunk => runWorker(browser, chunk, scrapeResults))
  );

  await browser.close();

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const successN = Object.values(scrapeResults).filter(r => r.reviewCount !== null).length;
  const failN    = BRANCHES.length - successN;

  console.log("\n" + "─".repeat(62));
  console.log(`  ✅ Scraped: ${successN}/${BRANCHES.length} branches in ${elapsed}s`);
  if (failN > 0) {
    const failNames = BRANCHES
      .filter(([id]) => !scrapeResults[id] || scrapeResults[id].reviewCount === null)
      .map(([,name]) => name);
    console.log(`  ❌ Failed:  ${failNames.join(", ")}`);
  }

  // ── Build JSON ──
  console.log("\n📊 Building data structure...");
  const { data, successCount, failCount, failedNames } = buildJson(existing, scrapeResults, today);

  const totalReviews = Object.values(data.branches).reduce((a, b) => a + (b.overall || 0), 0);
  const dailyTotal   = Object.values(data.daily[today] || {}).reduce((a, b) => a + (b.daily_count || 0), 0);
  console.log(`  Total reviews (all branches): ${totalReviews.toLocaleString("en-IN")}`);
  console.log(`  New reviews today:            ${dailyTotal}`);

  // ── Push to Hugging Face ──
  if (!HF_TOKEN) {
    console.log("\n⚠️  HF_TOKEN not set — skipping upload. Data preview:");
    console.log(JSON.stringify(data, null, 2).slice(0, 500) + "...");
  } else {
    console.log("\n📤 Uploading to Hugging Face...");
    try {
      await hfPut(JSON.stringify(data, null, 2));
      console.log(`  ✅ Uploaded to hf://datasets/${HF_REPO}/${HF_FILE}`);
    } catch (e) {
      console.error(`  ❌ Upload failed: ${e.message}`);
      process.exit(1);
    }
  }

  console.log("\n" + "=".repeat(62));
  console.log(`  🎉 ALL DONE!`);
  console.log(`  Success: ${successCount}  |  Failed: ${failCount}`);
  console.log(`  Total reviews: ${totalReviews.toLocaleString("en-IN")}  |  Today: +${dailyTotal}`);
  console.log("=".repeat(62) + "\n");

  if (failCount >= BRANCHES.length * 0.5) {
    console.error("  ❌ More than 50% branches failed. Exiting with error.");
    process.exit(1);
  }
}

main().catch(e => {
  console.error("FATAL:", e);
  process.exit(1);
});
