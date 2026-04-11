/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║   SATHYA MOBILES — Nightly Review Scraper (Node.js)         ║
 * ║   Parallel workers · Proper page-load waiting · No delays   ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const { chromium } = require("playwright");
const https = require("https");

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const HF_TOKEN           = process.env.HF_TOKEN || "";
const HF_REPO            = process.env.HF_REPO  || "RocklinKS/sathya-reviews";
const HF_FILE            = "sm.json";
const WORKERS            = 4;      // parallel browser contexts
const NAV_TIMEOUT        = 30000;  // page navigation timeout ms
const WAIT_TIMEOUT       = 12000;  // waitForSelector timeout ms
const PER_BRANCH_RETRIES = 2;      // immediate retries per branch if scrape returns null

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
  const istDate = new Date(now.getTime() + 5.5 * 60 * 60 * 1000);
  return istDate.toISOString().slice(0, 10);
}

function isoNow() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

// ─────────────────────────────────────────────
// SCRAPE SINGLE PLACE
// Waits for actual DOM elements to appear before parsing.
// No blind sleeps — exits as soon as data is found.
// ─────────────────────────────────────────────
async function scrapePlace(page, placeId) {
  const url = `https://www.google.com/maps/place/?q=place_id:${placeId}`;
  let reviewCount = null;
  let starRating  = null;

  try {
    // Wait for full network idle so all JS bundles have loaded
    await page.goto(url, { waitUntil: "networkidle", timeout: NAV_TIMEOUT });

    // Wait until the review count element actually exists in the DOM.
    // This is the key fix — we don't sleep blindly, we wait for the element.
    const reviewAppeared = await page
      .waitForSelector('[aria-label*="reviews"], [aria-label*="Reviews"]', { timeout: WAIT_TIMEOUT })
      .then(() => true)
      .catch(() => false);

    // If review element didn't appear, wait for star rating element as fallback
    if (!reviewAppeared) {
      await page
        .waitForSelector('[aria-label*="stars"], [aria-label*="star"]', { timeout: WAIT_TIMEOUT })
        .catch(() => {});
    }

    // ── Extract review count from aria-label ──
    const countSelectors = [
      '[aria-label*="reviews"]',
      '[aria-label*="Reviews"]',
      'button[jsaction*="review"]',
    ];
    for (const sel of countSelectors) {
      if (reviewCount) break;
      const els = await page.locator(sel).all();
      for (const el of els) {
        const label = (await el.getAttribute("aria-label")) || "";
        const m = label.match(/([\d,]+)/);
        if (m) {
          const val = parseInt(m[1].replace(/,/g, ""), 10);
          if (val > 0) { reviewCount = val; break; }
        }
      }
    }

    // ── Extract star rating from aria-label ──
    const starSelectors = [
      '[aria-label*="stars"]',
      '[aria-label*="star"]',
      'span[aria-label*="stars"]',
    ];
    for (const sel of starSelectors) {
      if (starRating) break;
      const els = await page.locator(sel).all();
      for (const el of els) {
        const label = (await el.getAttribute("aria-label")) || "";
        const m = label.match(/(\d\.\d)/);
        if (m) { starRating = parseFloat(m[1]); break; }
      }
    }

    // ── Fallback: parse raw page HTML ──
    const content = await page.content();

    if (!reviewCount) {
      const patterns = [
        /([\d,]+)\s*reviews?/i,
        /"reviewCount"["\s:]+(\d+)/i,
        /(\d[\d,]{2,})\s*Google review/i,
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
        /"ratingValue":"([\d.]+)"/i,
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
    console.error(`      ⚠️  Error: ${e.message.slice(0, 100)}`);
  }

  return { reviewCount, starRating };
}

// ─────────────────────────────────────────────
// WORKER — one page per worker, retries immediately on failure
// ─────────────────────────────────────────────
async function runWorker(browser, chunk, results) {
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    locale: "en-IN",
    viewport: { width: 1280, height: 800 },
  });
  const page = await context.newPage();

  for (const [id, name, agm, placeId] of chunk) {
    process.stdout.write(`  [${String(id).padStart(2, "0")}] ${name.padEnd(24)} → `);

    let reviewCount = null;
    let starRating  = null;

    for (let attempt = 1; attempt <= PER_BRANCH_RETRIES; attempt++) {
      const r = await scrapePlace(page, placeId);
      reviewCount = r.reviewCount;
      starRating  = r.starRating;

      if (reviewCount !== null) break;

      // Retry immediately — no sleep
      if (attempt < PER_BRANCH_RETRIES) {
        process.stdout.write(`[retry] `);
      }
    }

    if (reviewCount !== null) {
      console.log(`${reviewCount.toLocaleString("en-IN")} reviews  ${starRating ? starRating + "⭐" : "—"}  ✓`);
    } else {
      console.log(`FAILED ✗`);
    }
    results[id] = { reviewCount, starRating };
  }

  await context.close();
}

// ─────────────────────────────────────────────
// HF: fetch existing sm.json
// ─────────────────────────────────────────────
function hfGet() {
  return new Promise((resolve) => {
    const url = `https://huggingface.co/datasets/${HF_REPO}/resolve/main/${HF_FILE}?download=true`;
    https
      .get(url, { headers: { Authorization: `Bearer ${HF_TOKEN}`, "Cache-Control": "no-cache" } }, (res) => {
        if (res.statusCode === 404 || res.statusCode === 401) {
          console.log(`  ℹ️  HF: file not found (${res.statusCode}) — starting fresh.`);
          res.resume();
          resolve(null);
          return;
        }
        let raw = "";
        res.on("data", (c) => (raw += c));
        res.on("end", () => {
          try { resolve(JSON.parse(raw)); }
          catch { resolve(null); }
        });
      })
      .on("error", (e) => {
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
      files: [{ path: HF_FILE, content }],
    });
    const req = https.request(
      {
        hostname: "huggingface.co",
        path: `/api/datasets/${HF_REPO}/commit/main`,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${HF_TOKEN}`,
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (res) => {
        let raw = "";
        res.on("data", (c) => (raw += c));
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) resolve(true);
          else {
            console.error("  HF PUT error:", res.statusCode, raw.slice(0, 300));
            reject(new Error(`HF PUT failed: ${res.statusCode}`));
          }
        });
      }
    );
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

// ─────────────────────────────────────────────
// BUILD JSON
// ─────────────────────────────────────────────
function buildJson(existing, scrapeResults, today) {
  const data = existing || { last_updated: isoNow(), branches: {}, daily: {}, logs: [] };

  for (const [id, name, agm] of BRANCHES) {
    if (!data.branches[String(id)])
      data.branches[String(id)] = { id, name, agm, overall: 0, star_rating: 0 };
  }

  const knownDates = Object.keys(data.daily || {}).sort();
  const prevDate   = knownDates.filter((d) => d !== today).pop() || null;

  if (!data.daily[today]) data.daily[today] = {};

  let successCount = 0, failCount = 0;
  const failedNames = [];

  for (const [id, name] of BRANCHES) {
    const bid = String(id);
    const res = scrapeResults[id];

    if (!res || res.reviewCount === null) {
      failCount++;
      failedNames.push(name);
      const prevSnap = prevDate ? data.daily[prevDate]?.[bid] || {} : {};
      data.daily[today][bid] = {
        total_snap:  prevSnap.total_snap  || 0,
        daily_count: 0,
        monthly:     computeMonthly(data, bid, today, 0),
        star_rating: res?.starRating || prevSnap.star_rating || 0,
      };
      continue;
    }

    successCount++;
    const liveTotal  = res.reviewCount;
    const prevTotal  = prevDate
      ? data.daily[prevDate]?.[bid]?.total_snap || 0
      : data.branches[bid]?.overall || 0;

    const dailyCount = Math.max(0, liveTotal - prevTotal);
    const monthly    = computeMonthly(data, bid, today, dailyCount);

    data.daily[today][bid] = {
      total_snap:  liveTotal,
      daily_count: dailyCount,
      monthly,
      star_rating: res.starRating || 0,
    };

    data.branches[bid].overall     = liveTotal;
    data.branches[bid].star_rating = res.starRating || data.branches[bid].star_rating || 0;
    data.branches[bid].monthly     = monthly;
  }

  data.logs = [
    { ran_at: isoNow(), snap_date: today, baseline_date: prevDate || "none", success: successCount, failed: failCount, failed_names: failedNames },
    ...(data.logs || []),
  ].slice(0, 60);
  data.last_updated = isoNow();

  return { data, successCount, failCount, failedNames };
}

// ─────────────────────────────────────────────
// COMPUTE MONTHLY
// ─────────────────────────────────────────────
function computeMonthly(data, bid, today, todayDailyCount) {
  const mp = today.slice(0, 7);
  let sum = todayDailyCount;
  for (const [d, snap] of Object.entries(data.daily || {})) {
    if (d.startsWith(mp) && d !== today) sum += snap[bid]?.daily_count || 0;
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
  console.log(`  Running : ${nowStr} UTC`);
  console.log(`  Date    : ${today} (IST)`);
  console.log(`  Branches: ${BRANCHES.length}  |  Workers: ${WORKERS}  |  Retries: ${PER_BRANCH_RETRIES}`);
  console.log("=".repeat(62));

  console.log("\n📥 Fetching existing data from Hugging Face...");
  const existing = await hfGet();
  console.log(existing
    ? `  ✅ Found: ${Object.keys(existing.daily || {}).length} days of history`
    : "  ℹ️  Starting fresh");

  const chunks = Array.from({ length: WORKERS }, () => []);
  BRANCHES.forEach((b, i) => chunks[i % WORKERS].push(b));

  console.log(`\n🌐 Launching ${WORKERS} parallel Chromium contexts...\n`);

  const browser = await chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--disable-web-security",
      "--disable-features=IsolateOrigins,site-per-process",
    ],
  });

  const scrapeResults = {};
  const startTime = Date.now();

  await Promise.all(
    chunks.filter((c) => c.length > 0).map((c) => runWorker(browser, c, scrapeResults))
  );

  await browser.close();

  const elapsed  = ((Date.now() - startTime) / 1000).toFixed(1);
  const successN = Object.values(scrapeResults).filter((r) => r.reviewCount !== null).length;
  const failN    = BRANCHES.length - successN;

  console.log("\n" + "─".repeat(62));
  console.log(`  ✅ Done: ${successN}/${BRANCHES.length} in ${elapsed}s`);
  if (failN > 0) {
    const names = BRANCHES
      .filter(([id]) => !scrapeResults[id] || scrapeResults[id].reviewCount === null)
      .map(([, n]) => n);
    console.log(`  ❌ Failed: ${names.join(", ")}`);
  }

  console.log("\n📊 Building JSON...");
  const { data, successCount, failCount, failedNames } = buildJson(existing, scrapeResults, today);

  const totalReviews = Object.values(data.branches).reduce((a, b) => a + (b.overall || 0), 0);
  const dailyTotal   = Object.values(data.daily[today] || {}).reduce((a, b) => a + (b.daily_count || 0), 0);
  console.log(`  Total: ${totalReviews.toLocaleString("en-IN")}  |  Today: +${dailyTotal}`);

  if (!HF_TOKEN) {
    console.log("\n⚠️  No HF_TOKEN — skipping upload.");
  } else {
    console.log("\n📤 Uploading to Hugging Face...");
    try {
      await hfPut(JSON.stringify(data, null, 2));
      console.log(`  ✅ Uploaded → hf://datasets/${HF_REPO}/${HF_FILE}`);
    } catch (e) {
      console.error(`  ❌ Upload failed: ${e.message}`);
      process.exit(1);
    }
  }

  console.log("\n" + "=".repeat(62));
  console.log(`  🎉 DONE  |  ✅ ${successCount}  ❌ ${failCount}  ⏱ ${elapsed}s`);
  console.log("=".repeat(62) + "\n");

  if (failCount >= Math.ceil(BRANCHES.length * 0.5)) {
    console.error("Over 50% failed — marking run as failed.");
    process.exit(1);
  }
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
