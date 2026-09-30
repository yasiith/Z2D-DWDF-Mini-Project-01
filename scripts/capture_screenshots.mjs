// ============================================================================
// capture_screenshots.mjs — captures the submission screenshots with headless
// Chrome through the DevTools protocol (Node 22+ built-in WebSocket; no npm
// packages needed).
//
//   NiFi canvas: the pipeline overview and every process group (panels hidden,
//                "fit to screen"), plus the NiFi Summary page after a run
//   Database:    HTML pages produced by scripts/capture_evidence.sh (psql -H)
//
// Usage: node scripts/capture_screenshots.mjs [--nifi http://127.0.0.1:8080]
// ============================================================================
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync, existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(ROOT, "docs", "screenshots");
const NIFI = process.argv.includes("--nifi") ? process.argv[process.argv.indexOf("--nifi") + 1] : "http://127.0.0.1:8080";
const CHROME = process.env.CHROME || [
  "C:/Program Files/Google/Chrome/Application/chrome.exe",
  "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
  "/usr/bin/google-chrome", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
].find(existsSync);
const PORT = 9333;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- chrome / CDP
const chrome = spawn(CHROME, [
  "--headless=new", "--disable-gpu", "--hide-scrollbars", `--remote-debugging-port=${PORT}`,
  `--user-data-dir=${mkdtempSync(join(tmpdir(), "abc-shots-"))}`, "--window-size=1920,1080", "about:blank",
], { stdio: "ignore" });

let ws, nextId = 0;
const pending = new Map();
function send(method, params = {}) {
  const id = ++nextId;
  ws.send(JSON.stringify({ id, method, params }));
  return new Promise((res, rej) => pending.set(id, { res, rej }));
}
async function connect() {
  for (let i = 0; i < 50; i++) {
    try {
      const targets = await (await fetch(`http://127.0.0.1:${PORT}/json/list`)).json();
      const page = targets.find((t) => t.type === "page");
      if (page) {
        ws = new WebSocket(page.webSocketDebuggerUrl);
        await new Promise((r) => ws.addEventListener("open", r, { once: true }));
        ws.addEventListener("message", (e) => {
          const msg = JSON.parse(e.data);
          if (msg.id && pending.has(msg.id)) {
            const { res, rej } = pending.get(msg.id);
            pending.delete(msg.id);
            msg.error ? rej(new Error(msg.error.message)) : res(msg.result);
          }
        });
        return;
      }
    } catch { /* chrome still starting */ }
    await sleep(200);
  }
  throw new Error("could not connect to headless Chrome");
}
const evaluate = (expression) => send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });

async function shot(url, file, { prepare = "", height = 1080, fullPage = false, wait = 4000 } = {}) {
  await send("Emulation.setDeviceMetricsOverride", { width: 1920, height, deviceScaleFactor: 1, mobile: false });
  await send("Page.navigate", { url });
  await sleep(wait);
  if (prepare) await evaluate(`(async () => { ${prepare} })()`);
  await sleep(1200);
  let clip;
  if (fullPage) {
    const { result } = await evaluate("JSON.stringify([document.documentElement.scrollWidth, document.documentElement.scrollHeight])");
    const [w, h] = JSON.parse(result.value);
    await send("Emulation.setDeviceMetricsOverride", { width: Math.max(1200, Math.min(w, 1920)), height: h, deviceScaleFactor: 1, mobile: false });
    await sleep(500);
  }
  const { data } = await send("Page.captureScreenshot", { format: "png", captureBeyondViewport: fullPage, clip });
  writeFileSync(join(OUT, file), Buffer.from(data, "base64"));
  console.log(`  saved docs/screenshots/${file}`);
}

// ------------------------------------------------------------------ NiFi pages
const FIT = `
  await new Promise(r => setTimeout(r, 1500));
  const gc = document.querySelector('graph-controls'); if (gc) gc.style.display = 'none';
  const fit = document.querySelector('.icon-zoom-fit'); if (fit) fit.closest('button').click();
`;

async function main() {
  await connect();
  await send("Page.enable");
  const root = await (await fetch(`${NIFI}/nifi-api/flow/process-groups/root`)).json();
  const top = root.processGroupFlow.flow.processGroups.find((g) => g.component.name === "ABC Hub ETL Pipeline");
  if (!top) throw new Error("flow 'ABC Hub ETL Pipeline' not found - run nifi/build_flow.py first");
  const children = (await (await fetch(`${NIFI}/nifi-api/process-groups/${top.id}/process-groups`)).json()).processGroups
    .map((g) => ({ id: g.id, name: g.component.name })).sort((a, b) => a.name.localeCompare(b.name));

  console.log("==> NiFi canvas");
  await shot(`${NIFI}/nifi/#/process-groups/${top.id}`, "01_nifi_pipeline_overview.png", { prepare: FIT, wait: 6000 });
  for (const [i, g] of children.entries()) {
    const slug = g.name.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
    await shot(`${NIFI}/nifi/#/process-groups/${g.id}`, `${String(i + 2).padStart(2, "0")}_nifi_${slug}.png`, { prepare: FIT });
  }
  await shot(`${NIFI}/nifi/#/summary`, "08_nifi_summary_after_run.png", { wait: 6000 });

  console.log("==> database evidence pages");
  const htmlDir = join(OUT, "html");
  if (existsSync(htmlDir)) {
    for (const f of readdirSync(htmlDir).filter((f) => f.endsWith(".html")).sort()) {
      await shot(pathToFileURL(join(htmlDir, f)).href, f.replace(/\.html$/, ".png"), { fullPage: true, wait: 1500 });
    }
  }
}

main().then(() => { chrome.kill(); process.exit(0); })
      .catch((e) => { console.error(e); chrome.kill(); process.exit(1); });
