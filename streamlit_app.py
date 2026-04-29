from __future__ import annotations

import os
import streamlit as st

# --- 1. CLOUD BROWSER INITIALIZATION ---
@st.cache_resource(show_spinner=False)
def install_browser():
    os.system("playwright install chromium")
install_browser()

import asyncio
import html
import json
import re
import shutil
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse, urlunparse, unquote

import httpx
from bs4 import BeautifulSoup
import pandas as pd

# --- 2. GLOBAL CONSTANTS & REGEX ---
WHATSAPP_RE = re.compile(
    r"""(?ix)
    https?://(?:
        chat\.whatsapp\.com/(?:invite/)?[A-Za-z0-9_-]{8,}
    )
    """
)

GOOD_CLICK_WORDS = {
    "join", "join group", "join now", "join whatsapp", "join group now",
    "i agree", "agree", "continue", "proceed", "rules", "invite",
    "open group", "visit group", "whatsapp", "group", "click here"
}

BAD_CLICK_WORDS = {
    "report", "add group", "submit group", "privacy", "terms",
    "contact", "login", "register", "advertise", "facebook",
    "instagram", "telegram", "youtube", "remove", "delete"
}

# The absolute blacklist to prevent crawling into WA domains
BAD_HREF_PARTS = {
    "pagead2.googlesyndication.com", "doubleclick.net", "googleads",
    "/report", "/addgroup", "/login", "/register", "mailto:", "tel:", 
    "javascript:void", "whatsapp.com", "wa.me"
}

DEFAULT_SETTINGS = {
    "max_pages": 150, "max_depth": 3, "http_concurrency": 12, "browser_concurrency": 1,
    "http_timeout": 12, "browser_timeout_ms": 15000, "max_pages_per_domain": 80,
    "max_candidates_per_page": 18, "enable_browser_fallback": True,
    "same_domain_only": True, "respect_robots_hint": True, "live_refresh_delay": 0.4,
}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# --- 3. STORAGE SYSTEM ---
class LocalStore:
    def __init__(self, data_dir: Path | str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.results_file = self.data_dir / "results.json"
        self.settings_file = self.data_dir / "settings.json"
        self.logs_file = self.data_dir / "logs.jsonl"

    def _read_json(self, path: Path, default: Any) -> Any:
        if not path.exists(): return default
        try: return json.loads(path.read_text(encoding="utf-8"))
        except: return default

    def _write_json(self, path: Path, data: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def load_settings(self) -> dict:
        merged = {**DEFAULT_SETTINGS, **self._read_json(self.settings_file, {})}
        self.save_settings(merged)
        return merged

    def save_settings(self, settings: dict) -> None:
        self._write_json(self.settings_file, settings)

    def load_results(self) -> list[dict]:
        rows = self._read_json(self.results_file, [])
        return rows if isinstance(rows, list) else []

    def save_results(self, rows: list[dict]) -> None:
        deduped = {}
        for row in rows:
            key = row.get("normalized_url") or row.get("invite_url")
            if not key: continue
            if key not in deduped: deduped[key] = row
            else: deduped[key].update({k: v for k, v in row.items() if v not in [None, ""]})
        self._write_json(self.results_file, list(deduped.values()))

    def bulk_update_results(self, normalized_urls: list[str], updates: dict) -> None:
        target = set(normalized_urls)
        rows = self.load_results()
        for row in rows:
            if row.get("normalized_url") in target:
                row.update(updates)
                row["updated_at"] = now_iso()
        self.save_results(rows)

    def append_log(self, message: str, data: dict | None = None) -> None:
        item = {"time": now_iso(), "message": message, "data": data or {}}
        with self.logs_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    def load_logs(self, limit: int = 500) -> list[str]:
        if not self.logs_file.exists(): return []
        return self.logs_file.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]

    def export_backup_bytes(self) -> bytes:
        backup = {"created_at": now_iso(), "settings": self.load_settings(), "results": self.load_results(), "logs": self.load_logs(limit=1000)}
        return json.dumps(backup, ensure_ascii=False, indent=2).encode("utf-8")

    def import_backup_bytes(self, raw: bytes) -> None:
        data = json.loads(raw.decode("utf-8", errors="ignore"))
        if "settings" in data and isinstance(data["settings"], dict):
            self.save_settings({**DEFAULT_SETTINGS, **data["settings"]})
        if "results" in data and isinstance(data["results"], list):
            self.save_results(data["results"])

def ensure_app_dirs(*dirs: Path) -> None:
    for d in dirs: d.mkdir(parents=True, exist_ok=True)


# --- 4. CRAWLER CORE ENGINE ---
@dataclass
class CrawlConfig:
    max_pages: int
    max_depth: int
    http_concurrency: int
    browser_concurrency: int
    http_timeout: float
    browser_timeout_ms: int
    max_pages_per_domain: int
    max_candidates_per_page: int
    enable_browser_fallback: bool
    same_domain_only: bool
    polite_mode: bool

@dataclass
class CrawlEvent:
    kind: str
    message: str
    data: dict

@dataclass
class Candidate:
    url: str
    text: str
    score: int
    depth: int = 0

def clean_text(value: str | None) -> str:
    if not value: return ""
    return re.sub(r"\s+", " ", html.unescape(value)).strip().lower()

def normalize_page_url(raw: str | None, base: str | None = None) -> str | None:
    if not raw: return None
    raw = html.unescape(unquote(str(raw).strip().strip('"').strip("'")))
    if base: raw = urljoin(base, raw)
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}: return None
    return urlunparse((parsed.scheme, parsed.netloc.lower(), parsed.path or "/", "", parsed.query, ""))

def normalize_whatsapp_url(raw: str) -> str | None:
    raw = html.unescape(unquote(str(raw).strip().strip('"').strip("'")))
    parsed = urlparse(raw)
    if parsed.netloc.lower() == "chat.whatsapp.com":
        parts = parsed.path.strip("/").split("/")
        if parts and len(parts[-1]) >= 8:
            return f"https://chat.whatsapp.com/{parts[-1]}"
    return None

def source_domain(url: str) -> str:
    return urlparse(url).netloc.lower()

def extract_whatsapp_links(text: str) -> list[str]:
    found = []
    for match in WHATSAPP_RE.findall(html.unescape(text or "")):
        norm = normalize_whatsapp_url(match)
        if norm: found.append(norm)
    return list(dict.fromkeys(found))

def is_bad_href(href: str) -> bool:
    return any(part in (href or "").lower() for part in BAD_HREF_PARTS)

def click_score(text: str, href: str) -> int:
    t, h = clean_text(text), clean_text(href)
    combined = f"{t} {h}"
    if any(bad in combined for bad in BAD_CLICK_WORDS): return -10
    score = sum(3 for good in GOOD_CLICK_WORDS if good in combined)
    if "/group/rules/" in h: score += 10
    if "/group/invite/" in h: score += 8
    if "button" in combined: score += 1
    return score

def allowed_follow(candidate_url: str, start_url: str, config: CrawlConfig) -> bool:
    c, s = urlparse(candidate_url), urlparse(start_url)
    if c.scheme not in {"http", "https"} or is_bad_href(candidate_url): return False
    if config.same_domain_only and c.netloc.lower() != s.netloc.lower(): return False
    return True

def make_result(invite_url: str, source_page: str, method: str) -> dict:
    return {
        "invite_url": invite_url, "normalized_url": normalize_whatsapp_url(invite_url) or invite_url,
        "source_page": source_page, "source_domain": source_domain(source_page),
        "discovered_at": now_iso(), "extraction_method": method
    }

def extract_candidates(html_text: str, page_url: str, root_url: str, config: CrawlConfig, depth: int) -> list[Candidate]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    candidates = []
    for a in soup.select("a[href]"):
        href = normalize_page_url(a.get("href"), page_url)
        if not href: continue
        text = a.get_text(" ", strip=True) or ""
        score = click_score(text, href)
        if score > 0 and allowed_follow(href, root_url, config):
            candidates.append(Candidate(url=href, text=text, score=score, depth=depth + 1))

    onclick_blob = " ".join(tag.get("onclick", "") for tag in soup.select("[onclick]"))
    for raw in re.findall(r"""['"]([^'"]*(?:/group/rules/|/group/invite/)[^'"]*)['"]""", onclick_blob):
        href = normalize_page_url(raw, page_url)
        if href and allowed_follow(href, root_url, config):
            candidates.append(Candidate(url=href, text="onclick", score=8, depth=depth + 1))

    best = {}
    for c in candidates:
        if c.url not in best or c.score > best[c.url].score: best[c.url] = c
    return sorted(best.values(), key=lambda x: x.score, reverse=True)[: config.max_candidates_per_page]

def find_chromium_executable() -> str | None:
    for name in ["chromium", "chromium-browser", "google-chrome"]:
        if path := shutil.which(name): return path
    return None

class BrowserPiercer:
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.hits = {}

    def capture(self, url: str, source_page: str, method: str) -> None:
        norm = normalize_whatsapp_url(url)
        if norm and norm not in self.hits:
            self.hits[norm] = make_result(url, source_page, method)

    async def pierce(self, start_url: str) -> list[dict]:
        try: from playwright.async_api import async_playwright
        except: return []

        exe_path = find_chromium_executable()
        try:
            async with async_playwright() as pw:
                args = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]}
                if exe_path: args["executable_path"] = exe_path
                browser = await pw.chromium.launch(**args)
                context = await browser.new_context(java_script_enabled=True, ignore_https_errors=True)

                async def route_handler(route):
                    if normalize_whatsapp_url(route.request.url):
                        self.capture(route.request.url, start_url, "browser_network_intercept")
                        await route.abort()
                        return
                    await route.continue_()

                await context.route("**/*", route_handler)
                page = await context.new_page()
                page.on("request", lambda req: self.capture(req.url, page.url or start_url, "browser_request"))
                
                queue, visited = deque([Candidate(start_url, "", 100, 0)]), set()

                for _ in range(8):
                    if self.hits or not queue: break
                    cand = queue.popleft()
                    curr = normalize_page_url(cand.url)
                    if not curr or curr in visited: continue
                    visited.add(curr)

                    try: await page.goto(curr, wait_until="domcontentloaded", timeout=self.config.browser_timeout_ms)
                    except: continue

                    await page.wait_for_timeout(700)
                    for p in context.pages:
                        try:
                            html_text = await p.content()
                            for link in extract_whatsapp_links(html_text + " " + p.url):
                                self.capture(link, p.url, "browser_dom")
                        except: pass

                    if self.hits: break

                    try:
                        for c in extract_candidates(await page.content(), page.url, start_url, self.config, cand.depth):
                            if c.url not in visited: queue.append(c)
                    except: pass

                    try:
                        loc = page.locator("a, button, [role='button']")
                        scored = []
                        for i in range(min(await loc.count(), 20)):
                            try:
                                el = loc.nth(i)
                                if not await el.is_visible(timeout=200): continue
                                text = await el.inner_text(timeout=200)
                                href = await el.get_attribute("href") or ""
                                score = click_score(text, href)
                                if score > 0: scored.append((score, el))
                            except: continue
                            
                        scored.sort(key=lambda x: x[0], reverse=True)
                        for _, el in scored[:5]:
                            if self.hits: break
                            try:
                                await el.scroll_into_view_if_needed(timeout=500)
                                await el.click(timeout=1500)
                                await page.wait_for_timeout(500)
                            except: continue
                    except: pass

                await context.close()
                await browser.close()
        except: pass
        return list(self.hits.values())

async def run_crawl_job(seeds: list[str], config: CrawlConfig, on_event: Callable[[CrawlEvent], None]) -> dict:
    def emit(k: str, m: str, d: dict = None): on_event(CrawlEvent(k, m, d or {}))
    queue, visited, found, domain_counts, browser_q = asyncio.Queue(), set(), {}, defaultdict(int), asyncio.Queue()

    for raw in seeds:
        if url := normalize_page_url(raw): await queue.put(Candidate(url, "seed", 100, 0))

    counters = {"queued": queue.qsize(), "visited": 0, "found": 0, "failed": 0, "browser_pages": 0}

    async with httpx.AsyncClient(timeout=config.http_timeout, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
        sem = asyncio.Semaphore(config.http_concurrency)

        async def process_candidate(_):
            while not queue.empty() and len(visited) < config.max_pages:
                cand = await queue.get()
                url = normalize_page_url(cand.url)
                if not url or url in visited or cand.depth > config.max_depth or domain_counts[source_domain(url)] >= config.max_pages_per_domain:
                    queue.task_done(); continue

                visited.add(url)
                domain_counts[source_domain(url)] += 1
                counters["visited"], counters["queued"] = len(visited), queue.qsize()
                emit("status", f"Fetching: {url}", {"counters": counters})

                try:
                    async with sem:
                        resp = await client.get(url)
                        final_url, body = str(resp.url), resp.text or ""

                    links = extract_whatsapp_links(body + " " + final_url)
                    if links:
                        for link in links:
                            row = make_result(link, final_url, "http_html")
                            norm = row["normalized_url"]
                            if norm not in found:
                                found[norm] = row
                                counters["found"] = len(found)
                                emit("result", f"Found: {norm}", {"row": row})
                        queue.task_done(); continue

                    candidates = extract_candidates(body, final_url, url, config, cand.depth)
                    for c in candidates:
                        if len(visited) + queue.qsize() >= config.max_pages: break
                        if normalize_page_url(c.url, final_url) not in visited: await queue.put(c)

                    if config.enable_browser_fallback and any(c.score >= 5 for c in candidates) and config.browser_concurrency > 0:
                        await browser_q.put(final_url)
                except: counters["failed"] += 1
                finally: queue.task_done()

        await asyncio.gather(*(asyncio.create_task(process_candidate(i)) for i in range(max(1, config.http_concurrency))))

    if config.enable_browser_fallback and not browser_q.empty():
        b_sem = asyncio.Semaphore(config.browser_concurrency)
        async def browser_worker(_):
            while not browser_q.empty():
                p_url = await browser_q.get()
                if counters["browser_pages"] >= config.browser_concurrency * 20: browser_q.task_done(); continue
                async with b_sem:
                    counters["browser_pages"] += 1
                    emit("status", f"Playwright fallback: {p_url}", {"counters": counters})
                    try:
                        for row in await BrowserPiercer(config).pierce(p_url):
                            norm = row["normalized_url"]
                            if norm not in found:
                                found[norm] = row
                                counters["found"] = len(found)
                                emit("result", f"Found (JS): {norm}", {"row": row})
                    except: counters["failed"] += 1
                    finally: browser_q.task_done()
        await asyncio.gather(*(asyncio.create_task(browser_worker(i)) for i in range(config.browser_concurrency)))

    summary = {"visited": len(visited), "found_unique": len(found), "failed": counters["failed"]}
    emit("status", "Job complete", {"counters": counters, "summary": summary})
    return summary


# --- 5. STREAMLIT FRONTEND & LIQUID RED THEME ---
st.set_page_config(page_title="SCOLO Crimson Extractor", layout="wide", page_icon="🩸")

st.markdown("""
    <style>
    .stApp { background-color: #0a0303; color: #f5f5f5; }
    h1, h2, h3 { color: #ff3333 !important; text-shadow: 0 0 10px rgba(255, 51, 51, 0.4); font-family: 'Helvetica Neue', sans-serif; font-weight: 800; }
    .stButton > button { background: linear-gradient(135deg, #8b0000 0%, #ff1744 100%); color: white !important; border: none; border-radius: 8px; padding: 10px 24px; font-weight: 700; box-shadow: 0 4px 15px rgba(255, 23, 68, 0.3); transition: all 0.3s; text-transform: uppercase; }
    .stButton > button:hover { background: linear-gradient(135deg, #ff1744 0%, #ff5252 100%); transform: translateY(-2px); box-shadow: 0 6px 20px rgba(255, 23, 68, 0.6); }
    .stTextInput > div > div > input, .stTextArea > div > textarea { background-color: #1a0808 !important; color: #ff8a80 !important; border: 1px solid #d32f2f !important; border-radius: 6px; }
    .stTextInput > div > div > input:focus, .stTextArea > div > textarea:focus { border-color: #ff5252 !important; box-shadow: 0 0 8px rgba(255, 82, 82, 0.4) !important; }
    [data-testid="stSidebar"] { background-color: #120404 !important; border-right: 1px solid #3e0b0b; }
    [data-testid="stDataFrame"] { border: 1px solid #5c1010; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.5); }
    .stAlert { background-color: #1f0b0b !important; border: 1px solid #b71c1c !important; color: #ffcdd2 !important; }
    </style>
""", unsafe_allow_html=True)

DATA_DIR = Path("data")
def init_state() -> None:
    ensure_app_dirs(DATA_DIR, Path("exports"))
    if "store" not in st.session_state: st.session_state.store = LocalStore(DATA_DIR)
    if "settings" not in st.session_state: st.session_state.settings = st.session_state.store.load_settings()
    if "results" not in st.session_state: st.session_state.results = st.session_state.store.load_results()

def normalize_lines(raw: str) -> list[str]:
    return list(dict.fromkeys([line.strip() for line in (raw or "").splitlines() if line.strip()]))

def settings_panel() -> CrawlConfig:
    s = st.session_state.settings
    with st.sidebar:
        st.header("⚙️ Overlord Settings")
        s["max_pages"] = st.number_input("Max Pages", min_value=5, max_value=500, value=int(s.get("max_pages", 100)))
        s["max_depth"] = st.number_input("Max Depth", min_value=0, max_value=8, value=int(s.get("max_depth", 2)))
        s["http_concurrency"] = st.number_input("HTTP Concurrency", min_value=1, max_value=20, value=int(s.get("http_concurrency", 8)))
        s["enable_browser_fallback"] = st.toggle("Enable JS Engine", value=bool(s.get("enable_browser_fallback", True)))
        if st.button("Save Configuration", use_container_width=True):
            st.session_state.store.save_settings(s)
            st.success("Config Locked.")
    return CrawlConfig(
        max_pages=int(s["max_pages"]), max_depth=int(s["max_depth"]),
        http_concurrency=int(s["http_concurrency"]), browser_concurrency=1,
        http_timeout=15.0, browser_timeout_ms=20000, max_pages_per_domain=50,
        max_candidates_per_page=10, enable_browser_fallback=bool(s["enable_browser_fallback"]),
        same_domain_only=True, polite_mode=True
    )

async def run_job_async(seeds: list[str], config: CrawlConfig, ui_slots: dict) -> dict:
    store = st.session_state.store
    all_results = st.session_state.results
    seen = {r.get("normalized_url") for r in all_results if r.get("normalized_url")}
    live_rows = []
    
    def on_event(event: CrawlEvent) -> None:
        if event.kind == "status":
            ui_slots["status"].info(f"🕷️ {event.message}")
        elif event.kind == "result":
            row = event.data["row"]
            row["review_status"] = "unreviewed"
            row["kept"] = True
            norm = row.get("normalized_url")
            if norm and norm in seen: return
            if norm: seen.add(norm)
            all_results.append(row)
            live_rows.append(row)
            if len(live_rows) % 3 == 0: store.save_results(all_results)

    summary = await run_crawl_job(seeds=seeds, config=config, on_event=on_event)
    store.save_results(all_results)
    st.session_state.results = all_results
    return summary

def main() -> None:
    init_state()
    st.title("🩸 Crimson Spider Core")
    st.caption("Monolithic Architecture: Pure extraction. No channel noise. No proxy crashes.")

    config = settings_panel()
    tabs = st.tabs(["🔴 INITIATE BREACH", "🗄️ DATABASE MANAGER", "💾 EXPORT / BACKUP"])

    with tabs[0]:
        st.subheader("Target Acquisition")
        raw_seeds = st.text_area("Drop Target URLs Here:", height=150, placeholder="https://groupsor.link/")
        
        c1, c2 = st.columns([1, 1])
        with c1: start = st.button("LAUNCH SWARM", type="primary", use_container_width=True)
            
        status_slot = st.empty()
        
        if start:
            seeds = normalize_lines(raw_seeds)
            if not seeds:
                st.error("Input required.")
            else:
                with st.spinner("Bleeding the targets..."):
                    try:
                        summary = asyncio.run(run_job_async(seeds, config, {"status": status_slot}))
                        st.success("Extraction Complete.")
                        st.json(summary)
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        summary = loop.run_until_complete(run_job_async(seeds, config, {"status": status_slot}))
                        st.success("Extraction Complete.")
                        st.json(summary)

    with tabs[1]:
        st.subheader("Master Extraction Ledger")
        results = st.session_state.results
        if not results:
            st.info("System memory empty.")
        else:
            df = pd.DataFrame(results)
            for col in ["review_status", "normalized_url", "invite_url", "source_page", "extraction_method", "kept"]:
                if col not in df.columns: df[col] = ""
            df.insert(0, "select", False)
            df = df[["select", "review_status", "normalized_url", "invite_url", "source_page", "extraction_method", "kept"]]
            
            edited = st.data_editor(df, use_container_width=True, hide_index=True)
            selected = edited[edited["select"] == True] if "select" in edited.columns else pd.DataFrame()
            
            c1, c2 = st.columns(2)
            with c1:
                if st.button("🗑️ Purge Selected Data", use_container_width=True):
                    urls_to_drop = selected["normalized_url"].dropna().tolist()
                    st.session_state.store.bulk_update_results(urls_to_drop, {"review_status": "remove", "kept": False})
                    st.session_state.results = st.session_state.store.load_results()
                    st.rerun()
            with c2:
                if st.button("⚠️ Wipe Entire Database", type="primary", use_container_width=True):
                    st.session_state.store.save_results([])
                    st.session_state.results = []
                    st.rerun()

    with tabs[2]:
        st.subheader("State Preservation")
        if not st.session_state.results:
            st.info("Nothing to export.")
        else:
            df_out = pd.DataFrame(st.session_state.results)
            csv_data = df_out.to_csv(index=False).encode("utf-8")
            st.download_button("Download Raw CSV", csv_data, file_name=f"wa_groups_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv", use_container_width=True)
            
            backup = st.session_state.store.export_backup_bytes()
            st.download_button("Download System State (JSON Backup)", backup, file_name="crimson_core_backup.json", mime="application/json", use_container_width=True)
            
        st.divider()
        uploaded = st.file_uploader("Restore System State (JSON)", type=["json"])
        if uploaded and st.button("Initiate Restore"):
            st.session_state.store.import_backup_bytes(uploaded.read())
            st.session_state.results = st.session_state.store.load_results()
            st.success("Memory restored.")
            st.rerun()

if __name__ == "__main__":
    main()
