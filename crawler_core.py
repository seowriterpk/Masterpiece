from __future__ import annotations

import asyncio
import html
import re
import shutil
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Awaitable
from urllib.parse import urljoin, urlparse, urlunparse, unquote

import httpx
from bs4 import BeautifulSoup

# STRICT WA REGEX - Chat groups ONLY.
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

# The absolute blacklist. Crawler will NEVER visit these URLs.
BAD_HREF_PARTS = {
    "pagead2.googlesyndication.com", "doubleclick.net", "googleads",
    "/report", "/addgroup", "/login", "/register", "mailto:", "tel:", "javascript:void",
    "whatsapp.com", "wa.me"
}

@dataclass
class CrawlConfig:
    max_pages: int = 150
    max_depth: int = 3
    http_concurrency: int = 12
    browser_concurrency: int = 1
    http_timeout: float = 12.0
    browser_timeout_ms: int = 15000
    max_pages_per_domain: int = 80
    max_candidates_per_page: int = 18
    enable_browser_fallback: bool = True
    same_domain_only: bool = True
    polite_mode: bool = True

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
    source_label: str = ""

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip().lower()

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
    host = parsed.netloc.lower()
    path = parsed.path.strip("/")
    if host == "chat.whatsapp.com":
        parts = path.split("/")
        code = parts[-1] if parts else ""
        if len(code) >= 8:
            return f"https://chat.whatsapp.com/{code}"
    return None

def source_domain(url: str) -> str:
    return urlparse(url).netloc.lower()

def extract_whatsapp_links(text: str) -> list[str]:
    found = []
    for match in WHATSAPP_RE.findall(html.unescape(text or "")):
        normalized = normalize_whatsapp_url(match)
        if normalized: found.append(normalized)
    return list(dict.fromkeys(found))

def is_bad_href(href: str) -> bool:
    return any(part in (href or "").lower() for part in BAD_HREF_PARTS)

def click_score(text: str, href: str) -> int:
    combined = f"{clean_text(text)} {clean_text(href)}"
    if any(bad in combined for bad in BAD_CLICK_WORDS): return -10
    score = sum(3 for good in GOOD_CLICK_WORDS if good in combined)
    if "/group/rules/" in clean_text(href): score += 10
    if "/group/invite/" in clean_text(href): score += 8
    if "button" in combined: score += 1
    return score

def allowed_follow(candidate_url: str, start_url: str, config: CrawlConfig) -> bool:
    c, s = urlparse(candidate_url), urlparse(start_url)
    if c.scheme not in {"http", "https"} or is_bad_href(candidate_url): return False
    if config.same_domain_only and c.netloc.lower() != s.netloc.lower(): return False
    return True

def make_result(invite_url: str, source_page: str, method: str, click_text: str = "") -> dict:
    return {
        "invite_url": invite_url, "normalized_url": normalize_whatsapp_url(invite_url) or invite_url,
        "source_page": source_page, "source_domain": source_domain(source_page),
        "source_label": "", "discovered_at": now_iso(),
        "extraction_method": method, "click_text": click_text,
    }

def extract_candidates(html_text: str, page_url: str, root_url: str, config: CrawlConfig, depth: int) -> list[Candidate]:
    soup = BeautifulSoup(html_text or "", "lxml")
    candidates = []
    for a in soup.select("a[href]"):
        href = normalize_page_url(a.get("href"), page_url)
        if not href: continue
        text = a.get_text(" ", strip=True) or a.get("title") or ""
        score = click_score(text, href)
        if score > 0 and allowed_follow(href, root_url, config):
            candidates.append(Candidate(url=href, text=text, score=score, depth=depth + 1))

    onclick_urls = re.findall(r"""['"]([^'"]*(?:/group/rules/|/group/invite/)[^'"]*)['"]""", " ".join(t.get("onclick", "") for t in soup.select("[onclick]")))
    for raw in onclick_urls:
        href = normalize_page_url(raw, page_url)
        if href and allowed_follow(href, root_url, config):
            candidates.append(Candidate(url=href, text="onclick", score=8, depth=depth + 1))

    best = {}
    for c in candidates:
        if c.url not in best or c.score > best[c.url].score: best[c.url] = c
    return sorted(best.values(), key=lambda x: x.score, reverse=True)[: config.max_candidates_per_page]

async def fetch_page(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    resp = await client.get(url)
    return str(resp.url), resp.text or ""

def find_chromium_executable() -> str | None:
    for name in ["chromium", "chromium-browser", "google-chrome"]:
        if path := shutil.which(name): return path
    return None

class BrowserPiercer:
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.hits = {}

    def capture(self, url: str, source_page: str, method: str, click_text: str = "") -> None:
        normalized = normalize_whatsapp_url(url)
        if normalized and normalized not in self.hits:
            self.hits[normalized] = make_result(url, source_page, method, click_text)

    async def pierce(self, start_url: str) -> list[dict]:
        try: from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
        except Exception: return []

        exe_path = find_chromium_executable()
        try:
            async with async_playwright() as pw:
                l_args = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]}
                if exe_path: l_args["executable_path"] = exe_path
                browser = await pw.chromium.launch(**l_args)
                context = await browser.new_context(java_script_enabled=True, ignore_https_errors=True, user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

                async def route_handler(route):
                    req_url = route.request.url
                    if normalize_whatsapp_url(req_url):
                        self.capture(req_url, start_url, "browser_network_intercept")
                        await route.abort()
                        return
                    await route.continue_()

                await context.route("**/*", route_handler)
                
                def watch(page):
                    page.on("request", lambda req: self.capture(req.url, page.url or start_url, "browser_request"))
                    page.on("response", lambda res: self.capture(res.url, page.url or start_url, "browser_response"))

                page = await context.new_page()
                watch(page)

                queue, visited, root_url = deque([Candidate(start_url, "start", 100, 0)]), set(), start_url

                for _ in range(10):
                    if self.hits or not queue: break
                    cand = queue.popleft()
                    current = normalize_page_url(cand.url)
                    if not current or current in visited: continue
                    visited.add(current)

                    try: await page.goto(current, wait_until="domcontentloaded", timeout=self.config.browser_timeout_ms)
                    except Exception: continue

                    await page.wait_for_timeout(700)
                    await self.scan_pages(context, cand.text)
                    if self.hits: break

                    try:
                        for c in extract_candidates(await page.content(), page.url, root_url, self.config, cand.depth):
                            if c.url not in visited: queue.append(c)
                    except: pass

                    await self.click_relevant_controls(context, root_url)
                    await self.scan_pages(context, "after_click")

                await context.close()
                await browser.close()
        except Exception: return []
        return list(self.hits.values())

    async def scan_pages(self, context, click_text: str) -> None:
        for page in list(context.pages):
            try:
                html_text = await page.content()
                for link in extract_whatsapp_links(html_text + " " + page.url):
                    self.capture(link, page.url, "browser_dom", click_text)
            except: continue

    async def click_relevant_controls(self, context, root_url: str) -> None:
        for page in list(context.pages):
            if self.hits: return
            try:
                loc = page.locator("a, button, [role='button']")
                scored = []
                for i in range(min(await loc.count(), 35)):
                    el = loc.nth(i)
                    try:
                        if not await el.is_visible(timeout=500): continue
                        text = await el.inner_text(timeout=500)
                        href = await el.get_attribute("href") or ""
                        href_abs = normalize_page_url(href, page.url) if href else ""
                        score = click_score(text, href_abs or text)
                        if score > 0 and (not href_abs or allowed_follow(href_abs, root_url, self.config)):
                            scored.append((score, text, el))
                    except: continue

                scored.sort(key=lambda x: x[0], reverse=True)
                for _, text, el in scored[:8]:
                    if self.hits: return
                    try:
                        await el.scroll_into_view_if_needed(timeout=1000)
                        await el.click(timeout=2500)
                        await page.wait_for_timeout(700)
                        await self.scan_pages(context, text)
                    except: continue
            except: continue

async def run_crawl_job(seeds: list[str], config: CrawlConfig, on_event: Callable[[CrawlEvent], None] | None = None) -> dict:
    def emit(kind: str, msg: str, data: dict = None):
        if on_event: on_event(CrawlEvent(kind, msg, data or {}))

    queue, visited, found = asyncio.Queue(), set(), {}
    domain_counts, root_by_url = defaultdict(int), {}
    browser_queue = asyncio.Queue()

    for raw in seeds:
        url = normalize_page_url(raw)
        if url:
            root_by_url[url] = url
            await queue.put(Candidate(url, "seed", 100, 0))

    counters = {"queued": queue.qsize(), "visited": 0, "found": 0, "duplicates": 0, "failed": 0, "browser_pages": 0}
    limits_hit = False

    async with httpx.AsyncClient(timeout=config.http_timeout, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
        sem = asyncio.Semaphore(config.http_concurrency)

        async def process_candidate(w_id: int):
            nonlocal limits_hit
            while not queue.empty() and len(visited) < config.max_pages:
                cand = await queue.get()
                url = normalize_page_url(cand.url)
                if not url or url in visited or cand.depth > config.max_depth or domain_counts[source_domain(url)] >= config.max_pages_per_domain:
                    queue.task_done()
                    continue

                visited.add(url)
                domain_counts[source_domain(url)] += 1
                counters["visited"] = len(visited)
                counters["queued"] = queue.qsize()
                emit("status", f"Fetching: {url}", {"counters": counters})

                try:
                    async with sem: final_url, body = await fetch_page(client, url)
                    links = extract_whatsapp_links(body + " " + final_url)
                    if links:
                        for link in links:
                            row = make_result(link, final_url, "http_html")
                            norm = row["normalized_url"]
                            if norm not in found:
                                found[norm] = row
                                counters["found"] = len(found)
                                emit("result", f"Found: {norm}", {"row": row})
                            else: counters["duplicates"] += 1
                        queue.task_done()
                        continue

                    root = root_by_url.get(url) or url
                    candidates = extract_candidates(body, final_url, root, config, cand.depth)
                    good_internals = [c for c in candidates if c.score >= 5]

                    for c in candidates:
                        if len(visited) + queue.qsize() >= config.max_pages:
                            limits_hit = True
                            break
                        c_url = normalize_page_url(c.url, final_url)
                        if c_url and c_url not in visited:
                            root_by_url[c_url] = root
                            await queue.put(c)

                    if config.enable_browser_fallback and good_internals and config.browser_concurrency > 0:
                        await browser_queue.put((final_url, root))
                except Exception as exc:
                    counters["failed"] += 1
                finally:
                    queue.task_done()

        await asyncio.gather(*(asyncio.create_task(process_candidate(i)) for i in range(max(1, config.http_concurrency))))

    if config.enable_browser_fallback and config.browser_concurrency > 0 and not browser_queue.empty():
        b_sem = asyncio.Semaphore(config.browser_concurrency)
        async def browser_worker(i):
            while not browser_queue.empty():
                p_url, _ = await browser_queue.get()
                if counters["browser_pages"] >= max(1, config.browser_concurrency * 25):
                    browser_queue.task_done()
                    continue
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
                            else: counters["duplicates"] += 1
                    except Exception: counters["failed"] += 1
                    finally: browser_queue.task_done()
        await asyncio.gather(*(asyncio.create_task(browser_worker(i)) for i in range(config.browser_concurrency)))

    summary = {"visited": len(visited), "found_unique": len(found), "duplicates": counters["duplicates"], "failed": counters["failed"], "finished_at": now_iso()}
    emit("status", "Job complete", {"counters": counters, "summary": summary})
    return summary
