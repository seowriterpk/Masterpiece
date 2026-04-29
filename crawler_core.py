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

BAD_HREF_PARTS = {
    "pagead2.googlesyndication.com",
    "doubleclick.net",
    "googleads",
    "/report",
    "/addgroup",
    "/login",
    "/register",
    "mailto:",
    "tel:",
    "javascript:void",
    "whatsapp.com",
    "wa.me"
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
    if not value: return ""
    return re.sub(r"\s+", " ", html.unescape(value)).strip().lower()

def normalize_page_url(raw: str | None, base: str | None = None) -> str | None:
    if not raw: return None
    raw = html.unescape(unquote(str(raw).strip().strip('"').strip("'")))
    if base: raw = urljoin(base, raw)
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}: return None
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    return urlunparse((parsed.scheme, netloc, path, "", parsed.query, ""))

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
    low = (href or "").lower()
    return any(part in low for part in BAD_HREF_PARTS)

def click_score(text: str, href: str) -> int:
    t, h = clean_text(text), clean_text(href)
    combined = f"{t} {h}"
    if any(bad in combined for bad in BAD_CLICK_WORDS): return -10
    score = 0
    for good in GOOD_CLICK_WORDS:
        if good in combined: score += 3
    if "/group/rules/" in h: score += 10
    if "/group/invite/" in h: score += 8
    if "button" in combined: score += 1
    return score

def allowed_follow(candidate_url: str, start_url: str, config: CrawlConfig) -> bool:
    c, s = urlparse(candidate_url), urlparse(start_url)
    if c.scheme not in {"http", "https"}: return False
    if is_bad_href(candidate_url): return False
    if config.same_domain_only and c.netloc.lower() != s.netloc.lower(): return False
    return True

def make_result(invite_url: str, source_page: str, method: str, click_text: str = "") -> dict:
    normalized = normalize_whatsapp_url(invite_url) or invite_url
    return {
        "invite_url": invite_url, "normalized_url": normalized,
        "source_page": source_page, "source_domain": source_domain(source_page),
        "source_label": "", "discovered_at": now_iso(),
        "extraction_method": method, "click_text": click_text,
    }

def extract_candidates(html_text: str, page_url: str, root_url: str, config: CrawlConfig, depth: int) -> list[Candidate]:
    # FIX: Swapped to native html.parser. Bypasses the lxml C-compiler failure entirely.
    soup = BeautifulSoup(html_text or "", "html.parser")
    candidates: list[Candidate] = []

    for a in soup.select("a[href]"):
        href = normalize_page_url(a.get("href"), page_url)
        if not href: continue
        text = a.get_text(" ", strip=True) or a.get("title") or ""
        score = click_score(text, href)
        if score > 0 and allowed_follow(href, root_url, config):
            candidates.append(Candidate(url=href, text=text, score=score, depth=depth + 1))

    onclick_blob = " ".join(tag.get("onclick", "") for tag in soup.select("[onclick]"))
    onclick_urls = re.findall(r"""['"]([^'"]*(?:/group/rules/|/group/invite/)[^'"]*)['"]""", onclick_blob)

    for raw in onclick_urls:
        href = normalize_page_url(raw, page_url)
        if href and allowed_follow(href, root_url, config):
            candidates.append(Candidate(url=href, text="onclick", score=8, depth=depth + 1))

    if depth < config.max_depth - 1:
        for a in soup.select("a[href]"):
            href = normalize_page_url(a.get("href"), page_url)
            if not href or not allowed_follow(href, root_url, config): continue
            text = clean_text(a.get_text(" ", strip=True) or a.get("title") or "")
            hlow = href.lower()
            if any(x in text for x in ["next", "older", "more", "page"]) or any(x in hlow for x in ["page=", "/page/", "category", "groups"]):
                if not is_bad_href(href):
                    candidates.append(Candidate(url=href, text=text or "pagination", score=1, depth=depth + 1))

    best: dict[str, Candidate] = {}
    for c in candidates:
        if c.url not in best or c.score > best[c.url].score:
            best[c.url] = c

    return sorted(best.values(), key=lambda x: x.score, reverse=True)[: config.max_candidates_per_page]

async def fetch_page(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    response = await client.get(url)
    return str(response.url), response.text or ""

def find_chromium_executable() -> str | None:
    for name in ["chromium", "chromium-browser", "google-chrome", "google-chrome-stable"]:
        path = shutil.which(name)
        if path: return path
    return None

class BrowserPiercer:
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.hits: dict[str, dict] = {}

    def capture(self, url: str, source_page: str, method: str, click_text: str = "") -> None:
        normalized = normalize_whatsapp_url(url)
        if normalized and normalized not in self.hits:
            self.hits[normalized] = make_result(url, source_page, method, click_text)

    async def pierce(self, start_url: str) -> list[dict]:
        try:
            from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
        except Exception:
            return []

        executable_path = find_chromium_executable()

        try:
            async with async_playwright() as pw:
                launch_args = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]}
                if executable_path: launch_args["executable_path"] = executable_path

                browser = await pw.chromium.launch(**launch_args)
                context = await browser.new_context(
                    java_script_enabled=True, ignore_https_errors=True,
                    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome Safari",
                )

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
                    page.on("framenavigated", lambda frame: self.capture(frame.url, page.url or start_url, "browser_frame_navigation"))

                context.on("page", watch)
                page = await context.new_page()
                watch(page)

                queue = deque([Candidate(start_url, "start", 100, 0)])
                visited = set()
                root_url = start_url

                for _step in range(10):
                    if self.hits or not queue: break
                    cand = queue.popleft()
                    current = normalize_page_url(cand.url)
                    if not current or current in visited: continue
                    visited.add(current)

                    try: await page.goto(current, wait_until="domcontentloaded", timeout=self.config.browser_timeout_ms)
                    except PlaywrightTimeoutError: pass
                    except Exception: continue

                    await page.wait_for_timeout(700)
                    await self.scan_pages(context, cand.text)

                    if self.hits: break

                    try:
                        html_text = await page.content()
                        for c in extract_candidates(html_text, page.url, root_url, self.config, cand.depth):
                            if c.url not in visited: queue.append(c)
                    except Exception: pass

                    await self.click_relevant_controls(context, root_url)
                    await self.scan_pages(context, "after_click")

                await context.close()
                await browser.close()
        except Exception:
            return []

        return list(self.hits.values())

    async def scan_pages(self, context, click_text: str) -> None:
        for page in list(context.pages):
            try:
                self.capture(page.url, page.url, "browser_current_url", click_text)
                html_text = await page.content()
                for link in extract_whatsapp_links(html_text + " " + page.url):
                    self.capture(link, page.url, "browser_dom", click_text)
            except Exception:
                continue

    async def click_relevant_controls(self, context, root_url: str) -> None:
        selector = "a, button, [role='button'], input[type='button'], input[type='submit']"
        for page in list(context.pages):
            if self.hits: return
            try:
                loc = page.locator(selector)
                count = min(await loc.count(), 35)
                scored = []

                for i in range(count):
                    el = loc.nth(i)
                    try:
                        if not await el.is_visible(timeout=500): continue
                        try: text = await el.inner_text(timeout=500)
                        except: text = await el.get_attribute("value") or ""

                        href = await el.get_attribute("href") or ""
                        href_abs = normalize_page_url(href, page.url) if href else ""
                        score = click_score(text, href_abs or text)

                        if score <= 0: continue
                        if href_abs and not allowed_follow(href_abs, root_url, self.config): continue
                        scored.append((score, text, el))
                    except: continue

                scored.sort(key=lambda x: x[0], reverse=True)

                for _score, text, el in scored[:8]:
                    if self.hits: return
                    try:
                        await el.scroll_into_view_if_needed(timeout=1000)
                        await el.click(timeout=2500)
                        await page.wait_for_timeout(700)
                        await self.scan_pages(context, text)
                    except: continue
            except: continue

async def run_crawl_job(
    seeds: list[str],
    config: CrawlConfig,
    on_event: Callable[[CrawlEvent], None] | None = None,
) -> dict:
    def emit(kind: str, message: str, data: dict | None = None) -> None:
        if on_event: on_event(CrawlEvent(kind=kind, message=message, data=data or {}))

    queue: asyncio.Queue[Candidate] = asyncio.Queue()
    visited: set[str] = set()
    found: dict[str, dict] = {}
    domain_counts = defaultdict(int)
    root_by_url: dict[str, str] = {}
    browser_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

    for raw in seeds:
        url = normalize_page_url(raw)
        if not url:
            emit("log", f"Skipped invalid seed: {raw}", {"raw": raw})
            continue
        root_by_url[url] = url
        await queue.put(Candidate(url=url, text="seed", score=100, depth=0))

    counters = {"queued": queue.qsize(), "visited": 0, "found": 0, "duplicates": 0, "failed": 0, "browser_pages": 0}
    limits_hit = False

    async with httpx.AsyncClient(
        timeout=config.http_timeout, follow_redirects=True,
        headers={"Accept": "text/html,application/xhtml+xml", "User-Agent": "Mozilla/5.0 (compatible; StreamlitGroupFinder/1.0)"},
    ) as client:
        sem = asyncio.Semaphore(config.http_concurrency)

        async def process_candidate(worker_id: int) -> None:
            nonlocal limits_hit
            while not queue.empty() and len(visited) < config.max_pages:
                cand = await queue.get()
                url = normalize_page_url(cand.url)
                if not url or url in visited or cand.depth > config.max_depth:
                    queue.task_done()
                    continue

                dom = source_domain(url)
                if domain_counts[dom] >= config.max_pages_per_domain:
                    queue.task_done()
                    continue

                visited.add(url)
                domain_counts[dom] += 1
                counters["visited"] = len(visited)
                counters["queued"] = queue.qsize()
                emit("status", f"Fetching: {url}", {"counters": counters})

                try:
                    async with sem:
                        final_url, body = await fetch_page(client, url)

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
                    good_internal_candidates = [c for c in candidates if c.score >= 5]

                    for c in candidates:
                        if len(visited) + queue.qsize() >= config.max_pages:
                            limits_hit = True
                            break
                        c_url = normalize_page_url(c.url, final_url)
                        if c_url and c_url not in visited:
                            root_by_url[c_url] = root
                            await queue.put(c)

                    if config.enable_browser_fallback and good_internal_candidates and config.browser_concurrency > 0:
                        await browser_queue.put((final_url, root))

                except Exception as exc:
                    counters["failed"] += 1
                    emit("log", f"HTTP failed: {url} -> {exc}", {"url": url, "error": str(exc)})
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(process_candidate(i)) for i in range(max(1, config.http_concurrency))]
        while any(not w.done() for w in workers):
            await asyncio.sleep(0.1)
            if queue.empty(): await asyncio.sleep(0.2)
        await asyncio.gather(*workers, return_exceptions=True)

    if config.enable_browser_fallback and config.browser_concurrency > 0 and not browser_queue.empty():
        browser_sem = asyncio.Semaphore(config.browser_concurrency)

        async def browser_worker(i: int) -> None:
            while not browser_queue.empty():
                page_url, _root = await browser_queue.get()
                if counters["browser_pages"] >= max(1, config.browser_concurrency * 25):
                    browser_queue.task_done()
                    continue

                async with browser_sem:
                    counters["browser_pages"] += 1
                    emit("status", f"Rendering JS fallback: {page_url}", {"counters": counters})
                    try:
                        piercer = BrowserPiercer(config)
                        rows = await piercer.pierce(page_url)
                        for row in rows:
                            norm = row["normalized_url"]
                            if norm not in found:
                                found[norm] = row
                                counters["found"] = len(found)
                                emit("result", f"Found via browser: {norm}", {"row": row})
                            else: counters["duplicates"] += 1
                    except Exception as exc:
                        counters["failed"] += 1
                        emit("log", f"Browser fallback failed: {page_url} -> {exc}", {"url": page_url, "error": str(exc)})
                    finally:
                        browser_queue.task_done()

        bworkers = [asyncio.create_task(browser_worker(i)) for i in range(config.browser_concurrency)]
        await asyncio.gather(*bworkers, return_exceptions=True)

    summary = {
        "visited": len(visited),
        "found_unique": len(found),
        "duplicates": counters["duplicates"],
        "failed": counters["failed"],
        "browser_pages": counters["browser_pages"],
        "limits_hit": limits_hit,
        "finished_at": now_iso(),
    }

    emit("status", "Job complete", {"counters": counters, "summary": summary})
    emit("log", "Job complete", summary)
    return summary
