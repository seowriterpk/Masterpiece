import os
import streamlit as st

# NANO-FEATURE: Run browser install ONLY once per server boot to prevent UI lag
@st.cache_resource(show_spinner=False)
def install_browser():
    os.system("playwright install chromium")
install_browser()

from streamlit.runtime.scriptrunner import add_script_run_ctx
from streamlit_autorefresh import st_autorefresh
import httpx
import asyncio
import json
import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# FIX: Dynamic stealth import. If the library renames itself again, we bypass it instead of crashing.
try:
    from playwright_stealth import stealth
except ImportError:
    stealth = None

import threading
import random
import nest_asyncio
from filelock import FileLock, Timeout

nest_asyncio.apply()

# --- DIRECTORY & GLOBALS ---
DATA_DIR = "./finder_data"
os.makedirs(os.path.join(DATA_DIR, "cache"), exist_ok=True)

WA_REGEX = re.compile(r'(https?://(?:chat\.whatsapp\.com|wa\.me|whatsapp\.com/channel)/[A-Za-z0-9_-]+)', re.IGNORECASE)
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15"
]

# --- OS-LEVEL SAFE STORAGE ---
class Storage:
    @classmethod
    def init_files(cls):
        files = {"results.json": [], "logs.json": [], "settings.json": {
            "concurrency": 3, "timeout": 15, "use_js": True, "stealth": True
        }}
        for f, default in files.items():
            path = os.path.join(DATA_DIR, f)
            if not os.path.exists(path):
                cls.save(f, default)

    @classmethod
    def load(cls, filename):
        path = os.path.join(DATA_DIR, filename)
        lock = FileLock(f"{path}.lock", timeout=5)
        try:
            with lock:
                with open(path, 'r') as f:
                    return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, Timeout):
            return [] if filename != "settings.json" else {}

    @classmethod
    def save(cls, filename, data):
        path = os.path.join(DATA_DIR, filename)
        lock = FileLock(f"{path}.lock", timeout=5)
        try:
            with lock:
                temp_path = f"{path}.tmp"
                with open(temp_path, 'w') as f:
                    json.dump(data, f, indent=2)
                os.replace(temp_path, path)
        except Timeout:
            pass

# --- THE GHOST EXTRACTOR ---
class DeepExtractor:
    def __init__(self, settings):
        self.settings = settings
        db = Storage.load("results.json")
        self.global_found = set(r['invite_url'] for r in db) if db else set()

    async def tier1_httpx(self, url, client):
        try:
            resp = await client.get(url, headers={"User-Agent": random.choice(UAS)}, follow_redirects=True, timeout=self.settings['timeout'])
            return WA_REGEX.findall(resp.text), resp.text
        except:
            return [], ""

    async def tier3_to_5_playwright(self, url):
        js_links = []
        browser = None 
        
        async def handle_response(response):
            try:
                if response.ok and response.request.resource_type in ["fetch", "xhr"]:
                    text = await response.text()
                    js_links.extend(WA_REGEX.findall(text))
            except: pass

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True, 
                    args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--single-process']
                )
                context = await browser.new_context(user_agent=random.choice(UAS))
                page = await context.new_page()
                
                # Dynamic stealth injection
                if self.settings.get('stealth', True) and stealth:
                    await stealth(page)
                    
                page.on("response", handle_response)
                page.set_default_timeout(self.settings['timeout'] * 1000)
                
                try:
                    await page.goto(url, wait_until="domcontentloaded")
                except PlaywrightTimeout:
                    pass

                # Aggressive clicking
                if self.settings.get('use_js', True):
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await page.wait_for_timeout(1000)
                        buttons = await page.locator("text=/(join|whatsapp|wa\\.me|chat|group)/i").all()
                        for btn in buttons[:2]:
                            if await btn.is_visible():
                                await btn.click(force=True, timeout=1000)
                                await page.wait_for_timeout(800)
                    except: pass
                    
                final_html = await page.content()
                js_links.extend(WA_REGEX.findall(final_html))
                
        except Exception:
            pass 
        finally:
            if browser:
                await browser.close()

        return js_links

    async def process_url(self, url, sem, client):
        async with sem:
            st.session_state.stats['active_url'] = url
            found_this_run = set()

            t1_links, raw_html = await self.tier1_httpx(url, client)
            found_this_run.update(t1_links)

            if not found_this_run or self.settings.get('use_js', True):
                t3_links = await self.tier3_to_5_playwright(url)
                found_this_run.update(t3_links)

            new_inserts = []
            for link in found_this_run:
                if link not in self.global_found:
                    self.global_found.add(link)
                    rec = {
                        "invite_url": link, "source_domain": url.split('/')[2] if '//' in url else url,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "status": "New"
                    }
                    new_inserts.append(rec)
                    st.session_state.live_queue.append(rec)

            if new_inserts:
                db = Storage.load("results.json")
                db.extend(new_inserts)
                Storage.save("results.json", db)

            st.session_state.stats['processed'] += 1

    async def orchestrate(self, urls):
        sem = asyncio.Semaphore(min(self.settings.get('concurrency', 3), 5))
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
        async with httpx.AsyncClient(verify=False, limits=limits) as client:
            tasks = [self.process_url(u, sem, client) for u in urls]
            await asyncio.gather(*tasks, return_exceptions=True)
            
        st.session_state.is_running = False

# --- THREAD MANAGER ---
def launch_crawler(urls):
    st.session_state.is_running = True
    st.session_state.live_queue = []
    st.session_state.stats = {'total': len(urls), 'processed': 0, 'active_url': ''}
    settings = Storage.load("settings.json")
    
    extractor = DeepExtractor(settings)
    
    def worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(extractor.orchestrate(urls))
        loop.close()
        
    t = threading.Thread(target=worker, daemon=True)
    add_script_run_ctx(t) 
    t.start()

# --- FRONTEND ARCHITECTURE ---
st.set_page_config(page_title="SCOLO Extractor", layout="wide")
Storage.init_files()

for key in ['is_running', 'live_queue', 'stats']:
    if key not in st.session_state:
        st.session_state[key] = False if key == 'is_running' else [] if key == 'live_queue' else {'total':0, 'processed':0, 'active_url':''}

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Control Desk", "Live Radar", "Database Manager", "Sys Settings"])

if page == "Control Desk":
    st.markdown("### Target Acquisition")
    target_input = st.text_area("Input URLs (Batch Mode)")
    
    if st.button("EXECUTE SWARM", type="primary", disabled=st.session_state.is_running):
        clean_urls = [u.strip() for u in target_input.split('\n') if u.strip().startswith('http')]
        if clean_urls:
            launch_crawler(clean_urls)
            st.rerun()
            
    if st.session_state.is_running:
        st.progress(st.session_state.stats['processed'] / max(1, st.session_state.stats['total']))
        st.caption(f"Assaulting: {st.session_state.stats['active_url']}")

elif page == "Live Radar":
    st.markdown("### Live Telemetry")
    if st.session_state.is_running:
        st_autorefresh(interval=2000, key="radar_ping")
        
    if st.session_state.live_queue:
        st.dataframe(pd.DataFrame(st.session_state.live_queue), use_container_width=True)
    else:
        st.info("Radar clear. Awaiting data.")

elif page == "Database Manager":
    st.markdown("### Master Database")
    db_data = Storage.load("results.json")
    if db_data:
        df_db = pd.DataFrame(db_data).tail(500)[::-1] 
        df_db.insert(0, "Action", False)
        
        edited = st.data_editor(df_db, use_container_width=True, hide_index=True)
        
        if st.button("Delete Selected Rows"):
            urls_to_delete = edited[edited['Action']]['invite_url'].tolist()
            new_db = [r for r in db_data if r['invite_url'] not in urls_to_delete]
            Storage.save("results.json", new_db)
            st.rerun()

elif page == "Sys Settings":
    st.markdown("### Engine Configuration")
    settings = Storage.load("settings.json")
    
    new_conc = st.slider("Max Concurrency", 1, 5, settings.get('concurrency', 3))
    use_stealth = st.toggle("Anti-Bot Stealth", value=settings.get('stealth', True))
    
    if st.button("Commit Configuration"):
        settings.update({"concurrency": new_conc, "stealth": use_stealth})
        Storage.save("settings.json", settings)
        st.success("Config locked.")
