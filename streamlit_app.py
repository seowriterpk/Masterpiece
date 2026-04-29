import os
import streamlit as st
import json
import re
import html
import urllib.parse
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# --- CORE BROWSER INIT ---
@st.cache_resource(show_spinner=False)
def install_browser():
    os.system("playwright install chromium")
install_browser()

# --- EPHEMERAL STATE MANAGEMENT (Cloud-Optimized) ---
# We no longer rely on permanent local files. Everything lives in session state
# and relies on the user importing/exporting their master backup.
if 'master_db' not in st.session_state:
    st.session_state.master_db = []
if 'session_run' not in st.session_state:
    st.session_state.session_run = []

WA_REGEX = re.compile(r'(?:https?://)?(?:www\.)?(?:chat\.whatsapp\.com|wa\.me|wa\.link|whatsapp\.com/channel)/[A-Za-z0-9_-]+', re.IGNORECASE)

AGENTS = {
    "Googlebot Smartphone": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Chrome Windows": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

STEALTH_PAYLOAD = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 4});
window.chrome = { runtime: {} };
"""

# --- THE LIGHTWEIGHT FUNNEL-PIERCING SPIDER ---
class CloudPredator:
    def __init__(self, config):
        self.config = config
        self.visited_urls = set()
        # Deduplicate against the in-memory master DB
        self.global_links_found = set(r['invite_url'] for r in st.session_state.master_db)
        self.session_found = []
    
    def normalize_and_store(self, raw_str, source_url):
        decoded = html.unescape(urllib.parse.unquote(str(raw_str)))
        matches = WA_REGEX.findall(decoded)
        added_any = False
        
        for match in matches:
            cln = match.lower().strip()
            if not cln.startswith('http'):
                cln = 'https://' + cln
            if cln not in self.global_links_found:
                self.global_links_found.add(cln)
                entry = {
                    "invite_url": cln,
                    "source": source_url.split('/')[2] if '//' in source_url else source_url,
                    "found_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                self.session_found.append(entry)
                # Push directly to Streamlit memory
                st.session_state.master_db.append(entry)
                st.session_state.session_run.append(entry)
                added_any = True
        return added_any
        
    def extract_funnel_links(self, html_content, base_url):
        soup = BeautifulSoup(html_content, 'lxml')
        funnel_paths = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text().lower()
            if href.startswith(('javascript:', 'mailto:', 'tel:', '#')): continue
            full_url = urljoin(base_url, href)
            if any(k in full_url.lower() for k in ['/invite/', '/join', '/rules', '/group/']) or any(k in text for k in ['join', 'agree', 'continue']):
                funnel_paths.append(full_url)
        return list(set(funnel_paths))

    def assault_target(self, url):
        next_layer_urls = []
        try:
            with sync_playwright() as p:
                # Optimized args for 2.7GB RAM limit on Streamlit Cloud
                args = ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled', '--single-process', '--disable-gpu']
                if not self.config['load_images']: args.append('--blink-settings=imagesEnabled=false')
                
                browser = p.chromium.launch(headless=True, args=args)
                context = browser.new_context(
                    user_agent=AGENTS[self.config['user_agent']],
                    viewport={'width': 1920, 'height': 1080},
                    bypass_csp=True
                )
                
                if self.config['inject_stealth']: context.add_init_script(STEALTH_PAYLOAD)
                page = context.new_page()
                page.set_default_timeout(self.config['pw_timeout'] * 1000)

                def handle_request(route, request):
                    if "chat.whatsapp.com" in request.url or "wa.me" in request.url:
                        self.normalize_and_store(request.url, url)
                    route.continue_()
                
                page.route("**/*", handle_request)

                try:
                    page.goto(url, wait_until="domcontentloaded")
                    page.wait_for_timeout(self.config['page_delay'] * 1000) 
                except PlaywrightTimeout:
                    pass

                if self.config['auto_click']:
                    click_targets = page.locator("a, button, div.button1, span.joinbtn").filter(has_text=re.compile(fr"({self.config['click_keywords']})", re.I))
                    for i in range(min(click_targets.count(), self.config['max_clicks'])):
                        try:
                            loc = click_targets.nth(i)
                            if loc.is_visible():
                                loc.click(force=True, timeout=1000)
                                page.wait_for_timeout(1000) 
                        except: pass

                html_body = ""
                try: html_body = page.content()
                except: pass
                
                self.normalize_and_store(html_body, url)

                if self.config['deep_crawl'] and html_body:
                    next_layer_urls = self.extract_funnel_links(html_body, url)

                browser.close()
        except Exception:
            pass
            
        return next_layer_urls

    def execute_hunt(self, root_urls, ui_status, ui_bar, ui_stats):
        queue = [{'url': ru, 'depth': 0} for ru in root_urls]
        pages_processed = 0
        
        while queue and pages_processed < self.config['global_max_pages']:
            current = queue.pop(0)
            target_url = current['url']
            current_depth = current['depth']
            
            if target_url in self.visited_urls: continue
                
            self.visited_urls.add(target_url)
            ui_status.markdown(f"**Breaching Depth [{current_depth}]:** `{target_url}`")
            
            funnel_links = self.assault_target(target_url)
            
            if self.config['deep_crawl'] and current_depth < self.config['max_depth']:
                added = 0
                for f_link in funnel_links:
                    if f_link not in self.visited_urls and added < self.config['max_internals_per_page']:
                        queue.append({'url': f_link, 'depth': current_depth + 1})
                        added += 1
                        
            pages_processed += 1
            
            p = min(1.0, pages_processed / self.config['global_max_pages'])
            ui_bar.progress(p)
            ui_stats.info(f"🕷️ Pages Swept: **{pages_processed}** | 📝 Queued: **{len(queue)}** | 🔗 New Links: **{len(self.session_found)}**")
            
        return self.session_found


# --- FRONTEND UI DASHBOARD ---
st.set_page_config(page_title="SCOLO Cloud Extractor", layout="wide", initial_sidebar_state="expanded")

with st.sidebar:
    st.header("⚙️ Engine Configuration")
    
    with st.expander("🤖 Identity & Stealth", expanded=True):
        u_agent = st.selectbox("Spoof Identity", list(AGENTS.keys()))
        inject_stealth = st.toggle("Override Hardware Signatures", value=True)
        
    with st.expander("🕸️ Rat-Maze Tracker", expanded=True):
        do_deep = st.toggle("Chase Internal 'Join' Links", value=True)
        max_depth = st.slider("Maximum Depth Limit", 1, 5, 2)
        max_inter = st.slider("Max Links To Chase Per Page", 1, 20, 5)
        global_cap = st.slider("Total Page Kill-Switch", 1, 100, 20, help="Keep this low to prevent Streamlit Cloud from throttling CPU.")
        
    with st.expander("⚡ Interaction Rules", expanded=True):
        auto_click = st.toggle("Aggressively Click Elements", value=True)
        click_kws = st.text_input("Regex Trigger Words", value="join|agree|continue|rules")
        max_clx = st.slider("Max Clicks Per Page", 1, 10, 3)
        
    with st.expander("⏱️ Cloud Timing Limitations"):
        load_images = st.toggle("Load Images", value=False)
        pw_tout = st.slider("Browser Timeout (sec)", 10, 60, 15)
        pg_delay = st.slider("DDoS Wait (sec)", 1, 10, 2)

CONFIG = {
    'user_agent': u_agent, 'inject_stealth': inject_stealth,
    'deep_crawl': do_deep, 'max_depth': max_depth, 'max_internals_per_page': max_inter,
    'global_max_pages': global_cap, 'headless': True, 'load_images': load_images, 
    'auto_click': auto_click, 'click_keywords': click_kws, 'max_clicks': max_clx, 
    'pw_timeout': pw_tout, 'page_delay': pg_delay
}

st.title("☁️ Cloud-Optimized Link Extractor")
st.markdown("*Memory-safe, ephemeral architecture designed specifically for Streamlit Cloud constraints.*")

# --- STATE REHYDRATION (IMPORT/EXPORT) ---
with st.expander("💾 State Management (CRITICAL FOR CLOUD DEPLOYMENTS)", expanded=not bool(st.session_state.master_db)):
    st.warning("Streamlit Cloud containers reset frequently. You MUST export your database to save it, and re-upload it here to restore your deduplication memory.")
    
    col_up, col_down = st.columns(2)
    with col_up:
        uploaded_file = st.file_uploader("Upload Previous State Backup (JSON)", type="json")
        if uploaded_file is not None and st.button("Restore Memory State"):
            try:
                restored_data = json.load(uploaded_file)
                st.session_state.master_db = restored_data
                st.success(f"Restored {len(restored_data)} links into memory.")
                st.rerun()
            except Exception as e:
                st.error("Failed to parse JSON backup.")
                
    with col_down:
        if st.session_state.master_db:
            export_json = json.dumps(st.session_state.master_db, indent=2).encode('utf-8')
            st.download_button(
                label="📥 Export Master Backup (JSON)",
                data=export_json,
                file_name=f"wa_backup_state_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                mime="application/json",
                type="primary",
                use_container_width=True
            )

st.divider()

c1, c2 = st.columns([1.2, 2])

with c1:
    st.subheader("1. Target Acquisition")
    seed_input = st.text_area("Paste Directory URLs", placeholder="https://groupsor.link/", height=130)
    
    if st.button("🔥 INITIATE CLOUD BREACH", type="primary", use_container_width=True):
        urls = [u.strip() for u in seed_input.split('\n') if u.strip().startswith('http')]
        if not urls:
            st.error("I need a valid HTTP/HTTPS target.")
        else:
            st.session_state.session_run = []
            ui_status = st.empty()
            ui_bar = st.progress(0.0)
            ui_stats = st.empty()
            
            predator = CloudPredator(CONFIG)
            predator.execute_hunt(urls, ui_status, ui_bar, ui_stats)
            
            ui_status.success(f"Execution complete. Swept {len(st.session_state.session_run)} new links into memory.")
            ui_bar.progress(1.0)

with c2:
    st.subheader(f"2. Ephemeral Memory DB ({len(st.session_state.master_db)} total)")
    
    if not st.session_state.master_db:
        st.info("Memory is empty. Upload a backup or run a breach.")
    else:
        df = pd.DataFrame(st.session_state.master_db)[::-1]
        df.insert(0, "Wipe", False)
        
        editor = st.data_editor(df, use_container_width=True, hide_index=True)
        
        ca, cb = st.columns(2)
        with ca:
            if st.button("❌ Delete Checked", use_container_width=True):
                wipe_list = editor[editor['Wipe']]['invite_url'].tolist()
                st.session_state.master_db = [x for x in st.session_state.master_db if x['invite_url'] not in wipe_list]
                st.rerun()
        with cb:
            csv_data = pd.DataFrame(st.session_state.master_db).drop(columns=['Wipe'], errors='ignore').to_csv(index=False).encode('utf-8')
            st.download_button("📊 Download as CSV", csv_data, "whatsapp_links.csv", "text/csv", use_container_width=True)
            
        st.write("")
        if st.button("☢️ PURGE ACTIVE MEMORY"):
            st.session_state.master_db = []
            st.session_state.session_run = []
            st.rerun()
