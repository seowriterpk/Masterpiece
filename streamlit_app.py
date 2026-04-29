import os
import streamlit as st
import json
import re
import html
import urllib.parse
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse
import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# --- CORE BROWSER INIT ---
# Gotta make sure the server actually has the engine before we try to drive it.
@st.cache_resource(show_spinner=False)
def install_browser():
    os.system("playwright install chromium")
install_browser()

# --- THE FILESYSTEM (No databases, just raw local files) ---
DATA_DIR = "./finder_data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, "results.json")
LOG_FILE = os.path.join(DATA_DIR, "engine_log.txt")

if not os.path.exists(DB_FILE):
    with open(DB_FILE, 'w') as f: json.dump([], f)

def load_db():
    try:
        with open(DB_FILE, 'r') as f: return json.load(f)
    except: return []

def save_db(data):
    with open(DB_FILE, 'w') as f: json.dump(data, f, indent=2)

def write_log(msg):
    with open(LOG_FILE, 'a') as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

# The golden string we are hunting.
WA_REGEX = re.compile(r'(?:https?://)?(?:www\.)?(?:chat\.whatsapp\.com|wa\.me|wa\.link|whatsapp\.com/channel)/[A-Za-z0-9_-]+', re.IGNORECASE)

# --- GOOGLEBOT SPOOFING PROFILES ---
AGENTS = {
    "Googlebot Smartphone": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Chrome Windows (Stealth)": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Wipes out the webdriver flags so Cloudflare doesn't instantly block us.
STEALTH_PAYLOAD = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
window.chrome = { runtime: {} };
"""

# --- THE FUNNEL-PIERCING SPIDER ---
class ApexPredator:
    def __init__(self, config):
        self.config = config
        self.visited_urls = set()
        self.global_links_found = set(r['invite_url'] for r in load_db())
        self.session_found = []
    
    def normalize_and_store(self, raw_str, source_url):
        # Unpack weird URL encoding that directory sites use to hide links
        decoded = html.unescape(urllib.parse.unquote(str(raw_str)))
        matches = WA_REGEX.findall(decoded)
        added_any = False
        
        for match in matches:
            cln = match.lower().strip()
            if not cln.startswith('http'):
                cln = 'https://' + cln
            if cln not in self.global_links_found:
                self.global_links_found.add(cln)
                self.session_found.append({
                    "invite_url": cln,
                    "source": source_url.split('/')[2] if '//' in source_url else source_url,
                    "found_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                added_any = True
        return added_any
        
    def extract_funnel_links(self, html_content, base_url):
        """Hunts for the breadcrumbs. Finds 'invite', 'join', 'rules' internal links."""
        soup = BeautifulSoup(html_content, 'lxml')
        funnel_paths = []
        
        # Look for literal buttons or links that smell like the next step in the maze
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text().lower()
            
            if href.startswith(('javascript:', 'mailto:', 'tel:', '#')): 
                continue
                
            full_url = urljoin(base_url, href)
            
            # If the URL contains keywords like 'invite', 'join', 'group', or 'rules'
            if any(k in full_url.lower() for k in ['/invite/', '/join', '/rules', '/group/']):
                funnel_paths.append(full_url)
            # Or if the button text itself is a dead giveaway
            elif any(k in text for k in ['join', 'agree', 'click here', 'continue']):
                funnel_paths.append(full_url)
                
        return list(set(funnel_paths))

    def assault_target(self, url):
        write_log(f"Deploying Playwright to: {url}")
        next_layer_urls = []
        
        try:
            with sync_playwright() as p:
                args = ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled']
                if not self.config['load_images']: 
                    args.append('--blink-settings=imagesEnabled=false')
                
                browser = p.chromium.launch(headless=self.config['headless'], args=args)
                context = browser.new_context(
                    user_agent=AGENTS[self.config['user_agent']],
                    extra_http_headers={"Referer": "https://www.google.com/"} if self.config['spoof_referer'] else {},
                    viewport={'width': 1920, 'height': 1080},
                    bypass_csp=True
                )
                
                if self.config['inject_stealth']:
                    context.add_init_script(STEALTH_PAYLOAD)
                    
                page = context.new_page()
                page.set_default_timeout(self.config['pw_timeout'] * 1000)

                # NANO-FEATURE: The Network Interceptor. 
                # This is the magic bullet. If clicking "I agree" triggers a redirect to chat.whatsapp.com, 
                # we catch it IN THE AIR before the browser even loads it.
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

                # DOM Smash: Click everything that looks like an entry button
                if self.config['auto_click']:
                    # Look for buttons that say "Join Group", "I Agree", etc.
                    click_targets = page.locator("a, button, div.button1, span.joinbtn").filter(has_text=re.compile(fr"({self.config['click_keywords']})", re.I))
                    
                    count = click_targets.count()
                    for i in range(min(count, self.config['max_clicks'])):
                        try:
                            loc = click_targets.nth(i)
                            if loc.is_visible():
                                loc.click(force=True, timeout=1000)
                                page.wait_for_timeout(1000) # Wait for the potential redirect or popup
                        except: pass

                # Grab whatever HTML is left standing
                html_body = ""
                try: html_body = page.content()
                except: pass
                
                # Scan the raw HTML
                self.normalize_and_store(html_body, url)

                # If deep crawling is on, rip the internal funnel links so we can chase them
                if self.config['deep_crawl'] and html_body:
                    next_layer_urls = self.extract_funnel_links(html_body, url)

                browser.close()
        except Exception as e:
            write_log(f"Total failure on {url}: {str(e)}")
            
        return next_layer_urls

    def execute_hunt(self, root_urls, ui_status, ui_bar, ui_stats):
        # The queue holds dictionaries of URL and their current depth
        queue = [{'url': ru, 'depth': 0} for ru in root_urls]
        pages_processed = 0
        
        while queue and pages_processed < self.config['global_max_pages']:
            current = queue.pop(0)
            target_url = current['url']
            current_depth = current['depth']
            
            if target_url in self.visited_urls:
                continue
                
            self.visited_urls.add(target_url)
            ui_status.markdown(f"**Breaching Depth [{current_depth}]:** `{target_url}`")
            
            # Hit the target with the browser
            funnel_links = self.assault_target(target_url)
            
            # If we haven't hit max depth, push the new breadcrumbs into the queue
            if self.config['deep_crawl'] and current_depth < self.config['max_depth']:
                added = 0
                for f_link in funnel_links:
                    if f_link not in self.visited_urls and added < self.config['max_internals_per_page']:
                        queue.append({'url': f_link, 'depth': current_depth + 1})
                        added += 1
                        
            pages_processed += 1
            
            # Update UI
            p = min(1.0, pages_processed / self.config['global_max_pages'])
            ui_bar.progress(p)
            ui_stats.info(f"🕷️ Pages Swept: **{pages_processed}** | 📝 Rat-Maze Queue: **{len(queue)}** | 🔗 Escaped Links: **{len(self.session_found)}**")
            
        return self.session_found


# --- FRONTEND UI DASHBOARD ---
st.set_page_config(page_title="SCOLO Deep Funnel Piercer", layout="wide", initial_sidebar_state="expanded")

with st.sidebar:
    st.header("⚙️ Funnel-Piercer Config")
    
    with st.expander("🤖 Disguise & Stealth", expanded=True):
        u_agent = st.selectbox("Spoof Identity", list(AGENTS.keys()))
        inject_stealth = st.toggle("Override Hardware Signatures", value=True)
        spoof_referer = st.toggle("Forge Google Referer", value=True)
        
    with st.expander("🕸️ The Rat-Maze Tracker", expanded=True):
        do_deep = st.toggle("Chase Internal 'Join' Links", value=True, help="Crucial for sites like groupsor.link that hide the final URL behind multiple pages.")
        max_depth = st.slider("Maximum Depth Limit", 1, 5, 3)
        max_inter = st.slider("Max Links To Chase Per Page", 1, 20, 10)
        global_cap = st.slider("Total Page Kill-Switch", 1, 200, 30)
        
    with st.expander("⚡ Button Smashing Rules", expanded=True):
        auto_click = st.toggle("Aggressively Click Elements", value=True)
        click_kws = st.text_input("Regex Trigger Words", value="join|agree|continue|rules")
        max_clx = st.slider("Max Clicks Per Page", 1, 10, 4)
        
    with st.expander("⏱️ Engine Timing"):
        load_images = st.toggle("Load Images (Turn off for speed)", value=False)
        pw_tout = st.slider("Browser Timeout (sec)", 10, 60, 20)
        pg_delay = st.slider("DDoS / Cloudflare Wait (sec)", 1, 10, 3)

CONFIG = {
    'user_agent': u_agent, 'inject_stealth': inject_stealth, 'spoof_referer': spoof_referer,
    'deep_crawl': do_deep, 'max_depth': max_depth, 'max_internals_per_page': max_inter,
    'global_max_pages': global_cap, 'headless': True, 'load_images': load_images, 
    'auto_click': auto_click, 'click_keywords': click_kws, 'max_clicks': max_clx, 
    'pw_timeout': pw_tout, 'page_delay': pg_delay
}

st.title("🌐 Deep Funnel Extractor")
st.markdown("*Engineered to chase links across multi-page 'I Agree' funnels and intercept hidden network redirects.*")

if 'session_run' not in st.session_state:
    st.session_state.session_run = []

c1, c2 = st.columns([1.2, 2])

with c1:
    st.subheader("1. Target Acquisition")
    seed_input = st.text_area("Paste Directory URLs", placeholder="https://groupsor.link/\nhttps://another-directory.com/", height=130)
    
    if st.button("🔥 INITIATE FUNNEL BREACH", type="primary", use_container_width=True):
        urls = [u.strip() for u in seed_input.split('\n') if u.strip().startswith('http')]
        if not urls:
            st.error("I need a valid HTTP/HTTPS target, boss.")
        else:
            st.session_state.session_run = []
            ui_status = st.empty()
            ui_bar = st.progress(0.0)
            ui_stats = st.empty()
            
            # Unleash the hound
            predator = ApexPredator(CONFIG)
            caught_links = predator.execute_hunt(urls, ui_status, ui_bar, ui_stats)
            
            # Save state
            db = load_db()
            for rec in caught_links:
                db.append(rec)
                st.session_state.session_run.append(rec)
                
            if caught_links: save_db(db)
            
            ui_status.success(f"Maze cleared. Dragged out {len(st.session_state.session_run)} hidden group links.")
            ui_bar.progress(1.0)

with c2:
    st.subheader("2. Confirmed Extractions")
    db = load_db()
    
    if not db:
        st.info("Database is empty. Waiting for successful breach.")
    else:
        df = pd.DataFrame(db)[::-1]
        df.insert(0, "Wipe", False)
        
        editor = st.data_editor(df, use_container_width=True, hide_index=True)
        
        ca, cb = st.columns(2)
        with ca:
            if st.button("❌ Delete Checked", use_container_width=True):
                wipe_list = editor[editor['Wipe']]['invite_url'].tolist()
                new_db = [x for x in db if x['invite_url'] not in wipe_list]
                save_db(new_db)
                st.rerun()
        with cb:
            csv_data = pd.DataFrame(db).to_csv(index=False).encode('utf-8')
            st.download_button("💾 Download Full CSV", csv_data, "whatsapp_funnel_links.csv", "text/csv", use_container_width=True)
        
        st.divider()
        if st.button("☢️ FORMAT ENTIRE DATABASE", type="primary"):
            save_db([])
            st.session_state.session_run = []
            st.rerun()
