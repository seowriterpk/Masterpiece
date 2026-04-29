import os
import streamlit as st
import json
import re
import pandas as pd
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# Force Streamlit to install the browser on boot
@st.cache_resource(show_spinner=False)
def install_browser():
    os.system("playwright install chromium")
install_browser()

# --- GLOBALS & SIMPLE STORAGE ---
DATA_DIR = "./finder_data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, "results.json")

if not os.path.exists(DB_FILE):
    with open(DB_FILE, 'w') as f:
        json.dump([], f)

def load_db():
    try:
        with open(DB_FILE, 'r') as f:
            return json.load(f)
    except:
        return []

def save_db(data):
    with open(DB_FILE, 'w') as f:
        json.dump(data, f, indent=2)

WA_REGEX = re.compile(r'(https?://(?:chat\.whatsapp\.com|wa\.me|whatsapp\.com/channel)/[A-Za-z0-9_-]+)', re.IGNORECASE)

# --- THE SIMPLE SYNCHRONOUS EXTRACTOR ---
def extract_links(url, use_js=True):
    found_links = []
    
    # 1. Fast HTTP Check
    try:
        resp = httpx.get(url, timeout=10, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        found_links.extend(WA_REGEX.findall(resp.text))
        
        soup = BeautifulSoup(resp.text, 'lxml')
        for a in soup.find_all('a', href=True):
            found_links.extend(WA_REGEX.findall(a['href']))
    except Exception as e:
        pass

    # 2. Playwright JS Check (Only if requested)
    if use_js:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
                page = browser.new_page()
                page.set_default_timeout(15000)
                
                # Network sniffer
                page.on("response", lambda response: found_links.extend(WA_REGEX.findall(response.text())) if response.ok and response.request.resource_type in ["fetch", "xhr"] else None)
                
                try:
                    page.goto(url, wait_until="domcontentloaded")
                except PlaywrightTimeout:
                    pass
                
                # Aggressive scroll & click
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1000)
                    buttons = page.locator("text=/(join|whatsapp|wa\\.me|chat|group)/i").all()
                    for btn in buttons[:2]:
                        if btn.is_visible():
                            btn.click(force=True, timeout=1000)
                            page.wait_for_timeout(500)
                except:
                    pass
                
                found_links.extend(WA_REGEX.findall(page.content()))
                browser.close()
        except Exception:
            pass
            
    return list(set(found_links))

# --- UI / WEBSITE SYSTEM ---
st.set_page_config(page_title="Saeed's Link Finder", layout="wide")

st.title("🕷️ WA Group Finder")
st.write("A clean, server-processed system to discover and manage WhatsApp links.")

# Session memory for the live run
if 'current_run' not in st.session_state:
    st.session_state.current_run = []

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. Input Targets")
    urls_input = st.text_area("Paste URLs here (one per line)", height=150)
    use_js = st.checkbox("Use Deep JS Scraping (Slower, but finds hidden links)", value=True)
    
    if st.button("🚀 Start Processing", type="primary"):
        urls = [u.strip() for u in urls_input.split('\n') if u.strip().startswith('http')]
        if not urls:
            st.warning("Please enter valid URLs.")
        else:
            st.session_state.current_run = []
            db = load_db()
            global_found = set(r['invite_url'] for r in db)
            
            # Placeholders for live UI updates
            status_text = st.empty()
            progress_bar = st.progress(0)
            live_table = st.empty()
            
            for i, url in enumerate(urls):
                status_text.text(f"Processing ({i+1}/{len(urls)}): {url}")
                
                # Run extraction
                new_links = extract_links(url, use_js=use_js)
                
                # Save & Display
                added_count = 0
                for link in new_links:
                    if link not in global_found:
                        global_found.add(link)
                        rec = {
                            "invite_url": link,
                            "source_domain": url.split('/')[2] if '//' in url else url,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        db.append(rec)
                        st.session_state.current_run.append(rec)
                        added_count += 1
                
                if added_count > 0:
                    save_db(db) # Save to browser/server storage immediately
                
                # Update live table
                if st.session_state.current_run:
                    live_table.dataframe(pd.DataFrame(st.session_state.current_run), use_container_width=True)
                
                progress_bar.progress((i + 1) / len(urls))
                
            status_text.success(f"Done! Found {len(st.session_state.current_run)} new links in this batch.")

with col2:
    st.subheader("2. Database Manager")
    db = load_db()
    if db:
        df = pd.DataFrame(db)[::-1] # Reverse to show newest first
        df.insert(0, "Delete", False)
        
        edited_df = st.data_editor(df, use_container_width=True, hide_index=True)
        
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🗑️ Remove Selected"):
                to_delete = edited_df[edited_df['Delete']]['invite_url'].tolist()
                new_db = [r for r in db if r['invite_url'] not in to_delete]
                save_db(new_db)
                st.rerun()
        with col_b:
            csv = pd.DataFrame(db).to_csv(index=False).encode('utf-8')
            st.download_button("💾 Export All to CSV", csv, "wa_links.csv", "text/csv")
    else:
        st.info("No links in database yet.")

st.divider()
if st.button("⚠️ Format System (Delete Everything)"):
    save_db([])
    st.session_state.current_run = []
    st.rerun()
