from __future__ import annotations
import asyncio, io, time
from pathlib import Path
from typing import Any
import pandas as pd
import streamlit as st

from crawler_core import CrawlConfig, CrawlEvent, run_crawl_job
from storage import LocalStore, ensure_app_dirs

# --- LIQUID RED PREMIUM UI CSS ---
st.set_page_config(page_title="SCOLO Crimson Extractor", layout="wide", page_icon="🩸")

st.markdown("""
    <style>
    /* Global Base */
    .stApp {
        background-color: #0d0404;
        color: #f5f5f5;
    }
    
    /* Neon Red Accents & Headers */
    h1, h2, h3 {
        color: #ff3333 !important;
        text-shadow: 0 0 10px rgba(255, 51, 51, 0.4);
        font-family: 'Helvetica Neue', sans-serif;
        font-weight: 800;
    }
    
    /* Liquid Gradient Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #8b0000 0%, #ff1744 100%);
        color: white !important;
        border: none;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 700;
        letter-spacing: 1px;
        box-shadow: 0 4px 15px rgba(255, 23, 68, 0.3);
        transition: all 0.3s ease-in-out;
        text-transform: uppercase;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #ff1744 0%, #ff5252 100%);
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(255, 23, 68, 0.6);
    }
    
    /* Input Fields & Text Areas */
    .stTextInput > div > div > input, .stTextArea > div > textarea {
        background-color: #1a0808 !important;
        color: #ff8a80 !important;
        border: 1px solid #d32f2f !important;
        border-radius: 6px;
        transition: border-color 0.3s ease;
    }
    .stTextInput > div > div > input:focus, .stTextArea > div > textarea:focus {
        border-color: #ff5252 !important;
        box-shadow: 0 0 8px rgba(255, 82, 82, 0.4) !important;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #140505 !important;
        border-right: 1px solid #3e0b0b;
    }
    
    /* Dataframe & Tables */
    [data-testid="stDataFrame"] {
        border: 1px solid #5c1010;
        border-radius: 8px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.5);
    }
    
    /* Success/Info Alerts */
    .stAlert {
        background-color: #1f0b0b !important;
        border: 1px solid #b71c1c !important;
        color: #ffcdd2 !important;
    }
    
    /* Fade In Animation */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .main .block-container {
        animation: fadeIn 0.8s ease-out;
    }
    </style>
""", unsafe_allow_html=True)

# --- SYSTEM INIT ---
DATA_DIR = Path("data")
CACHE_DIR = Path("cache")
EXPORT_DIR = Path("exports")

def init_state() -> None:
    ensure_app_dirs(DATA_DIR, CACHE_DIR, EXPORT_DIR)
    if "store" not in st.session_state: st.session_state.store = LocalStore(DATA_DIR)
    if "settings" not in st.session_state: st.session_state.settings = st.session_state.store.load_settings()
    if "results" not in st.session_state: st.session_state.results = st.session_state.store.load_results()
    if "last_job_summary" not in st.session_state: st.session_state.last_job_summary = {}

def normalize_lines(raw: str) -> list[str]:
    return list(dict.fromkeys([line.strip() for line in (raw or "").splitlines() if line.strip()]))

def settings_panel() -> CrawlConfig:
    s = st.session_state.settings
    with st.sidebar:
        st.header("⚙️ Overlord Settings")
        s["max_pages"] = st.number_input("Max Pages", min_value=5, max_value=1000, value=int(s.get("max_pages", 150)))
        s["max_depth"] = st.number_input("Max Depth", min_value=0, max_value=8, value=int(s.get("max_depth", 3)))
        s["http_concurrency"] = st.number_input("HTTP Concurrency", min_value=1, max_value=40, value=int(s.get("http_concurrency", 12)))
        s["browser_concurrency"] = st.number_input("Browser Concurrency", min_value=0, max_value=3, value=int(s.get("browser_concurrency", 1)))
        s["enable_browser_fallback"] = st.toggle("Enable Playwright Engine", value=bool(s.get("enable_browser_fallback", True)))
        
        if st.button("Save Configuration", use_container_width=True):
            st.session_state.store.save_settings(s)
            st.success("Config Locked.")

    return CrawlConfig(
        max_pages=int(s["max_pages"]), max_depth=int(s["max_depth"]),
        http_concurrency=int(s["http_concurrency"]), browser_concurrency=int(s["browser_concurrency"]),
        enable_browser_fallback=bool(s["enable_browser_fallback"])
    )

def results_dataframe(results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=["select", "review_status", "normalized_url", "source_page", "extraction_method"])
    df = pd.DataFrame(results)
    for col in ["review_status", "normalized_url", "invite_url", "source_page", "extraction_method", "kept"]:
        if col not in df.columns: df[col] = ""
    df.insert(0, "select", False)
    return df[["select", "review_status", "normalized_url", "invite_url", "source_page", "extraction_method", "kept"]]

async def run_job_async(seeds: list[str], config: CrawlConfig, ui_slots: dict) -> dict:
    store = st.session_state.store
    all_results = st.session_state.results
    seen = {r.get("normalized_url") for r in all_results if r.get("normalized_url")}
    live_rows = []
    
    counters = {"queued": len(seeds), "visited": 0, "found": 0}

    def on_event(event: CrawlEvent) -> None:
        if event.kind == "status":
            counters.update(event.data.get("counters", {}))
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
            counters["found"] += 1
            if len(live_rows) % 3 == 0: store.save_results(all_results)

    summary = await run_crawl_job(seeds=seeds, config=config, on_event=on_event)
    store.save_results(all_results)
    st.session_state.results = all_results
    return {**summary, "new_results": len(live_rows)}

def main() -> None:
    init_state()
    st.title("🩸 Crimson Spider Core")
    st.caption("Aggressive, Cloud-Optimized WhatsApp Group Discovery. (Channels and direct WA crawls disabled by design).")

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
        df = results_dataframe(st.session_state.results)
        
        if df.empty:
            st.info("System memory empty.")
        else:
            edited = st.data_editor(df, use_container_width=True, hide_index=True)
            selected = edited[edited["select"] == True] if "select" in edited.columns else pd.DataFrame()
            
            c1, c2 = st.columns(2)
            with c1:
                if st.button("🗑️ Purge Selected Data", use_container_width=True):
                    st.session_state.store.bulk_update_results(
                        selected["normalized_url"].dropna().tolist(),
                        {"review_status": "remove", "kept": False}
                    )
                    st.session_state.results = st.session_state.store.load_results()
                    st.rerun()
            with c2:
                if st.button("⚠️ Wipe Entire Database", type="primary", use_container_width=True):
                    st.session_state.store.save_results([])
                    st.session_state.results = []
                    st.rerun()

    with tabs[2]:
        st.subheader("State Preservation")
        st.warning("Export your database frequently. Cloud instances are ephemeral.")
        
        if not st.session_state.results:
            st.info("Nothing to export.")
        else:
            df_out = results_dataframe(st.session_state.results).drop(columns=["select"], errors="ignore")
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
