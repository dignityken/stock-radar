import streamlit as st
import pandas as pd
import requests
from io import StringIO
import re
import datetime
import urllib3
import unicodedata
import yfinance as yf
import json
import streamlit.components.v1 as components

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="stock-radar", layout="wide")
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 👤 Users 分頁：email 帳號系統
# ==========================================
@st.cache_resource(ttl=300)
def get_users_sheet():
    if not GSHEETS_AVAILABLE or "gcp_service_account" not in st.secrets:
        return None
    if "gsheets" not in st.secrets or "spreadsheet_url" not in st.secrets["gsheets"]:
        return None
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = gspread.authorize(creds)
        raw_url = st.secrets["gsheets"]["spreadsheet_url"]
        doc = client.open_by_url(raw_url.split("?")[0])
        try:
            ws = doc.worksheet("Users")
        except gspread.exceptions.WorksheetNotFound:
            ws = doc.add_worksheet(title="Users", rows="500", cols="6")
            ws.append_row(["email", "password_hash", "username", "status", "expire_date", "role"])
        return ws
    except Exception:
        return None

def verify_email_login(email, password):
    if not BCRYPT_AVAILABLE:
        return False, "", "伺服器未安裝 bcrypt 套件", ""
    ws = get_users_sheet()
    if not ws:
        return False, "", "無法連線用戶資料庫", ""
    try:
        records = ws.get_all_records()
        email_lower = email.strip().lower()
        for row in records:
            if str(row.get("email", "")).strip().lower() == email_lower:
                status = str(row.get("status", "")).strip().lower()
                if status == "pending":
                    return False, "", "⏳ 帳號審核中，請等待管理員開通。", ""
                if status != "active":
                    return False, "", "🚫 帳號已停用，請洽管理員。", ""
                exp_str = str(row.get("expire_date", "2099-12-31")).strip()
                try:
                    if datetime.date.today() > datetime.datetime.strptime(exp_str, "%Y-%m-%d").date():
                        return False, "", "⚠️ 帳號已到期，請洽管理員續期。", ""
                except: pass
                stored_hash = str(row.get("password_hash", "")).strip()
                if bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
                    username = str(row.get("username", email)).strip()
                    role = str(row.get("role", "member")).strip().lower()
                    return True, username, "OK", role
                else:
                    return False, "", "🚫 密碼錯誤", ""
        return False, "", "🚫 找不到此 email", ""
    except Exception as e:
        return False, "", f"驗證失敗: {str(e)}", ""

SIGNUP_FORM_URL = st.secrets.get("signup_form_url", "")

def check_password():
    if st.session_state.get("password_correct") == True:
        return True

    valid_passwords = st.secrets.get("passwords", {})
    query_params = st.query_params
    if "token" in query_params:
        url_token = query_params["token"]
        for user, auth_string in valid_passwords.items():
            if url_token == auth_string.split("|")[0].strip():
                exp_date_str = auth_string.split("|")[1].strip() if "|" in auth_string else "2099-12-31"
                try:
                    if datetime.date.today() <= datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date():
                        st.session_state["password_correct"] = True
                        st.session_state["username"] = user
                        st.session_state["user_token"] = url_token
                        st.session_state["login_method"] = "legacy"
                        st.session_state["role"] = "member"
                        if "token" in st.query_params:
                            st.query_params.clear()
                        return True
                except:
                    st.session_state["password_correct"] = True
                    st.session_state["username"] = user
                    st.session_state["user_token"] = url_token
                    st.session_state["login_method"] = "legacy"
                    st.session_state["role"] = "member"
                    if "token" in st.query_params:
                        st.query_params.clear()
                    return True

    if st.session_state.get("password_correct") == True:
        return True

    st.markdown("""
    <style>
    .login-title { text-align: center; font-size: 2rem; font-weight: bold; margin-bottom: 4px; }
    .login-sub { text-align: center; color: #888; margin-bottom: 28px; font-size: 0.95rem; }
    .login-divider { text-align: center; color: #555; margin: 16px 0; font-size: 0.85rem; }
    .login-signup { text-align: center; margin-top: 20px; font-size: 0.85rem; color: #888; }
    .login-signup a { color: #4e8cff; text-decoration: none; }
    </style>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown('<div class="login-title">📡 stock-radar</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-sub">分點追蹤平台</div>', unsafe_allow_html=True)
        login_status = st.session_state.get("login_status", "")

        with st.form("login_form"):
            email_input = st.text_input("Email", placeholder="your@email.com", autocomplete="username")
            pw_input = st.text_input("密碼", type="password", placeholder="輸入密碼", autocomplete="current-password")
            submit_btn = st.form_submit_button("🔐 登入", use_container_width=True, type="primary")
            if submit_btn:
                if email_input and pw_input:
                    success, username, msg, role = verify_email_login(email_input, pw_input)
                    if success:
                        st.session_state["password_correct"] = True
                        st.session_state["username"] = username
                        st.session_state["user_token"] = email_input
                        st.session_state["login_method"] = "email"
                        st.session_state["role"] = role
                        st.session_state["login_status"] = ""
                        st.rerun()
                    else:
                        st.session_state["login_status"] = msg
                        st.rerun()
                else:
                    st.session_state["login_status"] = "請輸入 Email 和密碼"
                    st.rerun()

        if login_status and "OK" not in login_status:
            st.error(login_status)
        st.markdown('<div class="login-divider">── 或使用舊版通行密碼 ──</div>', unsafe_allow_html=True)

        def legacy_password_entered():
            user_pwd_input = st.session_state["legacy_pwd_input"].strip()
            for user, auth_string in valid_passwords.items():
                parts = str(auth_string).split("|")
                pwd = parts[0].strip()
                exp_date_str = parts[1].strip() if len(parts) > 1 else "2099-12-31"
                if user_pwd_input == pwd:
                    try:
                        if datetime.date.today() > datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date():
                            st.session_state["login_status"] = "⚠️ 通行密碼已到期，請洽管理員。"
                            return
                    except: pass
                    st.session_state["password_correct"] = True
                    st.session_state["username"] = user
                    st.session_state["user_token"] = user_pwd_input
                    st.session_state["login_method"] = "legacy"
                    st.session_state["role"] = "member"
                    st.session_state["login_status"] = ""
                    del st.session_state["legacy_pwd_input"]
                    return
            st.session_state["login_status"] = "🚫 通行密碼錯誤"

        st.text_input("通行密碼", type="password", placeholder="輸入通行密碼", on_change=legacy_password_entered, key="legacy_pwd_input")
        if SIGNUP_FORM_URL:
            st.markdown(f'<div class="login-signup">還沒有帳號？ <a href="{SIGNUP_FORM_URL}" target="_blank">申請加入 →</a></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="login-signup">需要帳號請洽管理員</div>', unsafe_allow_html=True)
    return False

if not check_password(): st.stop()

# ==========================================
# Google Sheets 函數
# ==========================================
@st.cache_resource(ttl=3600)
def init_gsheets():
    if not GSHEETS_AVAILABLE:
        return None, "伺服器未安裝 gspread 套件"
    if "gcp_service_account" not in st.secrets:
        return None, "找不到 [gcp_service_account] 金鑰設定"
    if "gsheets" not in st.secrets or "spreadsheet_url" not in st.secrets["gsheets"]:
        return None, "找不到 [gsheets] 試算表網址設定"
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = gspread.authorize(creds)
        raw_url = st.secrets["gsheets"]["spreadsheet_url"]
        doc = client.open_by_url(raw_url.split("?")[0])
        try:
            ws = doc.worksheet("Watchlist")
        except gspread.exceptions.WorksheetNotFound:
            ws = doc.add_worksheet(title="Watchlist", rows="1000", cols="2")
            ws.update_acell('A1', 'Username')
            ws.update_acell('B1', 'WatchlistJSON')
        return ws, "OK"
    except Exception as e:
        return None, f"GSheets 連線失敗: {str(e)}"

def load_gsheet_watchlist(username):
    ws, msg = init_gsheets()
    if not ws: return []
    try:
        cell = ws.find(username, in_column=1)
        if cell:
            data = ws.cell(cell.row, 2).value
            if data: return json.loads(data)
    except: pass
    return []

def save_gsheet_watchlist(username, wl_list):
    ws, msg = init_gsheets()
    if not ws: return False, msg
    try:
        data_str = json.dumps(wl_list, ensure_ascii=False)
        try:
            cell = ws.find(username, in_column=1)
            if cell:
                ws.update_cell(cell.row, 2, data_str)
            else:
                ws.append_row([username, data_str])
        except Exception as e:
            return False, f"寫入更新錯誤: {str(e)}"
        return True, "成功寫入雲端"
    except Exception as e:
        return False, f"寫入錯誤: {str(e)}"

# ==========================================
# 📡 掃描結果清單（VIP 專屬）
# ==========================================
@st.cache_data(ttl=300)
def load_scan_result():
    """從 Google Sheets 的 ScanResult 分頁讀取掃描結果"""
    if not GSHEETS_AVAILABLE or "gcp_service_account" not in st.secrets:
        return pd.DataFrame()
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = gspread.authorize(creds)
        raw_url = st.secrets["gsheets"]["spreadsheet_url"]
        doc = client.open_by_url(raw_url.split("?")[0])
        try:
            scan_ws = doc.worksheet("ScanResult")
        except:
            return pd.DataFrame()
        data = scan_ws.get_all_records()
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame()

# ==========================================
# 頁面常數
# ==========================================
PAGE_T1 = "🚀 特定分點"
PAGE_T2 = "📊 股票代號"
PAGE_T3 = "📍 地緣券商"
PAGE_T4 = "📊 主力 K 線圖"
ALL_PAGES = [PAGE_T1, PAGE_T2, PAGE_T3, PAGE_T4]

if 'current_page' not in st.session_state:
    st.session_state.current_page = PAGE_T1

# ==========================================
# 側邊欄
# ==========================================
with st.sidebar:
    current_user = st.session_state.get('username', 'VIP會員')
    login_method = st.session_state.get('login_method', 'legacy')
    user_role    = st.session_state.get('role', 'member')

    st.markdown(f"### 👤 {current_user}")
    if login_method == "email":
        st.caption("✅ 已用 Email 帳號登入")
    else:
        st.caption("✅ 網址列已隱藏密碼，直接複製分享網址絕對安全。")
        with st.expander("🔗 取得免登入書籤網址"):
            st.write("若要在您的電腦/手機免密碼登入，請將下方參數**接在您的網站主網址後方**並加入書籤：")
            st.code(f"?token={st.session_state.get('user_token', '')}", language="text")
            st.caption("⚠️ 此參數等同您的密碼，請勿外流！")
    if not GSHEETS_AVAILABLE or "gcp_service_account" not in st.secrets:
        st.warning("⚠️ 系統未偵測到 Google Sheets 金鑰，清單將僅暫存於本次連線。")

    # ── VIP 專屬：掃描結果清單 ──
    if user_role == "vip":
        st.markdown("---")
        st.markdown("#### 📡 VIP 掃描清單")
        with st.expander("展開查看", expanded=False):
            scan_df = load_scan_result()
            if scan_df.empty:
                st.caption("尚無資料，請先上傳掃描結果至 ScanResult 分頁")
            else:
                # ── 從訊號摘要抓最新訊號日期 ──
                def get_latest_signal_date(summary):
                    dates = re.findall(r"[0-9]{4}/[0-9]{2}/[0-9]{2}", str(summary))
                    if not dates: return None
                    try:
                        return max(datetime.datetime.strptime(d, "%Y/%m/%d").date() for d in dates)
                    except: return None

                scan_df["最新訊號日"] = scan_df["訊號摘要"].apply(get_latest_signal_date)

                # ── 強度分數 ──
                def calc_score(row):
                    score = 0
                    score += min(float(row.get("佔比%", 0)), 100) * 0.4
                    has_amt = "金額(萬)" in row.index and row.get("金額(萬)", 0)
                    scale = float(row.get("金額(萬)", 0)) if has_amt else float(row.get("張數", 0)) / 10
                    score += min(scale / 500, 1) * 40
                    latest = row.get("最新訊號日")
                    if latest:
                        days_ago = (datetime.date.today() - latest).days
                        score += max(0, 20 - days_ago * 0.3)
                    summary = str(row.get("訊號摘要", ""))
                    if ("MS" in summary or "WS" in summary) and ("ML" in summary or "WL" in summary):
                        score += 10
                    return round(score, 1)

                scan_df["強度"] = scan_df.apply(calc_score, axis=1)

                # ── 篩選條件 ──
                recent_n = st.slider("最近幾天", 7, 180, 60, step=7, key="scan_recent_days")
                cutoff = datetime.date.today() - datetime.timedelta(days=recent_n)

                col_dir = st.selectbox("方向", ["全部", "買進", "賣出"], key="scan_dir_filter")
                min_score = st.slider("最低強度", 0, 120, 60, step=5, key="scan_min_score")

                broker_list = ["全部分點"] + sorted(scan_df["分點名稱"].dropna().unique().tolist()) if "分點名稱" in scan_df.columns else ["全部分點"]
                sel_broker = st.selectbox("分點過濾", broker_list, key="scan_broker_sel")

                # ── 套用篩選 ──
                scan_show = scan_df.copy()
                scan_show = scan_show[scan_show["最新訊號日"].apply(lambda d: d is not None and d >= cutoff)]
                if col_dir != "全部" and "方向" in scan_show.columns:
                    scan_show = scan_show[scan_show["方向"] == col_dir]
                if sel_broker != "全部分點" and "分點名稱" in scan_show.columns:
                    scan_show = scan_show[scan_show["分點名稱"] == sel_broker]
                scan_show = scan_show[scan_show["強度"] >= min_score]
                scan_show = scan_show.sort_values("強度", ascending=False)

                st.caption(f"近{recent_n}天　強度≥{min_score}　共 **{len(scan_show)}** 檔")

                if not scan_show.empty:
                    display_cols = [c for c in ["股票代號", "股票名稱", "分點名稱", "方向", "強度", "張數", "金額(萬)", "佔比%", "最新訊號日", "訊號摘要"] if c in scan_show.columns]
                    scan_show = scan_show[display_cols].reset_index(drop=True).copy()
                    scan_show.insert(0, "📊", False)

                    refresh_key = st.session_state.get("table_refresh_key", 0)
                    scan_editor_key = f"scan_editor_{refresh_key}"
                    st.data_editor(
                        scan_show,
                        hide_index=True,
                        column_config={"📊": st.column_config.CheckboxColumn("載入K線")},
                        use_container_width=True,
                        key=scan_editor_key
                    )
                    if scan_editor_key in st.session_state:
                        edits = st.session_state[scan_editor_key].get("edited_rows", {})
                        for row_idx, changes in edits.items():
                            if changes.get("📊", False):
                                row = scan_show.iloc[row_idx]
                                sid = str(row.get("股票代號", "")).strip()
                                br  = str(row.get("分點名稱", "")).strip()
                                if sid and br:
                                    st.session_state["vip_pending_sid"] = sid
                                    st.session_state["vip_pending_br"]  = br
                                    st.session_state["table_refresh_key"] = refresh_key + 1
                                    st.session_state.current_page = PAGE_T4
                                    st.rerun()
                else:
                    st.caption("無符合條件的資料，請調整篩選條件")

    st.markdown("---")
    st.markdown("#### 🗺️ 頁面導航")
    for page in ALL_PAGES:
        is_active = st.session_state.current_page == page
        if st.button(page, use_container_width=True,
                     type="primary" if is_active else "secondary",
                     key=f"sidebar_nav_{page}"):
            st.session_state.current_page = page
            st.rerun()

# ==========================================
# Session State 初始化
# ==========================================
for tab in ['t1', 't2', 't3']:
    if f'{tab}_searched' not in st.session_state: st.session_state[f'{tab}_searched'] = False
    if f'{tab}_buy_df' not in st.session_state: st.session_state[f'{tab}_buy_df'] = pd.DataFrame()
    if f'{tab}_sell_df' not in st.session_state: st.session_state[f'{tab}_sell_df'] = pd.DataFrame()

if 't4_target_sid' not in st.session_state: st.session_state.t4_target_sid = "6488"
if 't4_target_br' not in st.session_state: st.session_state.t4_target_br = "兆豐-忠孝"
if 'auto_draw' not in st.session_state: st.session_state.auto_draw = False
if 'custom_hlines' not in st.session_state: st.session_state.custom_hlines = []
if 'click_lines' not in st.session_state: st.session_state.click_lines = []
if 'table_refresh_key' not in st.session_state: st.session_state.table_refresh_key = 0
if 'chart_render_key' not in st.session_state: st.session_state.chart_render_key = 0
if 'show_chart' not in st.session_state: st.session_state.show_chart = False
if 'drawn_sid' not in st.session_state: st.session_state.drawn_sid = "6488"
if 'drawn_br_name' not in st.session_state: st.session_state.drawn_br_name = "兆豐-忠孝"
if 'drawn_period' not in st.session_state: st.session_state.drawn_period = "日"
if 'drawn_days' not in st.session_state: st.session_state.drawn_days = 300

if 'watchlist_loaded' not in st.session_state:
    st.session_state.watchlist = load_gsheet_watchlist(current_user)
    st.session_state.watchlist_loaded = True

if 't1_val_hq' not in st.session_state: st.session_state.t1_val_hq = None
if 't1_val_br' not in st.session_state: st.session_state.t1_val_br = None
if 't1_last_br' not in st.session_state: st.session_state.t1_last_br = None
if 't1_last_br_id' not in st.session_state: st.session_state.t1_last_br_id = None
if 't2_val_sid' not in st.session_state: st.session_state.t2_val_sid = "2408"
if 't2_last_sid' not in st.session_state: st.session_state.t2_last_sid = "2408"
if 't3_val_loc' not in st.session_state: st.session_state.t3_val_loc = None
if 't3_val_br' not in st.session_state: st.session_state.t3_val_br = None
if 't3_last_br' not in st.session_state: st.session_state.t3_last_br = None
if 't3_last_br_id' not in st.session_state: st.session_state.t3_last_br_id = None

default_start = datetime.date.today() - datetime.timedelta(days=7)
default_end = datetime.date.today()
if 't1_val_sd' not in st.session_state: st.session_state.t1_val_sd = default_start
if 't1_val_ed' not in st.session_state: st.session_state.t1_val_ed = default_end
if 't2_val_sd' not in st.session_state: st.session_state.t2_val_sd = default_start
if 't2_val_ed' not in st.session_state: st.session_state.t2_val_ed = default_end
if 't3_val_sd' not in st.session_state: st.session_state.t3_val_sd = default_start
if 't3_val_ed' not in st.session_state: st.session_state.t3_val_ed = default_end

# ==========================================
# 資料載入函數
# ==========================================
@st.cache_data(ttl=3600)
def download_google_drive_file(url):
    file_id = url.split('/')[-2]
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        response = requests.get(download_url, stream=True, verify=False, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception: return None

@st.cache_data(ttl=3600)
def load_hq_data(url):
    content = download_google_drive_file(url)
    if not content: return {}
    hq_data = {}
    for line in content.strip().split('\n'):
        if "\t" in line and not line.startswith("證券商代號"):
            parts = line.split('\t')
            if len(parts) == 2: hq_data[parts[0].strip()] = parts[1].strip()
    return hq_data

@st.cache_data(ttl=3600)
def load_branch_data(url):
    content = download_google_drive_file(url)
    if not content: return ""
    return content.strip().lstrip("'").rstrip("'")

GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drive_link"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drive_link"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

HQ_DATA = load_hq_data(GOOGLE_DRIVE_HQ_DATA_URL)
FINAL_RAW_DATA_CLEANED = load_branch_data(GOOGLE_DRIVE_BRANCH_DATA_URL)

def build_full_broker_db_structure(raw_data_string, hq_data_map):
    tree = {}; name_map = {}
    for group_str in raw_data_string.strip().split(';'):
        if not group_str: continue
        parts = group_str.split('!')
        if not parts: continue
        head_info = parts[0].split(',')
        if len(head_info) != 2: continue
        bid, bname = head_info[0].strip(), head_info[1].replace("亚","亞").strip()
        final_bname = hq_data_map.get(bid, bname)
        branches_processed = {}
        for p_str in parts[1:]:
            if ',' in p_str:
                br_id, br_name_raw = p_str.split(',', 1)
                br_name = br_name_raw.replace("亚","亞").strip()
                if br_name not in branches_processed:
                    branches_processed[br_name] = br_id.strip()
                    name_map[br_name] = {"hq_id": bid, "br_id": br_id.strip(), "hq_name": final_bname}
        if final_bname not in branches_processed and bid not in [br_id for br_id in branches_processed.values()]:
            branches_processed[final_bname] = bid
            name_map[final_bname] = {"hq_id": bid, "br_id": bid, "hq_name": final_bname}
        tree[final_bname] = {"bid": bid, "branches": branches_processed}
    final_tree = {}
    for hq_name, hq_data in tree.items():
        unique_branches = {}
        seen_names = set()
        for br_name, br_id in hq_data['branches'].items():
            if br_name not in seen_names:
                unique_branches[br_name] = br_id
                seen_names.add(br_name)
        final_tree[hq_name] = {"bid": hq_data['bid'], "branches": unique_branches}
    if '北城證券' in final_tree and '北城' in final_tree:
        if final_tree['北城證券']['bid'] == final_tree['北城']['bid']:
            del final_tree['北城']
            if '北城' in name_map: del name_map['北城']
    return final_tree, name_map

UI_TREE, BROKER_MAP = build_full_broker_db_structure(FINAL_RAW_DATA_CLEANED, HQ_DATA)

GEO_MAP = {}
for br_name, br_info in BROKER_MAP.items():
    if "-" in br_name:
        loc_name = br_name.split("-")[-1].replace("(停)", "").strip()
        if loc_name and loc_name not in GEO_MAP: GEO_MAP[loc_name] = {}
        if loc_name: GEO_MAP[loc_name][br_name] = br_info

def get_stock_id(name_str):
    s = str(name_str).strip()
    s = unicodedata.normalize('NFKC', s)
    s = s.replace(" ", "")
    match_with_letter = re.match(r'^(\d+[A-Za-z])(?![A-Za-z])', s)
    if match_with_letter: return match_with_letter.group(1).upper()
    match_digits_only = re.match(r'^(\d+)', s)
    if match_digits_only: return match_digits_only.group(1).upper()
    return None

def calculate_macd(df, fast, slow, signal):
    exp1 = df['Close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['Close'].ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist

@st.cache_data(ttl=3600)
def get_stock_kline(stock_id, start_date="2015-01-01"):
    end_date = datetime.date.today() + datetime.timedelta(days=1)
    for suffix in ['.TW', '.TWO']:
        ticker = f"{stock_id}{suffix}"
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
        if not df.empty:
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            df.reset_index(inplace=True)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            return df
    return pd.DataFrame()

@st.cache_data(ttl=1800)
def get_history_and_name(sid, br_id, start_date="2015-01-01"):
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    url_history = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?A={sid}&BHID={br_id}&b={br_id}&C=3&D={start_date}&E={today_str}&ver=V3"
    try:
        res_hist = requests.get(url_history, headers=HEADERS, verify=False, timeout=20)
        res_hist.encoding = 'big5'
        stock_name = ""
        m_name = re.search(r"對\s+([^\(]+)\(\s*" + re.escape(sid) + r"\s*\)個股", res_hist.text)
        if m_name: stock_name = m_name.group(1).strip()
        tables = pd.read_html(StringIO(res_hist.text))
        df_broker = pd.DataFrame(columns=['Date', '買進', '賣出', '總額', '買賣超'])
        for tb in tables:
            if tb.shape[1] == 5 and '日期' in str(tb.iloc[0].values):
                df_b = tb.copy()
                df_b.columns = ['Date', '買進', '賣出', '總額', '買賣超']
                df_b = df_b.drop(0)
                df_b = df_b[~df_b['Date'].str.contains('日期|合計|說明', na=False)].copy()
                df_b['Date'] = pd.to_datetime(df_b['Date'].astype(str).str.replace(' ', ''))
                df_b['買賣超'] = pd.to_numeric(df_b['買賣超'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                df_broker = df_b
                break
        return df_broker, stock_name
    except:
        return pd.DataFrame(columns=['Date', '買賣超']), ""

def safe_float(val):
    if pd.isna(val): return None
    return float(val)

# ==========================================
# 頁面標題
# ==========================================
cur_page = st.session_state.current_page

# ==========================================
# 🚀 頁面一：特定分點
# ==========================================
if cur_page == PAGE_T1:
    st.markdown("### 🚀 特定分點")
    c1, c2 = st.columns(2)
    with c1:
        sorted_hq_keys = sorted(UI_TREE.keys())
        idx_hq = sorted_hq_keys.index(st.session_state.t1_val_hq) if st.session_state.t1_val_hq in sorted_hq_keys else 0
        sel_hq = st.selectbox("選擇券商", sorted_hq_keys, index=idx_hq, key="t1_b_sel")
        st.session_state.t1_val_hq = sel_hq
    with c2:
        b_opts = UI_TREE[sel_hq]['branches']
        sorted_br_keys = sorted(b_opts.keys())
        idx_br = sorted_br_keys.index(st.session_state.t1_val_br) if st.session_state.t1_val_br in sorted_br_keys else 0
        sel_br_l = st.selectbox("選擇分點", sorted_br_keys, index=idx_br, key="t1_br_sel")
        st.session_state.t1_val_br = sel_br_l
        sel_br_id = b_opts[sel_br_l]

    c3, c4, c5 = st.columns(3)
    with c3:
        t1_sd = st.date_input("區間起點", value=st.session_state.t1_val_sd, key="t1_sd")
        st.session_state.t1_val_sd = t1_sd
    with c4:
        t1_ed = st.date_input("區間終點", value=st.session_state.t1_val_ed, key="t1_ed")
        st.session_state.t1_val_ed = t1_ed
    with c5: t1_u = st.radio("統計單位", ["張數", "金額"], horizontal=True, key="t1_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t1_mode = st.radio("篩選條件", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t1_mode")
    with c7: t1_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, step=1.0, key="t1_pct")
    with c8: st.write(""); show_full = st.checkbox("顯示完整清單", value=False, key="t1_full")

    if st.button("開始分點尋寶 🚀", key="t1_go"):
        st.session_state.t1_searched = True
        st.session_state.t1_last_br = sel_br_l
        st.session_state.t1_last_br_id = sel_br_id
        sd_s, ed_s = t1_sd.strftime('%Y-%m-%d'), t1_ed.strftime('%Y-%m-%d')
        bid_hq = UI_TREE[sel_hq]['bid']
        c_param = "B" if '金額' in t1_u else "E"
        col_buy = '買進金額' if '金額' in t1_u else '買進張數'
        col_sell = '賣出金額' if '金額' in t1_u else '賣出張數'
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={bid_hq}&b={sel_br_id}&c={c_param}&e={sd_s}&f={ed_s}"
        try:
            res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
            res.encoding = 'big5'
            def extract_stock_name_from_script(match):
                m = re.search(r"GenLink2stk\s*\(\s*['\"](?:AS)?([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", match.group(0), re.IGNORECASE)
                if m: return f"{m.group(1).strip()}{m.group(2).strip()}"
                return ""
            processed_html_text = re.sub(r"<script[^>]*>(?:(?!</script>).)*GenLink2stk\s*\([^)]+\).*?</script>", extract_stock_name_from_script, res.text, flags=re.IGNORECASE | re.DOTALL)
            tables = pd.read_html(StringIO(processed_html_text))
            df_all = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] < 3: continue
                if any(word in str(tb) for word in ['買進','賣出','張數','金額','股票名稱']):
                    if tb.shape[1] >= 8:
                        l = tb.iloc[:, [0,1,2]].copy(); l.columns=['股票名稱', col_buy, col_sell]
                        r = tb.iloc[:, [5,6,7]].copy(); r.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, l, r], ignore_index=True)
                    else:
                        temp = tb.iloc[:, [0,1,2]].copy(); temp.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, temp], ignore_index=True)
            if not df_all.empty:
                df_all['股票名稱'] = df_all['股票名稱'].astype(str).str.strip()
                df_all = df_all[~df_all['股票名稱'].str.contains('名稱|買進|賣出|合計|說明|註|差額|請選擇|nan|NaN|None|^\s*$', na=False)]
                df_all = df_all[df_all['股票名稱'].apply(lambda x: bool(get_stock_id(x)))].copy()
                for c in [col_buy, col_sell]: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                df_all['總額'] = df_all[col_buy] + df_all[col_sell]
                df_all = df_all[df_all['總額'] > 0].copy()
                df_all['買%'] = (df_all[col_buy] / df_all['總額'] * 100).round(1)
                df_all['賣%'] = (df_all[col_sell] / df_all['總額'] * 100).round(1)
                if '嚴格' in t1_mode:
                    st.session_state.t1_buy_df = df_all[(df_all[col_buy] > 0) & (df_all[col_sell] == 0)].copy()
                    st.session_state.t1_sell_df = df_all[(df_all[col_sell] > 0) & (df_all[col_buy] == 0)].copy()
                else:
                    st.session_state.t1_buy_df = df_all[df_all['買%'] >= t1_p].copy()
                    st.session_state.t1_sell_df = df_all[df_all['賣%'] >= t1_p].copy()
            else: st.warning("無資料。")
        except Exception as e: st.error(f"發生錯誤: {e}")

    if st.session_state.t1_searched:
        def display_table_with_button(df_to_show, key_prefix):
            if not df_to_show.empty:
                df_show = df_to_show.copy()
                df_show['extracted_stock_id'] = df_show['股票名稱'].apply(get_stock_id)
                df_show['K線圖'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else "")
                df_show['分點明細'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={st.session_state.t1_last_br_id}&b={st.session_state.t1_last_br_id}&C=3" if sid else "")
                df_show['📊 K線圖'] = False
                col_buy_name = '買進金額' if '金額' in t1_u else '買進張數'
                col_sell_name = '賣出金額' if '金額' in t1_u else '賣出張數'
                df_show = df_show[['📊 K線圖', '股票名稱', 'K線圖', col_buy_name, col_sell_name, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                col_config = {
                    "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈"),
                    "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "📊 K線圖": st.column_config.CheckboxColumn("📊 K線圖"),
                    "extracted_stock_id": None
                }
                editor_key = f"editor_{key_prefix}_{st.session_state.table_refresh_key}"
                st.data_editor(df_show, hide_index=True, column_config=col_config, width="stretch", key=editor_key)
                if editor_key in st.session_state:
                    edits = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, changes in edits.items():
                        if changes.get('📊 K線圖', False) == True:
                            sid_clicked = df_show.iloc[row_idx]['extracted_stock_id']
                            st.session_state.t4_target_sid = sid_clicked
                            st.session_state.t4_target_br = st.session_state.t1_last_br
                            st.session_state.auto_draw = True
                            st.session_state.table_refresh_key += 1
                            st.session_state.current_page = PAGE_T4
                            st.rerun()

        col_b = '買進金額' if '金額' in t1_u else '買進張數'
        col_s = '賣出金額' if '金額' in t1_u else '賣出張數'
        st.markdown(f"### 🔴 買進 - 共 {len(st.session_state.t1_buy_df)} 檔")
        display_table_with_button(st.session_state.t1_buy_df.sort_values(by=col_b, ascending=False).head(999 if show_full else 10), "t1_buy")
        st.markdown(f"### 🟢 賣出 - 共 {len(st.session_state.t1_sell_df)} 檔")
        display_table_with_button(st.session_state.t1_sell_df.sort_values(by=col_s, ascending=False).head(999 if show_full else 10), "t1_sell")

# ==========================================
# 📊 頁面二：股票代號
# ==========================================
elif cur_page == PAGE_T2:
    st.markdown("### 📊 股票代號 — 誰在買賣這檔股票？")
    c1, c2, c3 = st.columns(3)
    with c1:
        t2_sid = st.text_input("股票代號", value=st.session_state.t2_val_sid, key="t2_s")
        st.session_state.t2_val_sid = t2_sid
    with c2:
        t2_sd = st.date_input("開始", value=st.session_state.t2_val_sd, key="t2_sd_in")
        st.session_state.t2_val_sd = t2_sd
    with c3:
        t2_ed = st.date_input("結束", value=st.session_state.t2_val_ed, key="t2_ed_in")
        st.session_state.t2_val_ed = t2_ed

    c4, c5, c6, c7 = st.columns([2, 1, 1, 1.2])
    with c4: t2_m = st.radio("模式", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t2_mode")
    with c5: t2_p = st.number_input("門檻佔比%", 0, 100, 95, step=1, key="t2_p_in")
    with c6: t2_v = st.number_input("最低張數", 0, 1000000, 10, step=1, key="t2_v_in")
    with c7: st.write(""); show_full_t2 = st.checkbox("顯示完整清單", value=False, key="t2_full")

    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
        t2_sid_clean = t2_sid.strip().replace(" ", "").upper()
        st.session_state.t2_last_sid = t2_sid_clean
        if not t2_sid_clean.isalnum():
            st.error("股票代號必須是字母數字組合。")
        else:
            url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={t2_sid_clean}&e={t2_sd.strftime('%Y-%m-%d')}&f={t2_ed.strftime('%Y-%m-%d')}"
            try:
                res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
                res.encoding = 'big5'
                tables = pd.read_html(StringIO(res.text))
                df_all = pd.DataFrame()
                for tb in tables:
                    if tb.shape[1] == 10:
                        l = tb.iloc[:,[0,1,2]].copy(); l.columns=['券商','買','賣']
                        r = tb.iloc[:,[5,6,7]].copy(); r.columns=['券商','買','賣']
                        df_all = pd.concat([df_all, l, r], ignore_index=True)
                if not df_all.empty:
                    df_all = df_all.dropna()
                    df_all = df_all[~df_all['券商'].str.contains('券商|合計|平均|說明|註', na=False)]
                    for c in ['買','賣']: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                    df_all['合計'] = df_all['買'] + df_all['賣']
                    df_all['買進%'] = (df_all['買']/df_all['合計']*100).round(1)
                    df_all['賣出%'] = (df_all['賣']/df_all['合計']*100).round(1)
                    if t2_m == "嚴格模式":
                        st.session_state.t2_buy_df = df_all[(df_all['買'] >= t2_v) & (df_all['賣'] == 0)].copy()
                        st.session_state.t2_sell_df = df_all[(df_all['賣'] >= t2_v) & (df_all['買'] == 0)].copy()
                    else:
                        st.session_state.t2_buy_df = df_all[(df_all['買進%'] >= t2_p) & (df_all['買'] >= t2_v)].copy()
                        st.session_state.t2_sell_df = df_all[(df_all['賣出%'] >= t2_p) & (df_all['賣'] >= t2_v)].copy()
                else: st.warning("未找到資料。請檢查股票代號或日期區間。")
            except Exception as e: st.error(f"發生錯誤: {e}")

    if st.session_state.get('t2_buy_df') is not None and not st.session_state.t2_buy_df.empty:
        t2_sid_clean = st.session_state.t2_last_sid

        def get_link_t2(broker_name):
            name_cleaned = broker_name.replace("亞","亞").strip()
            info = BROKER_MAP.get(name_cleaned)
            if info: return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid_clean}&BHID={info['br_id']}&b={info['br_id']}&C=3"
            for k, v in BROKER_MAP.items():
                if name_cleaned in k or k in name_cleaned:
                    return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid_clean}&BHID={v['br_id']}&b={v['br_id']}&C=3"
            return ""

        def display_table_with_button_t2(df_to_show, key_prefix):
            if not df_to_show.empty:
                df_show = df_to_show.copy()
                df_show['網頁明細'] = df_show['券商'].apply(get_link_t2)
                df_show['📊 K線圖'] = False
                df_show = df_show[['📊 K線圖', '券商', '買', '賣', '合計', '買進%', '賣出%', '網頁明細']]
                col_config = {
                    "網頁明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "📊 K線圖": st.column_config.CheckboxColumn("📊 K線圖")
                }
                editor_key = f"editor_{key_prefix}_{st.session_state.table_refresh_key}"
                st.data_editor(df_show, hide_index=True, column_config=col_config, width="stretch", key=editor_key)
                if editor_key in st.session_state:
                    edits = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, changes in edits.items():
                        if changes.get('📊 K線圖', False) == True:
                            br_clicked = df_show.iloc[row_idx]['券商']
                            st.session_state.t4_target_sid = st.session_state.t2_last_sid
                            clean_br = br_clicked.replace("亚","亞").strip()
                            matched_br = clean_br if clean_br in BROKER_MAP else next((k for k in BROKER_MAP if clean_br in k or k in clean_br), None)
                            if matched_br: st.session_state.t4_target_br = matched_br
                            st.session_state.auto_draw = True
                            st.session_state.table_refresh_key += 1
                            st.session_state.current_page = PAGE_T4
                            st.rerun()

        st.subheader("🔴 買進分點")
        display_table_with_button_t2(st.session_state.t2_buy_df.sort_values('買', ascending=False).head(999 if show_full_t2 else 10), "t2_buy")
        st.subheader("🟢 賣出分點")
        display_table_with_button_t2(st.session_state.t2_sell_df.sort_values('賣', ascending=False).head(999 if show_full_t2 else 10), "t2_sell")

# ==========================================
# 📍 頁面三：地緣券商
# ==========================================
elif cur_page == PAGE_T3:
    st.markdown("### 📍 地緣券商")
    st.caption("透過分點名稱後綴（例如：城中、三重、信義）跨券商尋找特定地區的買賣。")
    c1, c2 = st.columns(2)
    with c1:
        sorted_loc_keys = sorted(GEO_MAP.keys())
        if st.session_state.t3_val_loc is None and '城中' in sorted_loc_keys:
            st.session_state.t3_val_loc = '城中'
        idx_loc = sorted_loc_keys.index(st.session_state.t3_val_loc) if st.session_state.t3_val_loc in sorted_loc_keys else 0
        sel_loc = st.selectbox("選擇地緣關鍵字", sorted_loc_keys, index=idx_loc, key="t3_loc_sel")
        st.session_state.t3_val_loc = sel_loc
    with c2:
        loc_branches = GEO_MAP[sel_loc]
        sorted_loc_br_keys = sorted(loc_branches.keys())
        idx_loc_br = sorted_loc_br_keys.index(st.session_state.t3_val_br) if st.session_state.t3_val_br in sorted_loc_br_keys else 0
        sel_t3_br_l = st.selectbox("選擇該區特定分點", sorted_loc_br_keys, index=idx_loc_br, key=f"t3_br_sel_{sel_loc}")
        st.session_state.t3_val_br = sel_t3_br_l
        sel_t3_br_info = loc_branches[sel_t3_br_l]
        sel_t3_hq_id = sel_t3_br_info['hq_id']
        sel_t3_br_id = sel_t3_br_info['br_id']

    c3, c4, c5 = st.columns(3)
    with c3:
        t3_sd = st.date_input("區間起點", value=st.session_state.t3_val_sd, key="t3_sd")
        st.session_state.t3_val_sd = t3_sd
    with c4:
        t3_ed = st.date_input("區間終點", value=st.session_state.t3_val_ed, key="t3_ed")
        st.session_state.t3_val_ed = t3_ed
    with c5: t3_u = st.radio("統計單位", ["張數", "金額"], horizontal=True, key="t3_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t3_mode = st.radio("篩選條件", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t3_mode")
    with c7: t3_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, step=1.0, key="t3_pct")
    with c8: st.write(""); show_full_t3 = st.checkbox("顯示完整清單", value=False, key="t3_full")

    if st.button("啟動地緣雷達 📡", key="t3_go"):
        st.session_state.t3_last_br = sel_t3_br_l
        st.session_state.t3_last_br_id = sel_t3_br_id
        sd_s, ed_s = t3_sd.strftime('%Y-%m-%d'), t3_ed.strftime('%Y-%m-%d')
        is_amount = '金額' in t3_u
        c_param = "B" if is_amount else "E"
        col_buy = '買進金額' if is_amount else '買進張數'
        col_sell = '賣出金額' if is_amount else '賣出張數'
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={sel_t3_hq_id}&b={sel_t3_br_id}&c={c_param}&e={sd_s}&f={ed_s}"
        try:
            res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
            res.encoding = 'big5'
            html_text = res.text
            def extract_stock_name_from_script(match):
                script_content = match.group(0)
                m = re.search(r"GenLink2stk\s*\(\s*['\"](?:AS)?([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", script_content, re.IGNORECASE)
                if m: return f"{m.group(1).strip()}{m.group(2).strip()}"
                return ""
            processed_html_text = re.sub(r"<script[^>]*>(?:(?!</script>).)*GenLink2stk\s*\([^)]+\).*?</script>", extract_stock_name_from_script, html_text, flags=re.IGNORECASE | re.DOTALL)
            tables = pd.read_html(StringIO(processed_html_text))
            df_all = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] < 3: continue
                if any(word in str(tb) for word in ['買進','賣出','張數','金額','股票名稱']):
                    if tb.shape[1] >= 8:
                        l = tb.iloc[:, [0,1,2]].copy(); l.columns=['股票名稱', col_buy, col_sell]
                        r = tb.iloc[:, [5,6,7]].copy(); r.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, l, r], ignore_index=True)
                    else:
                        temp = tb.iloc[:, [0,1,2]].copy(); temp.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, temp], ignore_index=True)
            if not df_all.empty:
                df_all['股票名稱'] = df_all['股票名稱'].astype(str).str.strip()
                invalid_patterns = ['名稱','買進','賣出','合計','說明','註','差額','請選擇','nan','NaN','None',r'^\s*$']
                df_all = df_all[~df_all['股票名稱'].str.contains('|'.join(invalid_patterns), na=False)]
                df_all = df_all[df_all['股票名稱'].apply(lambda x: bool(get_stock_id(x)))].copy()
                for c in [col_buy, col_sell]:
                    df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                df_all['總額'] = df_all[col_buy] + df_all[col_sell]
                df_all = df_all[df_all['總額'] > 0].copy()
                df_all['買%'] = (df_all[col_buy] / df_all['總額'] * 100).round(1)
                df_all['賣%'] = (df_all[col_sell] / df_all['總額'] * 100).round(1)
                if '嚴格' in t3_mode:
                    st.session_state.t3_buy_df = df_all[(df_all[col_buy] > 0) & (df_all[col_sell] == 0)].copy()
                    st.session_state.t3_sell_df = df_all[(df_all[col_sell] > 0) & (df_all[col_buy] == 0)].copy()
                else:
                    st.session_state.t3_buy_df = df_all[df_all['買%'] >= t3_p].copy()
                    st.session_state.t3_sell_df = df_all[df_all['賣%'] >= t3_p].copy()
            else: st.warning("抓取不到數據。")
        except Exception as e: st.error(f"發生錯誤: {e}")

    if st.session_state.get('t3_buy_df') is not None and not st.session_state.t3_buy_df.empty:
        is_amount = '金額' in t3_u
        col_buy = '買進金額' if is_amount else '買進張數'
        col_sell = '賣出金額' if is_amount else '賣出張數'

        def display_table_with_button_t3(df_to_show, key_prefix):
            if not df_to_show.empty:
                df_show = df_to_show.copy()
                df_show['extracted_stock_id'] = df_show['股票名稱'].apply(get_stock_id)
                df_show['K線圖'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else "")
                df_show['分點明細'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={st.session_state.t3_last_br_id}&b={st.session_state.t3_last_br_id}&C=3" if sid else "")
                df_show['📊 K線圖'] = False
                df_show = df_show[['📊 K線圖', '股票名稱', 'K線圖', col_buy, col_sell, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                col_config = {
                    "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈"),
                    "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "📊 K線圖": st.column_config.CheckboxColumn("📊 K線圖"),
                    "extracted_stock_id": None
                }
                editor_key = f"editor_{key_prefix}_{st.session_state.table_refresh_key}"
                st.data_editor(df_show, hide_index=True, column_config=col_config, width="stretch", key=editor_key)
                if editor_key in st.session_state:
                    edits = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, changes in edits.items():
                        if changes.get('📊 K線圖', False) == True:
                            sid_clicked = df_show.iloc[row_idx]['extracted_stock_id']
                            st.session_state.t4_target_sid = sid_clicked
                            st.session_state.t4_target_br = st.session_state.t3_last_br
                            st.session_state.auto_draw = True
                            st.session_state.table_refresh_key += 1
                            st.session_state.current_page = PAGE_T4
                            st.rerun()

        sd_s, ed_s = t3_sd.strftime('%Y-%m-%d'), t3_ed.strftime('%Y-%m-%d')
        st.subheader(f"🕵️ 地緣雷達結果：{st.session_state.t3_last_br}")
        st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t3_u}")
        st.markdown(f"### 🔴 該分點買進 - 共 {len(st.session_state.t3_buy_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_buy_df.sort_values(by=col_buy, ascending=False).head(999 if show_full_t3 else 10), "t3_buy")
        st.markdown(f"### 🟢 該分點賣出 - 共 {len(st.session_state.t3_sell_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_sell_df.sort_values(by=col_sell, ascending=False).head(999 if show_full_t3 else 10), "t3_sell")

# ==========================================
# 📊 頁面四：主力 K 線圖
# ==========================================
elif cur_page == PAGE_T4:

    # 處理來自 VIP 掃描清單的跳轉（需在 BROKER_MAP 建立後執行）
    if st.session_state.get("vip_pending_sid"):
        sid = st.session_state.pop("vip_pending_sid")
        br  = st.session_state.pop("vip_pending_br", "")
        clean_br = br.replace("亚", "亞").strip()
        matched_br = clean_br if clean_br in BROKER_MAP else next(
            (k for k in BROKER_MAP if clean_br in k or k in clean_br), None)
        st.session_state.t4_target_sid = sid
        if matched_br:
            st.session_state.t4_target_br = matched_br
        st.session_state.auto_draw = True
        st.rerun()

    def get_pine_divergence_markers(df_res, macd_col, hist_col, prefix):
        markers_price = []; markers_macd = []
        cur_top_dif=0.0; cur_top_date=None; cur_top_close=0.0
        prev_top_dif=0.0; prev_top_date=None; prev_top_close=0.0; cur_wave_high=0.0
        cur_bot_dif=0.0; cur_bot_date=None; cur_bot_close=0.0
        prev_bot_dif=0.0; prev_bot_date=None; prev_bot_close=0.0; cur_wave_low=1e9
        in_red=False; in_grn=False; top_zero_broken=False; bot_zero_broken=False

        for i in range(1, len(df_res)):
            hist=df_res[hist_col].iloc[i]; hist_prev=df_res[hist_col].iloc[i-1]
            dif=df_res[macd_col].iloc[i]; high=df_res['High'].iloc[i]; low=df_res['Low'].iloc[i]; date=df_res['Date_str'].iloc[i]
            cross_up=(hist_prev<=0 and hist>0); cross_down=(hist_prev>=0 and hist<0)

            if cross_up:
                cur_top_dif=0.0; cur_top_date=None; cur_top_close=0.0; cur_wave_high=0.0; in_red=True; bot_zero_broken=False
            if not in_red and dif<0 and prev_top_dif>0: top_zero_broken=True
            if top_zero_broken and cross_up:
                prev_top_dif=0.0; prev_top_date=None; prev_top_close=0.0; top_zero_broken=False
            if cross_down:
                in_red=False
                if prev_top_date is not None and prev_top_dif>0:
                    markers_macd.append({"time":prev_top_date,"position":"aboveBar","color":"#FFD600","shape":"text","text":f"{prev_top_dif:.2f}","size":1})
                if cur_top_dif>0 and prev_top_dif>0 and cur_top_dif<prev_top_dif and cur_wave_high>=prev_top_close and prev_top_date is not None:
                    lbl="价同" if cur_wave_high==prev_top_close else "價破"
                    markers_price.append({"time":prev_top_date,"position":"aboveBar","color":"#ef5350","shape":"arrowDown","text":f"M{prefix}\n{lbl}\n{prev_top_close:.2f}","price":prev_top_close})
                if cur_top_dif>0:
                    prev_top_dif=cur_top_dif; prev_top_date=cur_top_date; prev_top_close=cur_top_close
                cur_top_dif=0.0; cur_top_date=None; cur_top_close=0.0; cur_wave_high=0.0
            if hist>0:
                if dif>cur_top_dif: cur_top_dif=dif; cur_top_date=date; cur_top_close=high
                if high>cur_wave_high: cur_wave_high=high
            if cross_down:
                cur_bot_dif=0.0; cur_bot_date=None; cur_bot_close=0.0; cur_wave_low=1e9; in_grn=True; top_zero_broken=False
            if not in_grn and dif>0 and prev_bot_dif<0: bot_zero_broken=True
            if bot_zero_broken and cross_down:
                prev_bot_dif=0.0; prev_bot_date=None; prev_bot_close=0.0; bot_zero_broken=False
            if cross_up:
                in_grn=False
                if prev_bot_date is not None and prev_bot_dif<0:
                    markers_macd.append({"time":prev_bot_date,"position":"belowBar","color":"#00E676","shape":"text","text":f"{prev_bot_dif:.2f}","size":1})
                if cur_bot_dif<0 and prev_bot_dif<0 and cur_bot_dif>prev_bot_dif and cur_wave_low<=prev_bot_close and prev_bot_date is not None:
                    lbl="价同" if cur_wave_low==prev_bot_close else "價破"
                    markers_price.append({"time":prev_bot_date,"position":"belowBar","color":"#26a69a","shape":"arrowUp","text":f"W{prefix}\n{lbl}\n{prev_bot_close:.2f}","price":prev_bot_close})
                if cur_bot_dif<0:
                    prev_bot_dif=cur_bot_dif; prev_bot_date=cur_bot_date; prev_bot_close=cur_bot_close
                cur_bot_dif=0.0; cur_bot_date=None; cur_bot_close=0.0; cur_wave_low=1e9
            if hist<0:
                if dif<cur_bot_dif: cur_bot_dif=dif; cur_bot_date=date; cur_bot_close=low
                if low<cur_wave_low: cur_wave_low=low
            if i==len(df_res)-1:
                if hist>0 and cur_top_dif>0 and prev_top_dif>0:
                    if prev_top_date is not None:
                        markers_macd.append({"time":prev_top_date,"position":"aboveBar","color":"#FFD600","shape":"text","text":f"{prev_top_dif:.2f}","size":1})
                    if cur_top_dif<prev_top_dif and cur_wave_high>=prev_top_close and prev_top_date is not None:
                        lbl="价同" if cur_wave_high==prev_top_close else "價破"
                        markers_price.append({"time":prev_top_date,"position":"aboveBar","color":"#ef5350","shape":"arrowDown","text":f"未M{prefix}\n{lbl}\n{prev_top_close:.2f}","price":prev_top_close})
                if hist<0 and cur_bot_dif<0 and prev_bot_dif<0:
                    if prev_bot_date is not None:
                        markers_macd.append({"time":prev_bot_date,"position":"belowBar","color":"#00E676","shape":"text","text":f"{prev_bot_dif:.2f}","size":1})
                    if cur_bot_dif>prev_bot_dif and cur_wave_low<=prev_bot_close and prev_bot_date is not None:
                        lbl="价同" if cur_wave_low==prev_bot_close else "價破"
                        markers_price.append({"time":prev_bot_date,"position":"belowBar","color":"#26a69a","shape":"arrowUp","text":f"未W{prefix}\n{lbl}\n{prev_bot_close:.2f}","price":prev_bot_close})

        def dedup(lst):
            seen,out=set(),[]
            for m in reversed(lst):
                k=f"{m['time']}_{m['text']}"
                if k not in seen: seen.add(k); out.append(m)
            out.reverse(); return out
        return dedup(markers_price), dedup(markers_macd)

    def merge_kline_markers(markers):
        merged={}
        for m in markers:
            t=m['time']
            if t not in merged: merged[t]=m.copy()
            else: merged[t]['text']+=f"\n---\n{m['text']}"
        return sorted(list(merged.values()), key=lambda x: x['time'])

    st.markdown("""
    <style>
    [data-testid="stHorizontalBlock"]:has(input, select, button) { max-width: 700px !important; }
    </style>
    """, unsafe_allow_html=True)

    if st.session_state.auto_draw:
        st.info("✅ 參數已帶入，請點下方 🎨 繪圖查看圖表。")

    c_sid, c_br, c_draw, c_fav = st.columns([1, 3, 1, 1])
    with c_sid:
        t4_sid = st.text_input("股票代號", value=st.session_state.get('t4_target_sid', '6488'))
    with c_br:
        all_br_names = sorted(list(BROKER_MAP.keys()))
        t4_br_val = st.session_state.get('t4_target_br', '兆豐-忠孝')
        idx = all_br_names.index(t4_br_val) if t4_br_val in all_br_names else 0
        t4_br_name = st.selectbox("搜尋分點", all_br_names, index=idx)
    with c_draw:
        st.write("")
        draw_btn = st.button("🎨 繪圖", use_container_width=True, key="draw_btn_top")
    with c_fav:
        st.write("")
        fav_btn = st.button("❤️ 存入", use_container_width=True)

    t4_sid_clean = t4_sid.strip().upper()

    if fav_btn:
        t4_br_id = BROKER_MAP[t4_br_name]['br_id']
        _, s_name = get_history_and_name(t4_sid_clean, t4_br_id)
        if not s_name: s_name = ""
        exists = False
        for item in st.session_state.watchlist:
            if item.get("股票代號") == t4_sid_clean and item.get("追蹤分點") == t4_br_name:
                exists = True
                updated = False
                if not item.get("股票名稱"):
                    item["股票名稱"] = s_name; updated = True
                if "筆記" not in item:
                    item["筆記"] = ""; updated = True
                if updated:
                    save_gsheet_watchlist(current_user, st.session_state.watchlist)
                break
        if not exists:
            entry = {"股票代號": t4_sid_clean, "股票名稱": s_name, "追蹤分點": t4_br_name, "筆記": ""}
            st.session_state.watchlist.append(entry)
            success, msg = save_gsheet_watchlist(current_user, st.session_state.watchlist)
            if success: st.success(f"✅ 已存入【{current_user}】專屬雲端清單！")
            else: st.warning(f"⚠️ 雲端同步失敗！(錯誤原因: {msg})")
        else:
            st.warning("⚠️ 此組合已在清單中。")

    with st.expander("⚙️ 進階指標參數設定", expanded=False):
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.markdown("**布林通道**")
            c_bb1, c_bb2 = st.columns(2)
            with c_bb1: bb_w = st.number_input("週期", value=52)
            with c_bb2: bb_std = st.number_input("標準差", value=2.0, step=0.1)
        with sc2:
            st.markdown("**短線 MACD**")
            c_m11, c_m12, c_m13 = st.columns(3)
            with c_m11: macd1_f = st.number_input("快", value=12, key="m1f")
            with c_m12: macd1_s = st.number_input("慢", value=26, key="m1s")
            with c_m13: macd1_sig = st.number_input("訊號", value=9, key="m1sig")
        with sc3:
            st.markdown("**長線 MACD**")
            c_m21, c_m22, c_m23 = st.columns(3)
            with c_m21: macd2_f = st.number_input("快", value=26, key="m2f")
            with c_m22: macd2_s = st.number_input("慢", value=52, key="m2s")
            with c_m23: macd2_sig = st.number_input("訊號", value=18, key="m2sig")

    if st.session_state.watchlist:
        with st.expander(f"⭐ 【{current_user}】的專屬主力清單", expanded=False):
            if 'wl_refresh_key' not in st.session_state: st.session_state.wl_refresh_key = 0
            for item in st.session_state.watchlist:
                if "股票名稱" not in item: item["股票名稱"] = ""
                if "筆記" not in item: item["筆記"] = ""
            wl_df = pd.DataFrame(st.session_state.watchlist)
            wl_df.insert(0, '載入', False)
            wl_df['刪除'] = False
            cols = ['載入', '股票代號', '股票名稱', '追蹤分點', '筆記', '刪除']
            wl_df = wl_df[[c for c in cols if c in wl_df.columns]]
            wl_config = {
                "載入": st.column_config.CheckboxColumn("載入繪圖"),
                "刪除": st.column_config.CheckboxColumn("刪除"),
                "股票代號": st.column_config.TextColumn(disabled=True),
                "股票名稱": st.column_config.TextColumn(disabled=True),
                "追蹤分點": st.column_config.TextColumn(disabled=True),
                "筆記": st.column_config.TextColumn("📝 筆記 (點擊編輯)")
            }
            editor_key = f"wl_editor_{st.session_state.wl_refresh_key}"
            st.data_editor(wl_df, hide_index=True, column_config=wl_config, width="stretch", key=editor_key)
            if editor_key in st.session_state:
                edits = st.session_state[editor_key].get('edited_rows', {})
                action_taken = False; note_edited = False
                for row_idx, changes in edits.items():
                    if '筆記' in changes:
                        st.session_state.watchlist[row_idx]['筆記'] = changes['筆記']
                        note_edited = True
                if note_edited:
                    success, msg = save_gsheet_watchlist(current_user, st.session_state.watchlist)
                    if not success: st.error(f"雲端筆記儲存失敗: {msg}")
                for row_idx, changes in edits.items():
                    if changes.get('載入', False) == True:
                        st.session_state.t4_target_sid = wl_df.iloc[row_idx]['股票代號']
                        st.session_state.t4_target_br = wl_df.iloc[row_idx]['追蹤分點']
                        st.session_state.auto_draw = True
                        st.session_state.chart_render_key += 1
                        action_taken = True; break
                    if changes.get('刪除', False) == True:
                        del_sid = wl_df.iloc[row_idx]['股票代號']
                        del_br = wl_df.iloc[row_idx]['追蹤分點']
                        st.session_state.watchlist = [item for item in st.session_state.watchlist if not (item['股票代號'] == del_sid and item['追蹤分點'] == del_br)]
                        success, msg = save_gsheet_watchlist(current_user, st.session_state.watchlist)
                        if not success: st.error(f"雲端刪除同步失敗: {msg}")
                        action_taken = True; break
                if action_taken:
                    st.session_state.wl_refresh_key += 1; st.rerun()

    st.markdown("---")

    c_back1, c_back2, c_back3, c_spacer = st.columns([1, 1, 1, 6])
    with c_back1:
        if st.button("🚀 特定分點", use_container_width=True):
            st.session_state.current_page = PAGE_T1; st.rerun()
    with c_back2:
        if st.button("📊 股票代號", use_container_width=True):
            st.session_state.current_page = PAGE_T2; st.rerun()
    with c_back3:
        if st.button("📍 地緣券商", use_container_width=True):
            st.session_state.current_page = PAGE_T3; st.rerun()

    if 't4_start_year' not in st.session_state: st.session_state.t4_start_year = "2015-01-01"
    if 'drawn_start_year' not in st.session_state: st.session_state.drawn_start_year = "2015-01-01"

    c_per, c_days, c_start, c_hval, c_hadd, c_click, c_hclr, c_draw2 = st.columns([1.5, 1.5, 1.5, 1.5, 1, 1, 1, 1])
    with c_per:
        t4_period = st.radio("週期", ["日", "週", "月"], horizontal=True, key="t4_period_bot")
    with c_days:
        days_mode = st.selectbox("顯示K棒數", [300, 500, 1000, "自訂..."])
        if days_mode == "自訂...":
            t4_days = st.number_input("自訂K棒", value=st.session_state.drawn_days, min_value=10, max_value=5000, label_visibility="collapsed")
        else:
            t4_days = days_mode
    with c_start:
        start_opt = st.selectbox("下載範圍", ["2015起 (快)", "2000起 (慢)"], index=0 if st.session_state.t4_start_year=="2015-01-01" else 1)
        t4_start_val = "2015-01-01" if "2015" in start_opt else "2000-01-01"
        st.session_state.t4_start_year = t4_start_val
    with c_hval:
        hline_val = st.number_input("📏 水平線", value=0.0, step=1.0, key="hline_val_input")
    with c_hadd:
        st.write("")
        if st.button("➕", use_container_width=True, help="加入水平線"):
            if hline_val > 0 and hline_val not in st.session_state.custom_hlines:
                st.session_state.custom_hlines.append(hline_val)
            st.session_state.t4_target_sid = t4_sid
            st.session_state.t4_target_br = t4_br_name
            st.session_state.auto_draw = True
            st.session_state.chart_render_key += 1
            st.rerun()
    with c_click:
        st.write("")
        if 'enable_click_line' not in st.session_state: st.session_state.enable_click_line = False
        enable_click_line = st.checkbox("👆", key="enable_click_line", help="啟用點擊K棒畫線")
    with c_hclr:
        st.write("")
        if st.button("🗑️", use_container_width=True, help="清除所有畫線"):
            st.session_state.custom_hlines = []
            st.session_state.click_lines = []
            st.session_state.t4_target_sid = t4_sid
            st.session_state.t4_target_br = t4_br_name
            st.session_state.auto_draw = True
            st.session_state.chart_render_key += 1
            st.rerun()
    with c_draw2:
        st.write("")
        draw_btn2 = st.button("🎨", use_container_width=True, help="繪圖")

    draw_btn = draw_btn or draw_btn2

    if st.session_state.get('show_chart', False):
        if t4_period != st.session_state.get('drawn_period', '日') or t4_start_val != st.session_state.get('drawn_start_year', '2015-01-01'):
            st.session_state.drawn_period = t4_period
            st.session_state.drawn_start_year = t4_start_val
            st.session_state.chart_render_key += 1
            st.rerun()

    if draw_btn or st.session_state.auto_draw:
        st.session_state.auto_draw = False
        st.session_state.show_chart = True
        st.session_state.chart_render_key += 1
        st.session_state.t4_target_sid = t4_sid
        st.session_state.t4_target_br = t4_br_name
        st.session_state.drawn_sid = t4_sid
        st.session_state.drawn_br_name = t4_br_name
        st.session_state.drawn_period = t4_period
        st.session_state.drawn_days = t4_days
        st.session_state.drawn_start_year = t4_start_val

    if st.session_state.get('show_chart', False):
        drawn_sid_clean = st.session_state.drawn_sid.strip().upper()
        drawn_br_name = st.session_state.drawn_br_name
        drawn_br_id = BROKER_MAP[drawn_br_name]['br_id']
        drawn_period = st.session_state.drawn_period
        drawn_days = st.session_state.drawn_days
        drawn_start_year = st.session_state.drawn_start_year

        with st.spinner(f"為您繪製 {drawn_sid_clean} 中..."):
            try:
                df_k = get_stock_kline(drawn_sid_clean, drawn_start_year)
                if df_k.empty:
                    st.error("找不到 K 線資料。")
                else:
                    df_broker, stock_name = get_history_and_name(drawn_sid_clean, drawn_br_id, drawn_start_year)
                    if df_broker.empty: st.info("近期無交易紀錄。")
                    df_merged = pd.merge(df_k, df_broker[['Date', '買賣超']], on='Date', how='left')
                    df_merged['買賣超'] = df_merged['買賣超'].fillna(0)
                    df_merged.set_index('Date', inplace=True)
                    if drawn_period == "週": df_resampled = df_merged.resample('W-FRI').agg({'Open':'first','High':'max','Low':'min','Close':'last','買賣超':'sum'})
                    elif drawn_period == "月": df_resampled = df_merged.resample('ME').agg({'Open':'first','High':'max','Low':'min','Close':'last','買賣超':'sum'})
                    else: df_resampled = df_merged.copy()
                    df_resampled = df_resampled.dropna(subset=['Close']).reset_index()
                    df_resampled['Date_str'] = df_resampled['Date'].dt.strftime('%Y-%m-%d')
                    df_resampled['BB_mid'] = df_resampled['Close'].rolling(int(bb_w)).mean()
                    df_resampled['BB_std'] = df_resampled['Close'].rolling(int(bb_w)).std()
                    df_resampled['BB_up'] = df_resampled['BB_mid'] + float(bb_std) * df_resampled['BB_std']
                    df_resampled['BB_dn'] = df_resampled['BB_mid'] - float(bb_std) * df_resampled['BB_std']
                    macd1,sig1,hist1 = calculate_macd(df_resampled, int(macd1_f), int(macd1_s), int(macd1_sig))
                    macd2,sig2,hist2 = calculate_macd(df_resampled, int(macd2_f), int(macd2_s), int(macd2_sig))
                    df_resampled['M1_hist']=hist1; df_resampled['M1_macd']=macd1; df_resampled['M1_sig']=sig1
                    df_resampled['M2_hist']=hist2; df_resampled['M2_macd']=macd2; df_resampled['M2_sig']=sig2
                    markers_price_m1,markers_macd_m1 = get_pine_divergence_markers(df_resampled,'M1_macd','M1_hist','S')
                    markers_price_m2,markers_macd_m2 = get_pine_divergence_markers(df_resampled,'M2_macd','M2_hist','L')
                    all_markers_price = merge_kline_markers(markers_price_m1+markers_price_m2)
                    df_plot = df_resampled.tail(int(drawn_days)).copy()
                    plot_valid_dates = set(df_plot['Date_str'].tolist())
                    final_markers_price = sorted([m for m in all_markers_price if m['time'] in plot_valid_dates], key=lambda x: x['time'])
                    final_markers_macd_m1 = sorted([m for m in markers_macd_m1 if m['time'] in plot_valid_dates], key=lambda x: x['time'])
                    final_markers_macd_m2 = sorted([m for m in markers_macd_m2 if m['time'] in plot_valid_dates], key=lambda x: x['time'])
                    all_data = []
                    for i, row in df_plot.iterrows():
                        if pd.isna(row['Close']): continue
                        item = {"time":row['Date_str'],"open":safe_float(row['Open']),"high":safe_float(row['High']),"low":safe_float(row['Low']),"close":safe_float(row['Close']),"vol":safe_float(row['買賣超'])}
                        if not pd.isna(row['BB_mid']): item["bbm"]=safe_float(row['BB_mid'])
                        if not pd.isna(row['BB_up']): item["bbu"]=safe_float(row['BB_up'])
                        if not pd.isna(row['BB_dn']): item["bbd"]=safe_float(row['BB_dn'])
                        if not pd.isna(row['M1_hist']): item["h1"]=safe_float(row['M1_hist'])
                        if not pd.isna(row['M1_macd']): item["m1"]=safe_float(row['M1_macd'])
                        if not pd.isna(row['M1_sig']): item["s1"]=safe_float(row['M1_sig'])
                        if not pd.isna(row['M2_hist']): item["h2"]=safe_float(row['M2_hist'])
                        if not pd.isna(row['M2_macd']): item["m2"]=safe_float(row['M2_macd'])
                        if not pd.isna(row['M2_sig']): item["s2"]=safe_float(row['M2_sig'])
                        all_data.append(item)
                    hlines_js_array = json.dumps(st.session_state.custom_hlines)
                    click_lines_js_array = json.dumps(st.session_state.click_lines)

                    html_code = f"""<!DOCTYPE html><html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body{{margin:0;padding:0;background-color:#131722;overflow:hidden;font-family:"Microsoft JhengHei",sans-serif;}}
        #wrapper{{position:relative;width:calc(100vw - 25px);height:95vh;}}
        #chart{{width:100%;height:100%;}}
        .tv-legend{{position:absolute;left:12px;top:12px;z-index:999;font-size:13px;color:#d1d4dc;pointer-events:none;background:rgba(19,23,34,0.85);padding:8px 12px;border-radius:6px;border:1px solid #363c4e;box-shadow:0 4px 12px rgba(0,0,0,0.5);}}
        .lg-title{{color:#2962FF;font-weight:bold;font-size:15px;margin-bottom:6px;border-bottom:1px solid #444;padding-bottom:4px;}}
        .lg-row{{display:flex;justify-content:space-between;margin-bottom:2px;width:160px;}}
        .lg-label{{color:#a0a3ab;}}
        .lg-vol{{margin-top:6px;padding-top:6px;border-top:1px dashed #555;font-size:15px;display:flex;justify-content:space-between;font-weight:bold;}}
        .lg-macd{{margin-top:6px;font-size:12px;color:#8a8d9d;line-height:1.4;}}
        #mobileDrawBtn{{position:absolute;right:12px;top:12px;z-index:1000;background-color:#ef5350;color:white;border:none;padding:10px 16px;font-size:14px;font-weight:bold;border-radius:8px;cursor:pointer;display:none;box-shadow:0 4px 10px rgba(0,0,0,0.5);}}
        #mobileDrawBtn:active{{background-color:#c62828;}}
    </style>
</head>
<body>
    <div id="wrapper">
        <div id="chart"></div>
        <div id="legend" class="tv-legend">將滑鼠(或手指)移至 K 線上查看數據</div>
        <button id="mobileDrawBtn">✏️ 畫線於此</button>
    </div>
    <script>
        const rawData={json.dumps(all_data)};
        const hlines={hlines_js_array};
        const clickLines={click_lines_js_array};
        const markersPrice={json.dumps(final_markers_price)};
        const markersMacd1={json.dumps(final_markers_macd_m1)};
        const markersMacd2={json.dumps(final_markers_macd_m2)};
        const enableClickLine={'true' if enable_click_line else 'false'};
        let clickLineEnabled=enableClickLine;

        const chart=LightweightCharts.createChart(document.getElementById('chart'),{{
            layout:{{backgroundColor:'#131722',textColor:'#d1d4dc'}},
            watermark:{{color:'rgba(255,255,255,0.08)',visible:true,text:'{drawn_sid_clean} {stock_name}',fontSize:80,horzAlign:'center',vertAlign:'center'}},
            grid:{{vertLines:{{visible:false}},horzLines:{{visible:false}}}},
            crosshair:{{mode:LightweightCharts.CrosshairMode.Normal}},
            localization:{{timeFormatter:time=>{{if(typeof time==='object'&&time.year)return time.year+'-'+String(time.month).padStart(2,'0')+'-'+String(time.day).padStart(2,'0');return time;}}}},
            timeScale:{{tickMarkFormatter:(time)=>{{if(typeof time==='object'&&time.year)return time.year+'-'+String(time.month).padStart(2,'0')+'-'+String(time.day).padStart(2,'0');return time;}}}}
        }});
        chart.priceScale('right').applyOptions({{scaleMargins:{{top:0.02,bottom:0.45}}}});
        const seriesK=chart.addCandlestickSeries({{upColor:'#ef5350',downColor:'#26a69a',borderVisible:false,wickUpColor:'#ef5350',wickDownColor:'#26a69a',priceLineVisible:false}});
        seriesK.setData(rawData.map(d=>({{time:d.time,open:d.open,high:d.high,low:d.low,close:d.close}})));
        seriesK.setMarkers(markersPrice);
        const lastDataTime=rawData[rawData.length-1].time;
        markersPrice.forEach(m=>{{
            if(m.price===undefined)return;
            const lc=m.color==='#ef5350'?'rgba(239,83,80,0.5)':'rgba(38,166,154,0.5)';
            const el=chart.addLineSeries({{color:lc,lineWidth:2,lineStyle:1,lastValueVisible:false,priceLineVisible:false,crosshairMarkerVisible:false}});
            if(m.time===lastDataTime)el.setData([{{time:m.time,value:m.price}}]);
            else el.setData([{{time:m.time,value:m.price}},{{time:lastDataTime,value:m.price}}]);
        }});
        const bbMid=chart.addLineSeries({{color:'#FFD600',lineWidth:1,crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        bbMid.setData(rawData.filter(d=>d.bbm!==undefined).map(d=>({{time:d.time,value:d.bbm}})));
        const bbUp=chart.addLineSeries({{color:'rgba(255,255,255,0.4)',lineWidth:1,lineStyle:2,crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        bbUp.setData(rawData.filter(d=>d.bbu!==undefined).map(d=>({{time:d.time,value:d.bbu}})));
        const bbDn=chart.addLineSeries({{color:'rgba(255,255,255,0.4)',lineWidth:1,lineStyle:2,crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        bbDn.setData(rawData.filter(d=>d.bbd!==undefined).map(d=>({{time:d.time,value:d.bbd}})));
        const seriesVol=chart.addHistogramSeries({{priceFormat:{{type:'volume'}},priceScaleId:'vol',lastValueVisible:false,priceLineVisible:false}});
        seriesVol.setData(rawData.map(d=>({{time:d.time,value:d.vol,color:d.vol>0?'rgba(239,83,80,0.8)':d.vol<0?'rgba(38,166,154,0.8)':'rgba(120,120,120,0.5)'}})));
        chart.priceScale('vol').applyOptions({{scaleMargins:{{top:0.58,bottom:0.28}},visible:false}});
        const seriesH1=chart.addHistogramSeries({{priceFormat:{{type:'volume'}},priceScaleId:'m1',lastValueVisible:false,priceLineVisible:false}});
        seriesH1.setData(rawData.filter(d=>d.h1!==undefined).map(d=>({{time:d.time,value:d.h1,color:d.h1>=0?'rgba(239,83,80,0.5)':'rgba(38,166,154,0.5)'}})));
        const seriesM1=chart.addLineSeries({{color:'#FFD600',lineWidth:1,priceScaleId:'m1',crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        seriesM1.setData(rawData.filter(d=>d.m1!==undefined).map(d=>({{time:d.time,value:d.m1}})));
        seriesM1.setMarkers(markersMacd1);
        const seriesS1=chart.addLineSeries({{color:'#00E676',lineWidth:1,priceScaleId:'m1',crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        seriesS1.setData(rawData.filter(d=>d.s1!==undefined).map(d=>({{time:d.time,value:d.s1}})));
        chart.priceScale('m1').applyOptions({{scaleMargins:{{top:0.72,bottom:0.15}},visible:false}});
        const seriesH2=chart.addHistogramSeries({{priceFormat:{{type:'volume'}},priceScaleId:'m2',lastValueVisible:false,priceLineVisible:false}});
        seriesH2.setData(rawData.filter(d=>d.h2!==undefined).map(d=>({{time:d.time,value:d.h2,color:d.h2>=0?'rgba(239,83,80,0.5)':'rgba(38,166,154,0.5)'}})));
        const seriesM2=chart.addLineSeries({{color:'#FFD600',lineWidth:1,priceScaleId:'m2',crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        seriesM2.setData(rawData.filter(d=>d.m2!==undefined).map(d=>({{time:d.time,value:d.m2}})));
        seriesM2.setMarkers(markersMacd2);
        const seriesS2=chart.addLineSeries({{color:'#00E676',lineWidth:1,priceScaleId:'m2',crosshairMarkerVisible:false,lastValueVisible:false,priceLineVisible:false}});
        seriesS2.setData(rawData.filter(d=>d.s2!==undefined).map(d=>({{time:d.time,value:d.s2}})));
        chart.priceScale('m2').applyOptions({{scaleMargins:{{top:0.85,bottom:0.0}},visible:false}});
        hlines.forEach(val=>{{seriesK.createPriceLine({{price:val,color:'#2962FF',lineWidth:2,lineStyle:2,axisLabelVisible:true,title:'📏'}});}});
        clickLines.forEach(line=>{{
            const rs=chart.addLineSeries({{color:line.color,lineWidth:2,lineStyle:2,lastValueVisible:true,priceLineVisible:false,crosshairMarkerVisible:false,title:line.title}});
            if(line.startTime===lastDataTime)rs.setData([{{time:line.startTime,value:line.price}}]);
            else rs.setData([{{time:line.startTime,value:line.price}},{{time:lastDataTime,value:line.price}}]);
        }});
        const legend=document.getElementById('legend');
        const mobileDrawBtn=document.getElementById('mobileDrawBtn');
        const getDayOfWeek=dStr=>['週日','週一','週二','週三','週四','週五','週六'][new Date(dStr).getDay()];
        const dictByTime={{}};rawData.forEach(d=>dictByTime[d.time]=d);
        if(clickLineEnabled)mobileDrawBtn.style.display='block';
        window.addEventListener('message',e=>{{if(e.data&&e.data.type==='toggle_click_line'){{clickLineEnabled=e.data.value;mobileDrawBtn.style.display=clickLineEnabled?'block':'none';}}}});
        let lastCrosshairParam=null;
        chart.subscribeCrosshairMove(param=>{{
            if(!param.time||param.point===undefined||param.point.x<0||param.point.y<0){{legend.innerHTML='將滑鼠(或手指)移至 K 線上查看數據';return;}}
            lastCrosshairParam=param;
            let timeStr=param.time;
            if(typeof timeStr==='object'&&timeStr.year)timeStr=timeStr.year+'-'+String(timeStr.month).padStart(2,'0')+'-'+String(timeStr.day).padStart(2,'0');
            const d=dictByTime[timeStr]||dictByTime[param.time];if(!d)return;
            const vColor=d.vol>=0?'#ef5350':'#26a69a';
            let html=`<div class="lg-title">{drawn_sid_clean} {stock_name} | ${{timeStr}} ${{getDayOfWeek(timeStr)}}</div><div class="lg-row"><span class="lg-label">開盤</span><span style="color:white;">${{d.open.toFixed(2)}}</span></div><div class="lg-row"><span class="lg-label">最高</span><span style="color:#ef5350;">${{d.high.toFixed(2)}}</span></div><div class="lg-row"><span class="lg-label">最低</span><span style="color:#26a69a;">${{d.low.toFixed(2)}}</span></div><div class="lg-row"><span class="lg-label">收盤</span><span style="color:white;font-weight:bold;">${{d.close.toFixed(2)}}</span></div><div class="lg-vol"><span class="lg-label">買賣 ({drawn_br_name})</span><span style="color:${{vColor}};">${{d.vol}} 張</span></div>`;
            if(d.h1!==undefined)html+=`<div class="lg-macd"><b>短 MACD:</b> 柱 <span style="color:${{d.h1>=0?'#ef5350':'#26a69a'}}">${{d.h1.toFixed(2)}}</span> | 快 <span style="color:#FFD600">${{d.m1.toFixed(2)}}</span> | 慢 <span style="color:#00E676">${{d.s1.toFixed(2)}}</span></div>`;
            if(d.h2!==undefined)html+=`<div class="lg-macd"><b>長 MACD:</b> 柱 <span style="color:${{d.h2>=0?'#ef5350':'#26a69a'}}">${{d.h2.toFixed(2)}}</span> | 快 <span style="color:#FFD600">${{d.m2.toFixed(2)}}</span> | 慢 <span style="color:#00E676">${{d.s2.toFixed(2)}}</span></div>`;
            legend.innerHTML=html;
        }});
        mobileDrawBtn.addEventListener('click',()=>{{
            if(!lastCrosshairParam||lastCrosshairParam.time===undefined||lastCrosshairParam.point===undefined||lastCrosshairParam.point.y<0){{alert('請先觸碰圖表，將十字線移動到指定的 K 棒上！');return;}}
            let timeStr=lastCrosshairParam.time;
            if(typeof timeStr==='object'&&timeStr.year)timeStr=timeStr.year+'-'+String(timeStr.month).padStart(2,'0')+'-'+String(timeStr.day).padStart(2,'0');
            const d=dictByTime[timeStr]||dictByTime[lastCrosshairParam.time];if(!d)return;
            const clickedPrice=seriesK.coordinateToPrice(lastCrosshairParam.point.y);if(clickedPrice===null)return;
            const midPrice=(d.high+d.low)/2;
            let targetPrice,lineColor,lineTitle;
            if(clickedPrice>=midPrice){{targetPrice=d.high;lineColor='#ef5350';lineTitle='壓力 '+targetPrice.toFixed(2);}}
            else{{targetPrice=d.low;lineColor='#26a69a';lineTitle='支撐 '+targetPrice.toFixed(2);}}
            const raySeries=chart.addLineSeries({{color:lineColor,lineWidth:2,lineStyle:2,lastValueVisible:true,priceLineVisible:false,crosshairMarkerVisible:false,title:lineTitle}});
            const ldt=rawData[rawData.length-1].time;
            if(d.time===ldt)raySeries.setData([{{time:d.time,value:targetPrice}}]);
            else raySeries.setData([{{time:d.time,value:targetPrice}},{{time:ldt,value:targetPrice}}]);
            window.parent.postMessage({{type:'streamlit:setComponentValue',value:{{action:'add_click_line',price:targetPrice,startTime:d.time,color:lineColor,title:lineTitle}}}}, '*');
        }});
        chart.subscribeClick(param=>{{if(!clickLineEnabled)return;if(param.point&&param.point.x<window.innerWidth-100)mobileDrawBtn.click();}});
        new ResizeObserver(()=>{{chart.applyOptions({{width:document.getElementById('wrapper').clientWidth,height:document.getElementById('wrapper').clientHeight}});chart.timeScale().fitContent();}}).observe(document.getElementById('wrapper'));
        setTimeout(()=>{{chart.timeScale().fitContent();}},100);
    </script>
    <!-- Render Key: {st.session_state.chart_render_key} -->
</body></html>"""
                    components.html(html_code, height=800)
            except Exception as e: st.error(f"發生錯誤: {e}")
