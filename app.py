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

# ==========================================
# 🆕 嘗試載入 Google Sheets 相關套件
# ==========================================
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="籌碼雷達", layout="wide")

# ==========================================
# 🔒 進入門檻：通行密碼與免登書籤系統
# ==========================================
def check_password():
    valid_passwords = st.secrets.get("passwords", {"測試帳號": "0000|2099-12-31"})
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
                        st.query_params.clear()
                        return True
                except:
                    st.session_state["password_correct"] = True
                    st.session_state["username"] = user
                    st.session_state["user_token"] = url_token
                    st.query_params.clear()
                    return True

    def password_entered():
        user_pwd_input = st.session_state["pwd_input"].strip()
        match_found = False
        is_expired = False
        matched_user = ""
        for user, auth_string in valid_passwords.items():
            parts = str(auth_string).split("|")
            pwd = parts[0].strip()
            exp_date_str = parts[1].strip() if len(parts) > 1 else "2099-12-31" 
            if user_pwd_input == pwd:
                match_found = True
                matched_user = user
                try:
                    if datetime.date.today() > datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date():
                        is_expired = True
                except: pass
                break 

        if match_found and not is_expired:
            st.session_state["password_correct"] = True
            st.session_state["username"] = matched_user
            st.session_state["user_token"] = user_pwd_input
            st.query_params.clear()
            del st.session_state["pwd_input"]  
        elif match_found and is_expired:
            st.session_state["password_correct"] = "expired"
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct") == True: 
        return True

    st.markdown("<br><br><h1 style='text-align: center;'>🔒 籌碼雷達</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'>成功登入後，將網址加入書籤即可免重複輸入密碼。</p>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.text_input("輸入通行密碼：", type="password", on_change=password_entered, key="pwd_input")
        status = st.session_state.get("password_correct")
        if status == False: st.error("🚫 密碼錯誤")
        elif status == "expired": st.warning("⚠️ 會員權限已到期，請洽管理員。")
    return False

if not check_password(): st.stop()  

# ==========================================
# 🆕 Google Sheets 雲端資料庫互動函數 (強化防呆與錯誤顯示)
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
        # 移除網址後方的 ?gid=0 參數，避免 gspread 解析錯誤
        raw_url = st.secrets["gsheets"]["spreadsheet_url"]
        clean_url = raw_url.split("?")[0]
        doc = client.open_by_url(clean_url)
        
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
    if not ws: 
        print(f"GSheets 載入失敗: {msg}")
        return []
    try:
        cell = ws.find(username, in_column=1)
        if cell:
            data = ws.cell(cell.row, 2).value
            if data:
                return json.loads(data)
    except Exception as e:
        # 如果找不到 Cell，gspread 會拋出錯誤，我們直接忽略當作「新用戶」處理
        if "not found" in str(e).lower() or "cellnotfound" in str(type(e)).lower():
            pass
        else:
            print(f"GSheets 讀取錯誤: {e}")
    return []

def save_gsheet_watchlist(username, wl_list):
    ws, msg = init_gsheets()
    if not ws: 
        return False, msg
    try:
        data_str = json.dumps(wl_list, ensure_ascii=False)
        try:
            cell = ws.find(username, in_column=1)
            ws.update_cell(cell.row, 2, data_str)
        except Exception as e:
            # 如果找不到該用戶 (CellNotFound)，就新增一行
            if "not found" in str(e).lower() or "cellnotfound" in str(type(e)).lower():
                ws.append_row([username, data_str])
            else:
                return False, f"寫入更新錯誤: {str(e)}"
        return True, "成功寫入雲端"
    except Exception as e:
        return False, f"寫入錯誤: {str(e)}"

# ==========================================
# 側邊欄：顯示登入者與專屬書籤連結
# ==========================================
with st.sidebar:
    current_user = st.session_state.get('username', 'VIP會員')
    st.markdown(f"### 👤 登入身份：{current_user}")
    st.caption("✅ 網址列已隱藏密碼，直接複製分享網址絕對安全。")
    with st.expander("🔗 取得免登入書籤網址"):
        st.write("若要在您的電腦/手機免密碼登入，請將下方參數**接在您的網站主網址後方**並加入書籤：")
        st.code(f"?token={st.session_state.get('user_token', '')}", language="text")
        st.caption("⚠️ 此參數等同您的密碼，請勿外流！")
        
    if not GSHEETS_AVAILABLE or "gcp_service_account" not in st.secrets:
        st.warning("⚠️ 系統未偵測到 Google Sheets 金鑰，清單將僅暫存於本次連線。")

# ==========================================
# 初始化全域變數與 Session State
# ==========================================
for tab in ['t1', 't2', 't3']:
    if f'{tab}_searched' not in st.session_state: st.session_state[f'{tab}_searched'] = False
    if f'{tab}_buy_df' not in st.session_state: st.session_state[f'{tab}_buy_df'] = pd.DataFrame()
    if f'{tab}_sell_df' not in st.session_state: st.session_state[f'{tab}_sell_df'] = pd.DataFrame()

if 't4_target_sid' not in st.session_state: st.session_state.t4_target_sid = "6488"
if 't4_target_br' not in st.session_state: st.session_state.t4_target_br = "兆豐-忠孝"
if 't4_sid_ui_real' not in st.session_state: st.session_state.t4_sid_ui_real = "6488"
if 't4_br_ui_real' not in st.session_state: st.session_state.t4_br_ui_real = "兆豐-忠孝"

if 'auto_draw' not in st.session_state: st.session_state.auto_draw = False
if 'custom_hlines' not in st.session_state: st.session_state.custom_hlines = []
if 'table_refresh_key' not in st.session_state: st.session_state.table_refresh_key = 0

if 'watchlist_loaded' not in st.session_state:
    st.session_state.watchlist = load_gsheet_watchlist(current_user)
    st.session_state.watchlist_loaded = True

# ==========================================
# 資料載入與處理函數
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
        if loc_name and loc_name not in GEO_MAP: 
            GEO_MAP[loc_name] = {}
        if loc_name:
            GEO_MAP[loc_name][br_name] = br_info

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
def get_stock_kline(stock_id):
    end_date = datetime.date.today() + datetime.timedelta(days=1)
    start_date = "2000-01-01" 
    for suffix in ['.TW', '.TWO']:
        ticker = f"{stock_id}{suffix}"
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if not df.empty:
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            df.reset_index(inplace=True)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            return df
    return pd.DataFrame()

@st.cache_data(ttl=1800)
def get_fubon_history_and_name(sid, br_id):
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    url_history = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?A={sid}&BHID={br_id}&b={br_id}&C=3&D=1999-1-1&E={today_str}&ver=V3"
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
# 1. UI 介面設定
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs(["🚀 特定分點", "📊 股票代號", "📍 地緣券商", "📊 主力 K 線圖"])

# --- Tab 1 ---
with tab1:
    c1, c2 = st.columns(2)
    with c1: 
        sorted_hq_keys = sorted(UI_TREE.keys())
        sel_hq = st.selectbox("選擇券商", sorted_hq_keys, key="t1_b_sel")
    with c2: 
        b_opts = UI_TREE[sel_hq]['branches']
        sel_br_l = st.selectbox("選擇分點", sorted(b_opts.keys()), key="t1_br_sel")
        sel_br_id = b_opts[sel_br_l]

    c3, c4, c5 = st.columns(3)
    with c3: t1_sd = st.date_input("區間起點", datetime.date.today()-datetime.timedelta(days=7), key="t1_sd")
    with c4: t1_ed = st.date_input("區間終點", datetime.date.today(), key="t1_ed")
    with c5: t1_u = st.radio("統計單位", ["張數", "金額"], horizontal=True, key="t1_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t1_mode = st.radio("篩選條件", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t1_mode")
    with c7: t1_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, step=1.0, key="t1_pct")
    with c8: st.write(""); show_full = st.checkbox("顯示完整清單", value=False, key="t1_full")

    if st.button("開始分點尋寶 🚀", key="t1_go"):
        st.session_state.t1_searched = True 
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
                        l = tb.iloc[:, [0, 1, 2]].copy(); l.columns=['股票名稱', col_buy, col_sell]
                        r = tb.iloc[:, [5, 6, 7]].copy(); r.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, l, r], ignore_index=True)
                    else:
                        temp = tb.iloc[:, [0, 1, 2]].copy(); temp.columns=['股票名稱', col_buy, col_sell]
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
                df_show['分點明細'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_br_id}&b={sel_br_id}&C=3" if sid else "")
                df_show['送至 Tab4 繪圖'] = False 
                
                col_buy_name = '買進金額' if '金額' in t1_u else '買進張數'
                col_sell_name = '賣出金額' if '金額' in t1_u else '賣出張數'

                df_show = df_show[['送至 Tab4 繪圖', '股票名稱', 'K線圖', col_buy_name, col_sell_name, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                col_config = {
                    "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈"),
                    "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "送至 Tab4 繪圖": st.column_config.CheckboxColumn("送至 Tab4 繪圖"),
                    "extracted_stock_id": None 
                }
                
                editor_key = f"editor_{key_prefix}_{st.session_state.table_refresh_key}"
                edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=editor_key)
                
                if editor_key in st.session_state:
                    edits = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, changes in edits.items():
                        if changes.get('送至 Tab4 繪圖', False) == True:
                            sid_clicked = df_show.iloc[row_idx]['extracted_stock_id']
                            st.session_state.t4_target_sid = sid_clicked
                            st.session_state['t4_sid_ui_real'] = sid_clicked # 🌟 強制覆寫 UI
                            st.session_state.t4_target_br = sel_br_l
                            st.session_state['t4_br_ui_real'] = sel_br_l     # 🌟 強制覆寫 UI
                            st.session_state.auto_draw = True
                            st.session_state.table_refresh_key += 1
                            st.rerun()

        col_b = '買進金額' if '金額' in t1_u else '買進張數'
        col_s = '賣出金額' if '金額' in t1_u else '賣出張數'
        st.markdown(f"### 🔴 大戶吃貨中 - 共 {len(st.session_state.t1_buy_df)} 檔")
        display_table_with_button(st.session_state.t1_buy_df.sort_values(by=col_b, ascending=False).head(999 if show_full else 10), "t1_buy")
        st.markdown(f"### 🟢 大戶倒貨中 - 共 {len(st.session_state.t1_sell_df)} 檔")
        display_table_with_button(st.session_state.t1_sell_df.sort_values(by=col_s, ascending=False).head(999 if show_full else 10), "t1_sell")

# --- Tab 2 ---
with tab2:
    st.markdown("### 📈 誰在買賣這檔股票？")
    c1, c2, c3 = st.columns(3)
    with c1: t2_sid = st.text_input("股票代號", "2408", key="t2_s")
    with c2: t2_sd = st.date_input("開始", datetime.date.today()-datetime.timedelta(days=7), key="t2_sd_in")
    with c3: t2_ed = st.date_input("結束", datetime.date.today(), key="t2_ed_in")
    
    c4, c5, c6, c7 = st.columns([2, 1, 1, 1.2])
    with c4: t2_m = st.radio("模式", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t2_mode")
    with c5: t2_p = st.number_input("門檻佔比%", 0, 100, 95, step=1, key="t2_p_in")
    with c6: t2_v = st.number_input("最低張數", 0, 1000000, 10, step=1, key="t2_v_in")
    with c7: st.write(""); show_full_t2 = st.checkbox("顯示完整清單", value=False, key="t2_full")

    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
        t2_sid_clean = t2_sid.strip().replace(" ", "").upper()
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
        t2_sid_clean = t2_sid.strip().replace(" ", "").upper()
        
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
                df_show['送至 Tab4 繪圖'] = False 
                df_show = df_show[['送至 Tab4 繪圖', '券商', '買', '賣', '合計', '買進%', '賣出%', '網頁明細']]
                col_config = {
                    "網頁明細": st.column_config.LinkColumn("網頁明細", display_text="🏦", help="外連富邦明細"),
                    "送至 Tab4 繪圖": st.column_config.CheckboxColumn("送至 Tab4 繪圖", help="打勾後請手動切換至分頁四")
                }
                
                editor_key = f"editor_{key_prefix}_{st.session_state.table_refresh_key}"
                edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=editor_key)
                
                if editor_key in st.session_state:
                    edits = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, changes in edits.items():
                        if changes.get('送至 Tab4 繪圖', False) == True:
                            br_clicked = df_show.iloc[row_idx]['券商']
                            st.session_state.t4_target_sid = t2_sid_clean
                            st.session_state['t4_sid_ui_real'] = t2_sid_clean # 🌟 強制覆寫 UI
                            
                            clean_br = br_clicked.replace("亚","亞").strip()
                            matched_br = clean_br if clean_br in BROKER_MAP else next((k for k in BROKER_MAP if clean_br in k or k in clean_br), None)
                            if matched_br:
                                st.session_state.t4_target_br = matched_br
                                st.session_state['t4_br_ui_real'] = matched_br # 🌟 強制覆寫 UI
                                
                            st.session_state.auto_draw = True
                            st.session_state.table_refresh_key += 1 
                            st.rerun()

        st.subheader("🔴 吃貨主力分點")
        display_table_with_button_t2(st.session_state.t2_buy_df.sort_values('買', ascending=False).head(999 if show_full_t2 else 10), "t2_buy")
        st.subheader("🟢 倒貨主力分點")
        display_table_with_button_t2(st.session_state.t2_sell_df.sort_values('賣', ascending=False).head(999 if show_full_t2 else 10), "t2_sell")

# --- Tab 3 ---
with tab3:
    st.markdown("### 📍 尋找地緣/同名分點進出")
    st.caption("透過分點名稱後綴（例如：城中、三重、信義）跨券商尋找特定地區的買賣神人。")
    c1, c2 = st.columns(2)
    with c1:
        sorted_loc_keys = sorted(GEO_MAP.keys())
        default_loc_idx = sorted_loc_keys.index('城中') if '城中' in sorted_loc_keys else 0
        sel_loc = st.selectbox("選擇地緣關鍵字 (支援手動輸入搜尋)", sorted_loc_keys, index=default_loc_idx, key="t3_loc_sel")
    with c2:
        loc_branches = GEO_MAP[sel_loc]
        sorted_loc_br_keys = sorted(loc_branches.keys())
        sel_t3_br_l = st.selectbox("選擇該區特定分點", sorted_loc_br_keys, key=f"t3_br_sel_{sel_loc}")
        sel_t3_br_info = loc_branches[sel_t3_br_l]
        sel_t3_hq_id = sel_t3_br_info['hq_id']
        sel_t3_br_id = sel_t3_br_info['br_id']

    c3, c4, c5 = st.columns(3)
    with c3: t3_sd = st.date_input("區間起點", datetime.date.today()-datetime.timedelta(days=7), key="t3_sd")
    with c4: t3_ed = st.date_input("區間終點", datetime.date.today(), key="t3_ed")
    with c5: t3_u = st.radio("統計單位", ["張數", "金額"], horizontal=True, key="t3_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t3_mode = st.radio("篩選條件", ["嚴格模式 (只買不賣)", "濾網模式 (自訂佔比)"], index=1, horizontal=True, key="t3_mode")
    with c7: t3_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, step=1.0, key="t3_pct")
    with c8: st.write(""); show_full_t3 = st.checkbox("顯示完整清單", value=False, key="t3_full")

    if st.button("啟動地緣雷達 📡", key="t3_go"):
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
                if m:
                    stock_id = m.group(1).strip()
                    stock_name = m.group(2).strip()
                    return f"{stock_id}{stock_name}"
                return ""
            processed_html_text = re.sub(r"<script[^>]*>(?:(?!</script>).)*GenLink2stk\s*\([^)]+\).*?</script>", extract_stock_name_from_script, html_text, flags=re.IGNORECASE | re.DOTALL)
            tables = pd.read_html(StringIO(processed_html_text))
            df_all = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] < 3: continue
                if any(word in str(tb) for word in ['買進','賣出','張數','金額','股票名稱']):
                    if tb.shape[1] >= 8:
                        l = tb.iloc[:, [0, 1, 2]].copy(); l.columns=['股票名稱', col_buy, col_sell]
                        r = tb.iloc[:, [5, 6, 7]].copy(); r.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, l, r], ignore_index=True)
                    else:
                        temp = tb.iloc[:, [0, 1, 2]].copy(); temp.columns=['股票名稱', col_buy, col_sell]
                        df_all = pd.concat([df_all, temp], ignore_index=True)

            if not df_all.empty:
                df_all['股票名稱'] = df_all['股票名稱'].astype(str).str.strip()
                invalid_patterns = ['名稱', '買進', '賣出', '合計', '說明', '註', '差額', '請選擇', 'nan', 'NaN', 'None', r'^\s*$']
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
            else: st.warning("抓取不到數據。請檢查股票代號或券商分點是否正確。")
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
                df_show['分點明細'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_t3_br_id}&b={sel_t3_br_id}&C=3" if sid else "")
                df_show['帶入K線'] = False 
                df_show = df_show[['帶入K線', '股票名稱', 'K線圖', col_buy, col_sell, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                col_config = {
                    "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈"),
                    "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "帶入K線": st.column_config.CheckboxColumn("送至 Tab4 繪圖"),
                    "extracted_stock_id": None 
                }
                
                editor_key = f"editor_{key_prefix}_{st.session_state.table_refresh_key}"
                edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=editor_key)
                
                if editor_key in st.session_state:
                    edits = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, changes in edits.items():
                        if changes.get('帶入K線', False) == True:
                            sid_clicked = df_show.iloc[row_idx]['extracted_stock_id']
                            st.session_state.t4_target_sid = sid_clicked
                            st.session_state['t4_sid_ui_real'] = sid_clicked # 🌟 強制覆寫 UI
                            st.session_state.t4_target_br = sel_t3_br_l
                            st.session_state['t4_br_ui_real'] = sel_t3_br_l  # 🌟 強制覆寫 UI
                            st.session_state.auto_draw = True
                            st.session_state.table_refresh_key += 1 
                            st.rerun()

        sd_s, ed_s = t3_sd.strftime('%Y-%m-%d'), t3_ed.strftime('%Y-%m-%d')
        st.subheader(f"🕵️ 地緣雷達結果：{sel_t3_br_l}")
        st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t3_u}")
        st.markdown(f"### 🔴 該分點吃貨中 (極端買進) - 共 {len(st.session_state.t3_buy_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_buy_df.sort_values(by=col_buy, ascending=False).head(999 if show_full_t3 else 10), "t3_buy")
        st.markdown(f"### 🟢 該分點倒貨中 (極端賣出) - 共 {len(st.session_state.t3_sell_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_sell_df.sort_values(by=col_sell, ascending=False).head(999 if show_full_t3 else 10), "t3_sell")

# --- Tab 4 (專業繪圖) ---
with tab4:
    if st.session_state.auto_draw:
        st.success("✅ 參數已帶入！請直接查看下方圖表。")
        
    col_t1, col_t2, col_t3, col_t4, col_t5 = st.columns([1, 1.5, 1, 1, 1])
    
    with col_t1:
        # 🌟 改為直接綁定 session state 中的 t4_sid_ui_real 確保完全同步
        t4_sid = st.text_input("股票代號", key="t4_sid_ui_real")
    with col_t2:
        all_br_names = sorted(list(BROKER_MAP.keys()))
        t4_br_name = st.selectbox("搜尋分點", all_br_names, key="t4_br_ui_real")
    with col_t3: 
        t4_period = st.radio("週期", ["日", "週", "月"], horizontal=True)
    with col_t4: 
        t4_days = st.number_input("K棒數", value=200, min_value=10, max_value=1000)
    with col_t5:
        st.write("") 
        draw_btn = st.button("🎨 繪圖", use_container_width=True)
        
    col_x1, col_x2_1, col_x2_2, col_x3 = st.columns([1.5, 2, 1, 1.5])
    with col_x1:
        fav_btn = st.button("❤️ 存入清單", use_container_width=True)
    with col_x2_1:
        hline_val = st.number_input("📏 水平線價格", value=0.0, step=1.0, key="hline_val_input")
    with col_x2_2:
        st.write("")
        if st.button("➕ 加入畫線", use_container_width=True):
            if hline_val > 0 and hline_val not in st.session_state.custom_hlines:
                st.session_state.custom_hlines.append(hline_val)
            st.session_state.t4_target_sid = t4_sid
            st.session_state.t4_target_br = t4_br_name
            st.session_state.auto_draw = True
            st.rerun()
    with col_x3:
        st.write("")
        if st.button("🗑️ 清除所有畫線", use_container_width=True):
            st.session_state.custom_hlines = []
            st.session_state.t4_target_sid = t4_sid
            st.session_state.t4_target_br = t4_br_name
            st.session_state.auto_draw = True
            st.rerun()
    
    t4_sid_clean = t4_sid.strip().upper()

    # 🌟 GSheets: 新增清單 (加入錯誤防呆回報)
    if fav_btn:
        entry = {"股票代號": t4_sid_clean, "追蹤分點": t4_br_name}
        if entry not in st.session_state.watchlist:
            st.session_state.watchlist.append(entry)
            
            # 寫入雲端並接收狀態
            success, msg = save_gsheet_watchlist(current_user, st.session_state.watchlist)
            if success:
                st.success(f"✅ 已存入【{current_user}】專屬雲端清單！")
            else:
                st.warning(f"⚠️ 雲端同步失敗！(錯誤原因: {msg})，目前僅存於本次暫存區。")
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
        with st.expander(f"⭐ 【{current_user}】的專屬主力清單", expanded=True):
            wl_df = pd.DataFrame(st.session_state.watchlist)
            wl_df.insert(0, '載入', False)
            wl_df['刪除'] = False
            wl_config = {"載入": st.column_config.CheckboxColumn("載入繪圖"), "刪除": st.column_config.CheckboxColumn("刪除")}
            edited_wl = st.data_editor(wl_df, hide_index=True, column_config=wl_config, use_container_width=True, key="wl_editor")
            
            if not edited_wl[edited_wl['載入'] == True].empty:
                load_sid = edited_wl[edited_wl['載入'] == True].iloc[0]['股票代號']
                load_br = edited_wl[edited_wl['載入'] == True].iloc[0]['追蹤分點']
                
                st.session_state.t4_target_sid = load_sid
                st.session_state['t4_sid_ui_real'] = load_sid
                st.session_state.t4_target_br = load_br
                st.session_state['t4_br_ui_real'] = load_br
                st.session_state.auto_draw = True
                st.rerun()
                
            # 🌟 GSheets: 刪除清單
            if not edited_wl[edited_wl['刪除'] == True].empty:
                del_sid = edited_wl[edited_wl['刪除'] == True].iloc[0]['股票代號']
                del_br = edited_wl[edited_wl['刪除'] == True].iloc[0]['追蹤分點']
                st.session_state.watchlist = [item for item in st.session_state.watchlist if not (item['股票代號'] == del_sid and item['追蹤分點'] == del_br)]
                
                # 同步刪除雲端
                success, msg = save_gsheet_watchlist(current_user, st.session_state.watchlist)
                if not success:
                    st.error(f"雲端刪除同步失敗: {msg}")
                st.rerun()

    # 執行繪圖
    if draw_btn or st.session_state.auto_draw:
        st.session_state.auto_draw = False 
        
        # 同步回寫 UI 狀態
        st.session_state.t4_target_sid = t4_sid
        st.session_state.t4_target_br = t4_br_name
        
        t4_br_id = BROKER_MAP[t4_br_name]['br_id']
        
        with st.spinner(f"為您繪製 {t4_sid_clean} 中..."):
            try:
                df_k = get_stock_kline(t4_sid_clean)
                
                if df_k.empty: 
                    st.error("找不到 K 線資料。")
                else:
                    df_broker, stock_name = get_fubon_history_and_name(t4_sid_clean, t4_br_id)
                    
                    if df_broker.empty:
                        st.info("近期無交易紀錄。")

                    df_merged = pd.merge(df_k, df_broker[['Date', '買賣超']], on='Date', how='left')
                    df_merged['買賣超'] = df_merged['買賣超'].fillna(0) 
                    
                    df_merged.set_index('Date', inplace=True)
                    if t4_period == "週": df_resampled = df_merged.resample('W-FRI').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', '買賣超':'sum'})
                    elif t4_period == "月": df_resampled = df_merged.resample('ME').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', '買賣超':'sum'})
                    else: df_resampled = df_merged.copy()
                    
                    df_resampled = df_resampled.dropna(subset=['Close']).reset_index()
                    df_resampled['Date_str'] = df_resampled['Date'].dt.strftime('%Y-%m-%d') 

                    df_resampled['BB_mid'] = df_resampled['Close'].rolling(int(bb_w)).mean()
                    df_resampled['BB_std'] = df_resampled['Close'].rolling(int(bb_w)).std()
                    df_resampled['BB_up'] = df_resampled['BB_mid'] + float(bb_std) * df_resampled['BB_std']
                    df_resampled['BB_dn'] = df_resampled['BB_mid'] - float(bb_std) * df_resampled['BB_std']
                    
                    macd1, sig1, hist1 = calculate_macd(df_resampled, int(macd1_f), int(macd1_s), int(macd1_sig))
                    macd2, sig2, hist2 = calculate_macd(df_resampled, int(macd2_f), int(macd2_s), int(macd2_sig))
                    
                    df_plot = df_resampled.tail(int(t4_days)).copy()
                    
                    df_plot['M1_hist'] = hist1.tail(int(t4_days))
                    df_plot['M1_macd'] = macd1.tail(int(t4_days))
                    df_plot['M1_sig'] = sig1.tail(int(t4_days))
                    
                    df_plot['M2_hist'] = hist2.tail(int(t4_days))
                    df_plot['M2_macd'] = macd2.tail(int(t4_days))
                    df_plot['M2_sig'] = sig2.tail(int(t4_days))

                    all_data = []
                    for i, row in df_plot.iterrows():
                        if pd.isna(row['Close']): continue
                        
                        item = {
                            "time": row['Date_str'],
                            "open": safe_float(row['Open']), "high": safe_float(row['High']),
                            "low": safe_float(row['Low']), "close": safe_float(row['Close']),
                            "vol": safe_float(row['買賣超'])
                        }
                        if not pd.isna(row['BB_mid']): item["bbm"] = safe_float(row['BB_mid'])
                        if not pd.isna(row['BB_up']): item["bbu"] = safe_float(row['BB_up'])
                        if not pd.isna(row['BB_dn']): item["bbd"] = safe_float(row['BB_dn'])
                        
                        if not pd.isna(row['M1_hist']): item["h1"] = safe_float(row['M1_hist'])
                        if not pd.isna(row['M1_macd']): item["m1"] = safe_float(row['M1_macd'])
                        if not pd.isna(row['M1_sig']): item["s1"] = safe_float(row['M1_sig'])

                        if not pd.isna(row['M2_hist']): item["h2"] = safe_float(row['M2_hist'])
                        if not pd.isna(row['M2_macd']): item["m2"] = safe_float(row['M2_macd'])
                        if not pd.isna(row['M2_sig']): item["s2"] = safe_float(row['M2_sig'])

                        all_data.append(item)

                    hlines_js_array = json.dumps(st.session_state.custom_hlines)

                    html_code = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
                        <script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
                        <style>
                            body {{ margin: 0; padding: 0; background-color: #131722; overflow: hidden; font-family: "Microsoft JhengHei", sans-serif; }}
                            #wrapper {{ position: relative; width: 100vw; height: 95vh; }}
                            #chart {{ width: 100%; height: 100%; }}
                            
                            .tv-legend {{
                                position: absolute; left: 12px; top: 12px; z-index: 999; font-size: 13px; color: #d1d4dc; 
                                pointer-events: none; background: rgba(19, 23, 34, 0.85); padding: 8px 12px; 
                                border-radius: 6px; border: 1px solid #363c4e; box-shadow: 0 4px 12px rgba(0,0,0,0.5);
                            }}
                            .lg-title {{ color: #2962FF; font-weight: bold; font-size: 15px; margin-bottom: 6px; border-bottom: 1px solid #444; padding-bottom: 4px; }}
                            .lg-row {{ display: flex; justify-content: space-between; margin-bottom: 2px; width: 160px; }}
                            .lg-label {{ color: #a0a3ab; }}
                            .lg-vol {{ margin-top: 6px; padding-top: 6px; border-top: 1px dashed #555; font-size: 15px; display: flex; justify-content: space-between; font-weight: bold; }}
                            .lg-macd {{ margin-top: 6px; font-size: 12px; color: #8a8d9d; line-height: 1.4; }}
                        </style>
                    </head>
                    <body>
                        <div id="wrapper">
                            <div id="chart"></div>
                            <div id="legend" class="tv-legend">將滑鼠移至 K 線上查看數據</div>
                        </div>
                        <script>
                            const rawData = {json.dumps(all_data)};
                            const hlines = {hlines_js_array};
                            
                            const layoutOptions = {{ 
                                layout: {{ backgroundColor: '#131722', textColor: '#d1d4dc' }},
                                watermark: {{
                                    color: 'rgba(255, 255, 255, 0.08)',
                                    visible: true,
                                    text: '{t4_sid_clean} {stock_name}',
                                    fontSize: 80,
                                    horzAlign: 'center',
                                    vertAlign: 'center',
                                }},
                                grid: {{ vertLines: {{ color: '#242733' }}, horzLines: {{ color: '#242733' }} }}, 
                                crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }},
                                localization: {{
                                    timeFormatter: time => {{
                                        if (typeof time === 'object' && time.year) return time.year + '-' + String(time.month).padStart(2, '0') + '-' + String(time.day).padStart(2, '0');
                                        return time;
                                    }}
                                }},
                                timeScale: {{
                                    tickMarkFormatter: (time) => {{
                                        if (typeof time === 'object' && time.year) return time.year + '-' + String(time.month).padStart(2, '0') + '-' + String(time.day).padStart(2, '0');
                                        return time;
                                    }}
                                }}
                            }};
                            
                            const chart = LightweightCharts.createChart(document.getElementById('chart'), layoutOptions);
                            
                            chart.priceScale('right').applyOptions({{
                                scaleMargins: {{ top: 0.02, bottom: 0.45 }}
                            }});

                            // K線
                            const seriesK = chart.addCandlestickSeries({{ upColor: '#ef5350', downColor: '#26a69a', borderVisible: false, wickUpColor: '#ef5350', wickDownColor: '#26a69a' }});
                            seriesK.setData(rawData.map(d => ({{time: d.time, open: d.open, high: d.high, low: d.low, close: d.close}})));
                            
                            // BB
                            const bbMid = chart.addLineSeries({{ color: '#FFD600', lineWidth: 1, crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            bbMid.setData(rawData.filter(d => d.bbm !== undefined).map(d => ({{time: d.time, value: d.bbm}})));
                            const bbUp = chart.addLineSeries({{ color: 'rgba(255, 255, 255, 0.4)', lineWidth: 1, lineStyle: 2, crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            bbUp.setData(rawData.filter(d => d.bbu !== undefined).map(d => ({{time: d.time, value: d.bbu}})));
                            const bbDn = chart.addLineSeries({{ color: 'rgba(255, 255, 255, 0.4)', lineWidth: 1, lineStyle: 2, crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            bbDn.setData(rawData.filter(d => d.bbd !== undefined).map(d => ({{time: d.time, value: d.bbd}})));
                            
                            // 買賣超 (獨立座標)
                            const seriesVol = chart.addHistogramSeries({{ priceFormat: {{ type: 'volume' }}, priceScaleId: 'vol', lastValueVisible: false, priceLineVisible: false }});
                            seriesVol.setData(rawData.map(d => ({{time: d.time, value: d.vol, color: d.vol >= 0 ? 'rgba(239, 83, 80, 0.8)' : 'rgba(38, 166, 154, 0.8)'}})));
                            chart.priceScale('vol').applyOptions({{ scaleMargins: {{ top: 0.58, bottom: 0.28 }}, visible: false }});
                            
                            // MACD 短線
                            const seriesH1 = chart.addHistogramSeries({{ priceFormat: {{ type: 'volume' }}, priceScaleId: 'm1', lastValueVisible: false, priceLineVisible: false }});
                            seriesH1.setData(rawData.filter(d => d.h1 !== undefined).map(d => ({{time: d.time, value: d.h1, color: d.h1 >= 0 ? 'rgba(239, 83, 80, 0.5)' : 'rgba(38, 166, 154, 0.5)'}})));
                            const seriesM1 = chart.addLineSeries({{ color: '#FFD600', lineWidth: 1, priceScaleId: 'm1', crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            seriesM1.setData(rawData.filter(d => d.m1 !== undefined).map(d => ({{time: d.time, value: d.m1}})));
                            const seriesS1 = chart.addLineSeries({{ color: '#00E676', lineWidth: 1, priceScaleId: 'm1', crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            seriesS1.setData(rawData.filter(d => d.s1 !== undefined).map(d => ({{time: d.time, value: d.s1}})));
                            chart.priceScale('m1').applyOptions({{ scaleMargins: {{ top: 0.72, bottom: 0.15 }}, visible: false }});

                            // MACD 長線
                            const seriesH2 = chart.addHistogramSeries({{ priceFormat: {{ type: 'volume' }}, priceScaleId: 'm2', lastValueVisible: false, priceLineVisible: false }});
                            seriesH2.setData(rawData.filter(d => d.h2 !== undefined).map(d => ({{time: d.time, value: d.h2, color: d.h2 >= 0 ? 'rgba(239, 83, 80, 0.5)' : 'rgba(38, 166, 154, 0.5)'}})));
                            const seriesM2 = chart.addLineSeries({{ color: '#FFD600', lineWidth: 1, priceScaleId: 'm2', crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            seriesM2.setData(rawData.filter(d => d.m2 !== undefined).map(d => ({{time: d.time, value: d.m2}})));
                            const seriesS2 = chart.addLineSeries({{ color: '#00E676', lineWidth: 1, priceScaleId: 'm2', crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false }});
                            seriesS2.setData(rawData.filter(d => d.s2 !== undefined).map(d => ({{time: d.time, value: d.s2}})));
                            chart.priceScale('m2').applyOptions({{ scaleMargins: {{ top: 0.85, bottom: 0.0 }}, visible: false }});

                            // 水平線
                            hlines.forEach(val => {{
                                seriesK.createPriceLine({{
                                    price: val, color: '#2962FF', lineWidth: 2, lineStyle: 2, 
                                    axisLabelVisible: true, title: '📏',
                                }});
                            }});

                            const legend = document.getElementById('legend');
                            const getDayOfWeek = (dStr) => ['週日', '週一', '週二', '週三', '週四', '週五', '週六'][new Date(dStr).getDay()];
                            const dictByTime = {{}}; rawData.forEach(d => dictByTime[d.time] = d);

                            chart.subscribeCrosshairMove(param => {{
                                if(!param.time || param.point === undefined || param.point.x < 0 || param.point.y < 0) {{
                                    legend.innerHTML = '將滑鼠移至 K 線上查看數據'; return;
                                }}
                                
                                let timeStr = param.time;
                                if (typeof timeStr === 'object' && timeStr.year) {{
                                    timeStr = timeStr.year + '-' + String(timeStr.month).padStart(2, '0') + '-' + String(timeStr.day).padStart(2, '0');
                                }}

                                const d = dictByTime[timeStr] || dictByTime[param.time];
                                if(!d) return;

                                const vColor = d.vol >= 0 ? '#ef5350' : '#26a69a';
                                
                                let html = `
                                    <div class="lg-title">{t4_sid_clean} {stock_name} | ${{timeStr}} ${{getDayOfWeek(timeStr)}}</div>
                                    <div class="lg-row"><span class="lg-label">開盤</span><span style="color:white;">${{d.open.toFixed(2)}}</span></div>
                                    <div class="lg-row"><span class="lg-label">最高</span><span style="color:#ef5350;">${{d.high.toFixed(2)}}</span></div>
                                    <div class="lg-row"><span class="lg-label">最低</span><span style="color:#26a69a;">${{d.low.toFixed(2)}}</span></div>
                                    <div class="lg-row"><span class="lg-label">收盤</span><span style="color:white;font-weight:bold;">${{d.close.toFixed(2)}}</span></div>
                                    <div class="lg-vol"><span class="lg-label">買賣超 ({t4_br_name})</span> <span style="color:${{vColor}};">${{d.vol}} 張</span></div>
                                `;

                                if(d.h1 !== undefined) html += `<div class="lg-macd"><b>短 MACD:</b> 柱 <span style="color:${{d.h1>=0?'#ef5350':'#26a69a'}}">${{d.h1.toFixed(2)}}</span> | 快 <span style="color:#FFD600">${{d.m1.toFixed(2)}}</span> | 慢 <span style="color:#00E676">${{d.s1.toFixed(2)}}</span></div>`;
                                if(d.h2 !== undefined) html += `<div class="lg-macd"><b>長 MACD:</b> 柱 <span style="color:${{d.h2>=0?'#ef5350':'#26a69a'}}">${{d.h2.toFixed(2)}}</span> | 快 <span style="color:#FFD600">${{d.m2.toFixed(2)}}</span> | 慢 <span style="color:#00E676">${{d.s2.toFixed(2)}}</span></div>`;
                                
                                legend.innerHTML = html;
                            }});

                            new ResizeObserver(() => {{
                                chart.applyOptions({{
                                    width: document.getElementById('wrapper').clientWidth, 
                                    height: document.getElementById('wrapper').clientHeight
                                }});
                                chart.timeScale().fitContent();
                            }}).observe(document.getElementById('wrapper'));
                            
                            setTimeout(() => {{ chart.timeScale().fitContent(); }}, 100);
                        </script>
                    </body>
                    </html>
                    """
                    components.html(html_code, height=800)
            except Exception as e: st.error(f"發生錯誤: {e}")
