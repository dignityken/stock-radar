import streamlit as st
import pandas as pd
import requests
from io import StringIO
import re
import datetime
import urllib3
import unicodedata
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
        for auth_string in valid_passwords.values():
            if url_token == auth_string.split("|")[0].strip():
                exp_date_str = auth_string.split("|")[1].strip() if "|" in auth_string else "2099-12-31"
                try:
                    if datetime.date.today() <= datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date():
                        return True
                except:
                    return True

    def password_entered():
        user_pwd_input = st.session_state["pwd_input"].strip()
        match_found = False
        is_expired = False
        for user, auth_string in valid_passwords.items():
            parts = str(auth_string).split("|")
            pwd = parts[0].strip()
            exp_date_str = parts[1].strip() if len(parts) > 1 else "2099-12-31" 
            if user_pwd_input == pwd:
                match_found = True
                try:
                    if datetime.date.today() > datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date():
                        is_expired = True
                except: pass
                break 

        if match_found and not is_expired:
            st.session_state["password_correct"] = True
            st.query_params["token"] = user_pwd_input
            del st.session_state["pwd_input"]  
        elif match_found and is_expired:
            st.session_state["password_correct"] = "expired"
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct") == True: return True

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
# 初始化全域變數與 Session State
# ==========================================
HEADERS = {"User-Agent": "Mozilla/5.0"} 
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drivesdk"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drivesdk"

for tab in ['t1', 't2', 't3']:
    if f'{tab}_searched' not in st.session_state: st.session_state[f'{tab}_searched'] = False
    if f'{tab}_buy_df' not in st.session_state: st.session_state[f'{tab}_buy_df'] = pd.DataFrame()
    if f'{tab}_sell_df' not in st.session_state: st.session_state[f'{tab}_sell_df'] = pd.DataFrame()

# 🎯 核心修正：強制綁定 UI 輸入框的 Key
if 't4_sid_ui' not in st.session_state: st.session_state.t4_sid_ui = "6488"
if 't4_br_ui' not in st.session_state: st.session_state.t4_br_ui = "兆豐-忠孝"
if 'auto_draw' not in st.session_state: st.session_state.auto_draw = False
if 't4_drawn' not in st.session_state: st.session_state.t4_drawn = False
if 'locked_sid' not in st.session_state: st.session_state.locked_sid = "6488"
if 'locked_br_id' not in st.session_state: st.session_state.locked_br_id = "0037003000300061"
if 'locked_br_name' not in st.session_state: st.session_state.locked_br_name = "兆豐-忠孝"
if 'watchlist' not in st.session_state: st.session_state.watchlist = []

def send_to_tab4(sid, br_name):
    """跨頁傳送函數：直接改寫 Tab4 輸入框的底層數值"""
    st.session_state.t4_sid_ui = sid
    clean_br = br_name.replace("亚","亞").strip()
    if clean_br in BROKER_MAP:
        st.session_state.t4_br_ui = clean_br
    st.session_state.auto_draw = True

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
        if loc_name not in GEO_MAP: GEO_MAP[loc_name] = {}
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
def get_fubon_history(sid, br_id):
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    url_history = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?A={sid}&BHID={br_id}&b={br_id}&C=3&D=1999-1-1&E={today_str}&ver=V3"
    res_hist = requests.get(url_history, headers=HEADERS, verify=False, timeout=20)
    res_hist.encoding = 'big5'
    tables = pd.read_html(StringIO(res_hist.text))
    for tb in tables:
        if tb.shape[1] == 5 and '日期' in str(tb.iloc[0].values):
            df_broker = tb.copy()
            df_broker.columns = ['Date', '買進', '賣出', '總額', '買賣超']
            df_broker = df_broker.drop(0) 
            df_broker = df_broker[~df_broker['Date'].str.contains('日期|合計|說明', na=False)].copy()
            df_broker['Date'] = pd.to_datetime(df_broker['Date'].astype(str).str.replace(' ', ''))
            df_broker['買賣超'] = pd.to_numeric(df_broker['買賣超'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            return df_broker
    return pd.DataFrame(columns=['Date', '買賣超'])

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
                df_show['畫圖'] = False 
                
                col_buy_name = '買進金額' if '金額' in t1_u else '買進張數'
                col_sell_name = '賣出金額' if '金額' in t1_u else '賣出張數'

                df_show = df_show[['畫圖', '股票名稱', 'K線圖', col_buy_name, col_sell_name, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                col_config = {
                    "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈"),
                    "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "畫圖": st.column_config.CheckboxColumn("送至Tab4"),
                    "extracted_stock_id": None 
                }
                edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=f"editor_{key_prefix}")
                clicked_rows = edited_df[edited_df['畫圖'] == True]
                if not clicked_rows.empty:
                    sid_clicked = clicked_rows.iloc[0]['extracted_stock_id']
                    send_to_tab4(sid_clicked, sel_br_l)
                    st.success(f"✅ 已設定！請點擊上方「📊 主力 K 線圖」。")

        col_b = '買進金額' if '金額' in t1_u else '買進張數'
        col_s = '賣出金額' if '金額' in t1_u else '賣出張數'
        st.markdown(f"### 🔴 大戶吃貨中 - 共 {len(st.session_state.t1_buy_df)} 檔")
        display_table_with_button(st.session_state.t1_buy_df.sort_values(by=col_b, ascending=False).head(999 if show_full else 10), "t1_buy")
        st.markdown(f"### 🟢 大戶倒貨中 - 共 {len(st.session_state.t1_sell_df)} 檔")
        display_table_with_button(st.session_state.t1_sell_df.sort_values(by=col_s, ascending=False).head(999 if show_full else 10), "t1_sell")

# --- Tab 2 ---
with tab2:
    c1, c2, c3 = st.columns(3)
    with c1: t2_sid = st.text_input("股票代號", "2408", key="t2_s")
    with c2: t2_sd = st.date_input("開始", datetime.date.today()-datetime.timedelta(days=7), key="t2_sd_in")
    with c3: t2_ed = st.date_input("結束", datetime.date.today(), key="t2_ed_in")
    
    c4, c5, c6, c7 = st.columns([2, 1, 1, 1.2])
    with c4: t2_m = st.radio("模式", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t2_mode")
    with c5: t2_p = st.number_input("門檻佔比%", 0, 100, 95, step=1, key="t2_p_in")
    with c6: t2_v = st.number_input("最低張數", 0, 1000000, 10, step=1, key="t2_v_in")
    with c7: st.write(""); show_full_t2 = st.checkbox("顯示完整清單", value=False, key="t2_full")

    t2_sid_clean = t2_sid.strip().replace(" ", "").upper()

    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
        st.session_state.t2_searched = True
        if not t2_sid_clean.isalnum(): st.error("代號錯誤。")
        else:
            url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={t2_sid_clean}&e={t2_sd.strftime('%Y-%m-%d')}&f={t2_ed.strftime('%Y-%m-%d')}"
            try:
                res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
                res.encoding = 'big5'
                tables = pd.read_html(StringIO(res.text))
                df_all = pd.DataFrame()
                
                # ✅ 【還原修正】穩健的表格過濾法 (避免欄位數量跳動導致崩潰)
                for tb in tables:
                    if tb.shape[1] >= 8 and any('券商' in str(c) for c in tb.values.flatten()):
                        try:
                            l = tb.iloc[:, [0, 1, 2]].copy(); l.columns = ['券商', '買', '賣']
                            mid_idx = tb.shape[1] // 2
                            r = tb.iloc[:, [mid_idx, mid_idx+1, mid_idx+2]].copy(); r.columns = ['券商', '買', '賣']
                            df_all = pd.concat([df_all, l, r], ignore_index=True)
                        except: pass
                
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
                else: st.warning("無資料。")
            except Exception as e: st.error(f"發生錯誤: {e}")

    if st.session_state.t2_searched:
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
                df_show['畫圖'] = False 
                df_show = df_show[['畫圖', '券商', '買', '賣', '合計', '買進%', '賣出%', '網頁明細']]
                col_config = {
                    "網頁明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "畫圖": st.column_config.CheckboxColumn("送至Tab4")
                }
                
                # ✅ 【還原修正】精準攔截繪圖勾選狀態
                editor_key = f"editor_{key_prefix}"
                st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=editor_key)
                
                if editor_key in st.session_state:
                    edited_rows = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, edits in edited_rows.items():
                        if edits.get('畫圖', False) == True:
                            br_clicked = df_show.iloc[row_idx]['券商']
                            del st.session_state[editor_key]
                            send_to_tab4(t2_sid_clean, br_clicked)
                            st.success(f"✅ 參數已設定！請點擊上方「📊 主力 K 線圖」。")
                            break

        st.subheader("🔴 吃貨主力分點")
        display_table_with_button_t2(st.session_state.t2_buy_df.sort_values('買', ascending=False).head(999 if show_full_t2 else 10), "t2_buy")
        st.subheader("🟢 倒貨主力分點")
        display_table_with_button_t2(st.session_state.t2_sell_df.sort_values('賣', ascending=False).head(999 if show_full_t2 else 10), "t2_sell")

# --- Tab 3 ---
with tab3:
    # ✅ 【還原修正】直覺式的關鍵字搜尋分點
    c1, c2 = st.columns(2)
    with c1:
        t3_keyword = st.text_input("輸入地緣關鍵字 (如: 城中, 忠孝, 竹科)", "城中", key="t3_kw")
    with c2:
        matched_brs = {k: v for k, v in BROKER_MAP.items() if t3_keyword in k}
        if not matched_brs:
            st.warning("找不到包含此關鍵字的分點")
        else:
            sel_t3_br_l = st.selectbox("選擇特定分點", sorted(matched_brs.keys()), key="t3_br_sel")
            sel_t3_br_id = matched_brs[sel_t3_br_l]['br_id']
            sel_t3_hq_id = matched_brs[sel_t3_br_l]['hq_id']

    c3, c4, c5 = st.columns(3)
    with c3: t3_sd = st.date_input("區間起點", datetime.date.today()-datetime.timedelta(days=7), key="t3_sd")
    with c4: t3_ed = st.date_input("區間終點", datetime.date.today(), key="t3_ed")
    with c5: t3_u = st.radio("單位", ["張數", "金額"], horizontal=True, key="t3_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t3_mode = st.radio("篩選", ["嚴格", "濾網"], index=1, horizontal=True, key="t3_mode")
    with c7: t3_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, key="t3_pct")
    with c8: st.write(""); show_full_t3 = st.checkbox("完整清單", value=False, key="t3_full")

    # ✅ 【還原修正】阻擋空集合時按鈕錯誤
    if st.button("啟動地緣雷達 📡", key="t3_go") and matched_brs:
        st.session_state.t3_searched = True
        sd_s, ed_s = t3_sd.strftime('%Y-%m-%d'), t3_ed.strftime('%Y-%m-%d')
        c_param = "B" if '金額' in t3_u else "E"
        col_buy = '買進金額' if '金額' in t3_u else '買進張數'
        col_sell = '賣出金額' if '金額' in t3_u else '賣出張數'
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={sel_t3_hq_id}&b={sel_t3_br_id}&c={c_param}&e={sd_s}&f={ed_s}"
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
                
                if '嚴格' in t3_mode:
                    st.session_state.t3_buy_df = df_all[(df_all[col_buy] > 0) & (df_all[col_sell] == 0)].copy()
                    st.session_state.t3_sell_df = df_all[(df_all[col_sell] > 0) & (df_all[col_buy] == 0)].copy()
                else:
                    st.session_state.t3_buy_df = df_all[df_all['買%'] >= t3_p].copy()
                    st.session_state.t3_sell_df = df_all[df_all['賣%'] >= t3_p].copy()
            else: st.warning("無資料。")
        except Exception as e: st.error(f"發生錯誤: {e}")

    if st.session_state.t3_searched:
        def display_table_with_button_t3(df_to_show, key_prefix):
            if not df_to_show.empty:
                df_show = df_to_show.copy()
                df_show['extracted_stock_id'] = df_show['股票名稱'].apply(get_stock_id)
                df_show['K線圖'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else "")
                df_show['分點明細'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_t3_br_id}&b={sel_t3_br_id}&C=3" if sid else "")
                df_show['畫圖'] = False 
                col_b = '買進金額' if '金額' in t3_u else '買進張數'
                col_s = '賣出金額' if '金額' in t3_u else '賣出張數'
                df_show = df_show[['畫圖', '股票名稱', 'K線圖', col_b, col_s, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                col_config = {
                    "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈"),
                    "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦"),
                    "畫圖": st.column_config.CheckboxColumn("送至Tab4"),
                    "extracted_stock_id": None 
                }
                
                # ✅ 【還原修正】精準攔截繪圖勾選狀態
                editor_key = f"editor_{key_prefix}"
                st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=editor_key)
                
                if editor_key in st.session_state:
                    edited_rows = st.session_state[editor_key].get('edited_rows', {})
                    for row_idx, edits in edited_rows.items():
                        if edits.get('畫圖', False) == True:
                            sid_clicked = df_show.iloc[row_idx]['extracted_stock_id']
                            del st.session_state[editor_key]
                            send_to_tab4(sid_clicked, sel_t3_br_l)
                            st.success(f"✅ 參數已設定！請點擊上方「📊 主力 K 線圖」。")
                            break

        col_b = '買進金額' if '金額' in t3_u else '買進張數'
        col_s = '賣出金額' if '金額' in t3_u else '賣出張數'
        st.markdown(f"### 🔴 該分點吃貨中 - 共 {len(st.session_state.t3_buy_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_buy_df.sort_values(by=col_b, ascending=False).head(999 if show_full_t3 else 10), "t3_buy")
        st.markdown(f"### 🟢 該分點倒貨中 - 共 {len(st.session_state.t3_sell_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_sell_df.sort_values(by=col_s, ascending=False).head(999 if show_full_t3 else 10), "t3_sell")

# --- Tab 4 (專業繪圖 - 回歸 Plotly 並強化手機體驗) ---
with tab4:
    col1, col2, col3, col4 = st.columns([1, 1.5, 1, 1])
    with col1:
        # 強制綁定 Session State
        t4_sid = st.text_input("股票代號", key="t4_sid_ui")
    with col2:
        all_br_names = sorted(list(BROKER_MAP.keys()))
        t4_br_name = st.selectbox("搜尋分點", all_br_names, key="t4_br_ui")
    with col3:
        st.write("") 
        draw_btn = st.button("🎨 繪製專業圖表", use_container_width=True)
    with col4:
        st.write("")
        fav_btn = st.button("❤️ 加入暫存清單", use_container_width=True)

    t4_sid_clean = t4_sid.strip().upper()
    
    if fav_btn:
        entry = {"股票代號": t4_sid_clean, "追蹤分點": t4_br_name}
        if entry not in st.session_state.watchlist:
            st.session_state.watchlist.append(entry)
            st.success(f"✅ 已加入暫存清單！")
        else:
            st.warning("⚠️ 已在清單中。")

    with st.expander("⚙️ 圖表設定與手機水平線工具", expanded=False):
        hline_val = st.number_input("📏 手機畫線救星：輸入價格後圖表即會產生精準水平線", value=0.0, step=1.0)
        tc1, tc2 = st.columns([1,2])
        with tc1: 
            t4_days = st.number_input("顯示最近幾根K棒?", value=200, min_value=10, max_value=1000)
            t4_period = st.radio("K線週期", ["日", "週", "月"], horizontal=True)
        with tc2:
            st.markdown("**布林通道**")
            bb_w = st.number_input("週期", value=52)
            bb_std = st.number_input("標準差", value=2.0, step=0.1)
        st.markdown("---")
        sc1, sc2 = st.columns(2)
        with sc1: 
            st.markdown("**短線 MACD**")
            macd1_f = st.number_input("快線", value=12, key="m1f")
            macd1_s = st.number_input("慢線", value=26, key="m1s")
            macd1_sig = st.number_input("訊號", value=9, key="m1sig")
        with sc2: 
            st.markdown("**長線 MACD**")
            macd2_f = st.number_input("快線", value=26, key="m2f")
            macd2_s = st.number_input("慢線", value=52, key="m2s")
            macd2_sig = st.number_input("訊號", value=18, key="m2sig")

    if st.session_state.watchlist:
        with st.expander("⭐ 暫存主力清單", expanded=True):
            wl_df = pd.DataFrame(st.session_state.watchlist)
            wl_df.insert(0, '載入', False)
            wl_df['刪除'] = False
            wl_config = {"載入": st.column_config.CheckboxColumn("載入繪圖"), "刪除": st.column_config.CheckboxColumn("刪除")}
            edited_wl = st.data_editor(wl_df, hide_index=True, column_config=wl_config, use_container_width=True, key="wl_editor")
            
            if not edited_wl[edited_wl['載入'] == True].empty:
                send_to_tab4(edited_wl[edited_wl['載入'] == True].iloc[0]['股票代號'], edited_wl[edited_wl['載入'] == True].iloc[0]['追蹤分點'])
                st.rerun() 
            if not edited_wl[edited_wl['刪除'] == True].empty:
                del_sid = edited_wl[edited_wl['刪除'] == True].iloc[0]['股票代號']
                del_br = edited_wl[edited_wl['刪除'] == True].iloc[0]['追蹤分點']
                st.session_state.watchlist = [item for item in st.session_state.watchlist if not (item['股票代號'] == del_sid and item['追蹤分點'] == del_br)]
                st.rerun()

    # 執行繪圖 (點擊按鈕或從其他分頁傳遞參數)
    if draw_btn or st.session_state.auto_draw:
        st.session_state.auto_draw = False 
        t4_br_id = BROKER_MAP[t4_br_name]['br_id']
        
        with st.spinner(f"繪製 {t4_sid_clean} 中..."):
            try:
                df_k = get_stock_kline(t4_sid_clean)
                if df_k.empty: st.error("找不到 K 線資料。")
                else:
                    df_broker = get_fubon_history(t4_sid_clean, t4_br_id)
                    if df_broker.empty: st.info("近期無交易紀錄。")

                    df_merged = pd.merge(df_k, df_broker[['Date', '買賣超']], on='Date', how='left')
                    df_merged['買賣超'] = df_merged['買賣超'].fillna(0) 
                    
                    df_merged.set_index('Date', inplace=True)
                    if t4_period == "週": df_resampled = df_merged.resample('W-FRI').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', '買賣超':'sum'})
                    elif t4_period == "月": df_resampled = df_merged.resample('M').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', '買賣超':'sum'})
                    else: df_resampled = df_merged.copy()
                    
                    df_resampled = df_resampled.dropna(subset=['Close']).reset_index()
                    df_resampled['Date_str'] = df_resampled['Date'].dt.strftime('%Y-%m-%d') 

                    df_resampled['BB_mid'] = df_resampled['Close'].rolling(int(bb_w)).mean()
                    df_resampled['BB_std'] = df_resampled['Close'].rolling(int(bb_w)).std()
                    df_resampled['BB_up'] = df_resampled['BB_mid'] + bb_std * df_resampled['BB_std']
                    df_resampled['BB_dn'] = df_resampled['BB_mid'] - bb_std * df_resampled['BB_std']
                    
                    macd1, sig1, hist1 = calculate_macd(df_resampled, int(macd1_f), int(macd1_s), int(macd1_sig))
                    macd2, sig2, hist2 = calculate_macd(df_resampled, int(macd2_f), int(macd2_s), int(macd2_sig))
                    
                    df_plot = df_resampled.tail(int(t4_days)).copy()
                    
                    fig = make_subplots(
                        rows=4, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, row_heights=[0.5, 0.2, 0.15, 0.15],
                        subplot_titles=("K線與布林通道", f"分點買賣超 ({t4_br_name})", "MACD (短線)", "MACD (長線)")
                    )
                    
                    colors_k = ['#FF3333' if close >= open else '#00AA00' for close, open in zip(df_plot['Close'], df_plot['Open'])]
                    colors_vol = ['#FF3333' if val >= 0 else '#00AA00' for val in df_plot['買賣超']]

                    # 終極清晰黑底白字 Hover (移除 T00:00:00)
                    custom_hover = "<b>%{x}</b><br><br><b>開盤: %{open:.2f}</b><br><b>最高: %{high:.2f}</b><br><b>最低: %{low:.2f}</b><br><b>收盤: %{close:.2f}</b><extra></extra>"

                    fig.add_trace(go.Candlestick(
                        x=df_plot['Date_str'], open=df_plot['Open'], high=df_plot['High'],
                        low=df_plot['Low'], close=df_plot['Close'], name='K線',
                        increasing_line_color='#FF3333', decreasing_line_color='#00AA00',
                        hovertemplate=custom_hover
                    ), row=1, col=1)
                    
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=df_plot['BB_mid'], line=dict(color='rgba(255, 255, 255, 0.4)', width=1), name='BB中軌', hoverinfo='skip'), row=1, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=df_plot['BB_up'], line=dict(color='rgba(173, 216, 230, 0.5)', width=1, dash='dot'), name='BB上軌', hoverinfo='skip'), row=1, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=df_plot['BB_dn'], fill='tonexty', fillcolor='rgba(173, 216, 230, 0.1)', line=dict(color='rgba(173, 216, 230, 0.5)', width=1, dash='dot'), name='BB下軌', hoverinfo='skip'), row=1, col=1)

                    fig.add_trace(go.Bar(x=df_plot['Date_str'], y=df_plot['買賣超'], marker_color=colors_vol, hovertemplate="<b>%{y} 張</b><extra></extra>"), row=2, col=1)

                    fig.add_trace(go.Bar(x=df_plot['Date_str'], y=hist1.tail(int(t4_days)), marker_color=['#FF3333' if val >= 0 else '#00AA00' for val in hist1.tail(int(t4_days))], hovertemplate="<b>%{y:.2f}</b><extra></extra>"), row=3, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=macd1.tail(int(t4_days)), line=dict(color='yellow', width=1), hoverinfo='skip'), row=3, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=sig1.tail(int(t4_days)), line=dict(color='cyan', width=1), hoverinfo='skip'), row=3, col=1)

                    fig.add_trace(go.Bar(x=df_plot['Date_str'], y=hist2.tail(int(t4_days)), marker_color=['#FF3333' if val >= 0 else '#00AA00' for val in hist2.tail(int(t4_days))], hovertemplate="<b>%{y:.2f}</b><extra></extra>"), row=4, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=macd2.tail(int(t4_days)), line=dict(color='yellow', width=1), hoverinfo='skip'), row=4, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=sig2.tail(int(t4_days)), line=dict(color='cyan', width=1), hoverinfo='skip'), row=4, col=1)

                    # 畫入使用者指定的手動水平線
                    if hline_val > 0:
                        fig.add_hline(y=hline_val, line_dash="dash", line_color="#2962FF", annotation_text=str(hline_val), annotation_font_color="white", row=1, col=1)

                    fig.update_layout(
                        height=900, margin=dict(l=10, r=10, t=30, b=10),
                        plot_bgcolor='#131722', paper_bgcolor='#131722', font=dict(color='#d1d4dc'),
                        showlegend=False, xaxis_rangeslider_visible=False,
                        # 強制黑底白字高對比
                        hoverlabel=dict(bgcolor="#111111", font=dict(color="white", size=15), bordercolor="#666666"),
                        # 保留並增強滑鼠繪圖工具
                        modebar_add=['drawline', 'drawhline', 'drawrect', 'drawcircle', 'eraseshape'],
                        dragmode='pan'
                    )
                    fig.update_xaxes(showgrid=False, zeroline=False, type='category', showspikes=True, spikemode='across', spikethickness=1, spikedash='dot', spikecolor='#777777') 
                    fig.update_yaxes(showgrid=False, zeroline=False)

                    st.plotly_chart(fig, use_container_width=True, config={
                        'displayModeBar': True, 
                        'scrollZoom': True,
                        'displaylogo': False,
                        'modeBarButtonsToRemove': ['lasso2d', 'select2d'] # 移除容易在手機誤觸的框選放大
                    })

            except Exception as e: st.error(f"繪圖發生錯誤: {e}")
