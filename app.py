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

if 't4_sid_ui' not in st.session_state: st.session_state.t4_sid_ui = "6488"
if 't4_br_ui' not in st.session_state: st.session_state.t4_br_ui = "兆豐-忠孝"
if 'auto_draw' not in st.session_state: st.session_state.auto_draw = False
if 'watchlist' not in st.session_state: st.session_state.watchlist = []

def send_to_tab4(sid, br_name):
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
                for tb in tables:
                    if tb.shape[1] == 10:
                        l = tb.iloc[:,[0,1,2]].copy(); l.columns=['券商','買','賣']
                        r = tb.iloc[:,[5,6,7]].copy(); r.columns=['券商','買','賣']
                        df_all = pd.concat([l, r])
                
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
                edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=f"editor_{key_prefix}")
                clicked_rows = edited_df[edited_df['畫圖'] == True]
                if not clicked_rows.empty:
                    br_clicked = clicked_rows.iloc[0]['券商']
                    send_to_tab4(t2_sid_clean, br_clicked)
                    st.success(f"✅ 參數已設定！請點擊上方「📊 主力 K 線圖」。")

        st.subheader("🔴 吃貨主力分點")
        display_table_with_button_t2(st.session_state.t2_buy_df.sort_values('買', ascending=False).head(999 if show_full_t2 else 10), "t2_buy")
        st.subheader("🟢 倒貨主力分點")
        display_table_with_button_t2(st.session_state.t2_sell_df.sort_values('賣', ascending=False).head(999 if show_full_t2 else 10), "t2_sell")

# --- Tab 3 ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        sorted_loc_keys = sorted(GEO_MAP.keys())
        sel_loc = st.selectbox("選擇地緣關鍵字", sorted_loc_keys, key="t3_loc_sel")
    with c2:
        loc_branches = GEO_MAP[sel_loc]
        sel_t3_br_l = st.selectbox("選擇特定分點", sorted(loc_branches.keys()), key="t3_br_sel")
        sel_t3_br_id = loc_branches[sel_t3_br_l]['br_id']
        sel_t3_hq_id = loc_branches[sel_t3_br_l]['hq_id']

    c3, c4, c5 = st.columns(3)
    with c3: t3_sd = st.date_input("區間起點", datetime.date.today()-datetime.timedelta(days=7), key="t3_sd")
    with c4: t3_ed = st.date_input("區間終點", datetime.date.today(), key="t3_ed")
    with c5: t3_u = st.radio("單位", ["張數", "金額"], horizontal=True, key="t3_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t3_mode = st.radio("篩選", ["嚴格", "濾網"], index=1, horizontal=True, key="t3_mode")
    with c7: t3_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, key="t3_pct")
    with c8: st.write(""); show_full_t3 = st.checkbox("完整清單", value=False, key="t3_full")

    if st.button("啟動地緣雷達 📡", key="t3_go"):
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
                edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=f"editor_{key_prefix}")
                clicked_rows = edited_df[edited_df['畫圖'] == True]
                if not clicked_rows.empty:
                    sid_clicked = clicked_rows.iloc[0]['extracted_stock_id']
                    send_to_tab4(sid_clicked, sel_t3_br_l)
                    st.success(f"✅ 參數已設定！請點擊上方「📊 主力 K 線圖」。")

        col_b = '買進金額' if '金額' in t3_u else '買進張數'
        col_s = '賣出金額' if '金額' in t3_u else '賣出張數'
        st.markdown(f"### 🔴 該分點吃貨中 - 共 {len(st.session_state.t3_buy_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_buy_df.sort_values(by=col_b, ascending=False).head(999 if show_full_t3 else 10), "t3_buy")
        st.markdown(f"### 🟢 該分點倒貨中 - 共 {len(st.session_state.t3_sell_df)} 檔")
        display_table_with_button_t3(st.session_state.t3_sell_df.sort_values(by=col_s, ascending=False).head(999 if show_full_t3 else 10), "t3_sell")

# --- Tab 4 (TradingView 輕量版 旗艦升級) ---
with tab4:
    col1, col2, col3, col4, col5 = st.columns([1, 1.5, 0.5, 0.5, 1])
    with col1:
        t4_sid = st.text_input("股票代號", st.session_state.t4_sid_ui, key="t4_sid_input")
    with col2:
        all_br_names = sorted(list(BROKER_MAP.keys()))
        passed_br = st.session_state.t4_br_ui
        default_br_idx = all_br_names.index(passed_br) if passed_br in all_br_names else 0
        t4_br_name = st.selectbox("搜尋分點", all_br_names, index=default_br_idx, key="t4_br_input")
    with col3: 
        t4_period = st.radio("週期", ["日", "週", "月"], horizontal=False)
    with col4:
        st.write("") 
        draw_btn = st.button("🎨 繪圖", use_container_width=True)
    with col5:
        st.write("")
        fav_btn = st.button("❤️ 存入清單", use_container_width=True)

    t4_sid_clean = t4_sid.strip().upper()
    
    if fav_btn:
        entry = {"股票代號": t4_sid_clean, "追蹤分點": t4_br_name}
        if entry not in st.session_state.watchlist:
            st.session_state.watchlist.append(entry)
            st.success(f"✅ 已加入暫存清單！")
        else:
            st.warning("⚠️ 已在清單中。")

    with st.expander("⚙️ 圖表設定與手機專用畫線工具", expanded=False):
        hline_val = st.number_input("📏 新增水平線 (輸入價格後按 Enter 即畫線)", value=0.0, step=1.0)
        tc1, tc2 = st.columns([1,2])
        with tc1: 
            t4_days = st.number_input("近期K棒數", value=150, min_value=10, max_value=1000)
        with tc2:
            st.markdown("**布林通道**")
            c_bb1, c_bb2 = st.columns(2)
            with c_bb1: bb_w = st.number_input("週期", value=52)
            with c_bb2: bb_std = st.number_input("標準差", value=2.0, step=0.1)
        st.markdown("---")
        sc1, sc2 = st.columns(2)
        with sc1: 
            st.markdown("**短線 MACD**")
            c_m11, c_m12, c_m13 = st.columns(3)
            with c_m11: macd1_f = st.number_input("快", value=12, key="m1f")
            with c_m12: macd1_s = st.number_input("慢", value=26, key="m1s")
            with c_m13: macd1_sig = st.number_input("訊號", value=9, key="m1sig")
        with sc2: 
            st.markdown("**長線 MACD**")
            c_m21, c_m22, c_m23 = st.columns(3)
            with c_m21: macd2_f = st.number_input("快", value=26, key="m2f")
            with c_m22: macd2_s = st.number_input("慢", value=52, key="m2s")
            with c_m23: macd2_sig = st.number_input("訊號", value=18, key="m2sig")

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

    if draw_btn or st.session_state.auto_draw:
        st.session_state.auto_draw = False 
        t4_br_id = BROKER_MAP[t4_br_name]['br_id']
        
        with st.spinner(f"為您繪製 {t4_sid_clean} 中..."):
            try:
                df_k = get_stock_kline(t4_sid_clean)
                if df_k.empty: st.error("找不到 K 線資料。")
                else:
                    df_broker = get_fubon_history(t4_sid_clean, t4_br_id)
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
                    
                    # 🚀 嚴格濾除 NaN 毒藥，確保 JS 絕對不崩潰
                    candle_data, volume_data, bb_mid_data, bb_up_data, bb_dn_data = [], [], [], [], []
                    for i, row in df_plot.iterrows():
                        time_str = row['Date_str']
                        candle_data.append({"time": time_str, "open": safe_float(row['Open']), "high": safe_float(row['High']), "low": safe_float(row['Low']), "close": safe_float(row['Close'])})
                        volume_data.append({"time": time_str, "value": safe_float(row['買賣超']), "color": '#ef5350' if row['買賣超'] >= 0 else '#26a69a'})
                        if not pd.isna(row['BB_mid']): bb_mid_data.append({"time": time_str, "value": safe_float(row['BB_mid'])})
                        if not pd.isna(row['BB_up']): bb_up_data.append({"time": time_str, "value": safe_float(row['BB_up'])})
                        if not pd.isna(row['BB_dn']): bb_dn_data.append({"time": time_str, "value": safe_float(row['BB_dn'])})

                    def extract_indicator(series, color_logic=False):
                        res = []
                        for t, v in zip(df_plot['Date_str'], series.tail(int(t4_days))):
                            if not pd.isna(v):
                                item = {"time": t, "value": float(v)}
                                if color_logic: item["color"] = '#ef5350' if v >= 0 else '#26a69a'
                                res.append(item)
                        return res

                    h1_data, m1_data, s1_data = extract_indicator(hist1, True), extract_indicator(macd1), extract_indicator(sig1)
                    h2_data, m2_data, s2_data = extract_indicator(hist2, True), extract_indicator(macd2), extract_indicator(sig2)

                    hline_code = f"""
                        const hline = chartMain.addLineSeries({{ color: '#2962FF', lineWidth: 2, lineStyle: 1, crosshairMarkerVisible: false, priceLineVisible: true, lastValueVisible: false }});
                        hline.setData(candleData.map(d => ({{time: d.time, value: {hline_val}}})));
                    """ if hline_val > 0 else ""

                    html_code = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
                        <script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
                        <style>
                            body {{ margin: 0; padding: 0; background-color: #131722; overflow: hidden; font-family: "Microsoft JhengHei", sans-serif; }}
                            #chart-container {{ width: 100vw; height: 95vh; display: flex; flex-direction: column; position: relative; }}
                            .chart-pane {{ width: 100%; position: relative; border-bottom: 1px solid #2b2b43; }}
                            #pane-main {{ flex: 4; }} #pane-macd1, #pane-macd2 {{ flex: 1.2; }}
                            
                            /* 專業浮動視窗 CSS */
                            .floating-tooltip {{
                                position: absolute; display: none; padding: 10px; box-sizing: border-box;
                                font-size: 13px; color: #d1d4dc; background-color: rgba(30, 34, 45, 0.95);
                                border: 1px solid #2962FF; border-radius: 6px; pointer-events: none; z-index: 1000;
                                top: 10px; left: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                            }}
                            .tt-title {{ color: #2962FF; font-weight: bold; margin-bottom: 6px; font-size: 14px; border-bottom: 1px solid #444; padding-bottom: 4px; }}
                            .tt-row {{ display: flex; justify-content: space-between; margin-bottom: 2px; width: 140px; }}
                            .tt-label {{ color: #a0a3ab; }}
                            .tt-vol {{ margin-top: 6px; padding-top: 6px; border-top: 1px dashed #555; font-size: 14px; }}
                            
                            .tv-legend {{ position: absolute; left: 10px; top: 5px; z-index: 100; font-size: 12px; color: #d1d4dc; pointer-events: none; }}
                        </style>
                    </head>
                    <body>
                        <div id="chart-container">
                            <div id="pane-main" class="chart-pane">
                                <div id="main-tooltip" class="floating-tooltip"></div>
                            </div>
                            <div id="pane-macd1" class="chart-pane"><div id="legend-macd1" class="tv-legend"></div></div>
                            <div id="pane-macd2" class="chart-pane"><div id="legend-macd2" class="tv-legend"></div></div>
                        </div>
                        <script>
                            const candleData = {json.dumps(candle_data)};
                            const volumeData = {json.dumps(volume_data)};
                            const bbMidData = {json.dumps(bb_mid_data)};
                            const bbUpData = {json.dumps(bb_up_data)};
                            const bbDnData = {json.dumps(bb_dn_data)};
                            const h1Data = {json.dumps(h1_data)}; const m1Data = {json.dumps(m1_data)}; const s1Data = {json.dumps(s1_data)};
                            const h2Data = {json.dumps(h2_data)}; const m2Data = {json.dumps(m2_data)}; const s2Data = {json.dumps(s2_data)};

                            const layoutOptions = {{ layout: {{ backgroundColor: '#131722', textColor: '#d1d4dc' }}, grid: {{ vertLines: {{ color: '#242733' }}, horzLines: {{ color: '#242733' }} }}, crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }} }};
                            
                            // 主圖 (K線 + 買賣超 + BB)
                            const chartMain = LightweightCharts.createChart(document.getElementById('pane-main'), {{ ...layoutOptions, timeScale: {{ visible: false }} }});
                            const seriesK = chartMain.addCandlestickSeries({{ upColor: '#ef5350', downColor: '#26a69a', borderVisible: false, wickUpColor: '#ef5350', wickDownColor: '#26a69a' }});
                            seriesK.setData(candleData);
                            
                            const bbMid = chartMain.addLineSeries({{ color: '#FFEB3B', lineWidth: 1, crosshairMarkerVisible: false }});
                            bbMid.setData(bbMidData);
                            const bbUp = chartMain.addLineSeries({{ color: 'rgba(255, 255, 255, 0.4)', lineWidth: 1, lineStyle: 2, crosshairMarkerVisible: false }});
                            bbUp.setData(bbUpData);
                            const bbDn = chartMain.addLineSeries({{ color: 'rgba(255, 255, 255, 0.4)', lineWidth: 1, lineStyle: 2, crosshairMarkerVisible: false }});
                            bbDn.setData(bbDnData);
                            
                            const seriesVol = chartMain.addHistogramSeries({{ priceFormat: {{ type: 'volume' }}, priceScaleId: '', scaleMargins: {{ top: 0.8, bottom: 0 }} }});
                            seriesVol.setData(volumeData);
                            {hline_code}

                            // 副圖 1 (MACD 短線)
                            const chartM1 = LightweightCharts.createChart(document.getElementById('pane-macd1'), {{ ...layoutOptions, timeScale: {{ visible: false }} }});
                            const seriesH1 = chartM1.addHistogramSeries({{ priceFormat: {{ type: 'volume' }} }});
                            seriesH1.setData(h1Data);
                            const seriesMacd1 = chartM1.addLineSeries({{ color: '#FFD600', lineWidth: 1, crosshairMarkerVisible: false }});
                            seriesMacd1.setData(m1Data);
                            const seriesSig1 = chartM1.addLineSeries({{ color: '#00E676', lineWidth: 1, crosshairMarkerVisible: false }});
                            seriesSig1.setData(s1Data);

                            // 副圖 2 (MACD 長線)
                            const chartM2 = LightweightCharts.createChart(document.getElementById('pane-macd2'), {{ ...layoutOptions, timeScale: {{ borderColor: '#363c4e', rightOffset: 5 }} }});
                            const seriesH2 = chartM2.addHistogramSeries({{ priceFormat: {{ type: 'volume' }} }});
                            seriesH2.setData(h2Data);
                            const seriesMacd2 = chartM2.addLineSeries({{ color: '#FFD600', lineWidth: 1, crosshairMarkerVisible: false }});
                            seriesMacd2.setData(m2Data);
                            const seriesSig2 = chartM2.addLineSeries({{ color: '#00E676', lineWidth: 1, crosshairMarkerVisible: false }});
                            seriesSig2.setData(s2Data);

                            // 時間軸同步
                            const syncTime = (source, targets) => {{ source.timeScale().subscribeVisibleLogicalRangeChange(range => {{ if(range) targets.forEach(t => t.timeScale().setVisibleLogicalRange(range)); }}); }};
                            syncTime(chartMain, [chartM1, chartM2]); syncTime(chartM1, [chartMain, chartM2]); syncTime(chartM2, [chartMain, chartM1]);

                            // 完美浮動資料窗與副圖 Legend
                            const mainTooltip = document.getElementById('main-tooltip');
                            const legM1 = document.getElementById('legend-macd1'), legM2 = document.getElementById('legend-macd2');
                            
                            const getDayOfWeek = (dateStr) => {{
                                const days = ['週日', '週一', '週二', '週三', '週四', '週五', '週六'];
                                return days[new Date(dateStr).getDay()];
                            }};

                            const syncCrosshair = (source, targets, seriesMap) => {{
                                source.subscribeCrosshairMove(param => {{
                                    targets.forEach(t => {{ if(param.time) t.setCrosshairPosition(param.seriesPrices.get(seriesMap.get(source)), param.time, source); else t.clearCrosshairPosition(); }});
                                    
                                    if(!param.time || param.point.x < 0) {{
                                        mainTooltip.style.display = 'none';
                                        legM1.innerHTML = ''; legM2.innerHTML = '';
                                        return;
                                    }}
                                    
                                    // 更新主圖浮動視窗
                                    const dK = param.seriesPrices.get(seriesK), dV = param.seriesPrices.get(seriesVol);
                                    if(dK) {{
                                        mainTooltip.style.display = 'block';
                                        const dow = getDayOfWeek(param.time);
                                        const vText = dV !== undefined ? dV : 0;
                                        const vColor = vText >= 0 ? '#ef5350' : '#26a69a';
                                        
                                        mainTooltip.innerHTML = `
                                            <div class="tt-title">{t4_sid_clean} | ${{param.time}} ${{dow}}</div>
                                            <div class="tt-row"><span class="tt-label">開盤</span><span style="color:white;">${{dK.open.toFixed(2)}}</span></div>
                                            <div class="tt-row"><span class="tt-label">最高</span><span style="color:#ef5350;">${{dK.high.toFixed(2)}}</span></div>
                                            <div class="tt-row"><span class="tt-label">最低</span><span style="color:#26a69a;">${{dK.low.toFixed(2)}}</span></div>
                                            <div class="tt-row"><span class="tt-label">收盤</span><span style="color:white;font-weight:bold;">${{dK.close.toFixed(2)}}</span></div>
                                            <div class="tt-vol"><span class="tt-label">分點買賣超</span> <b style="color:${{vColor}}; float:right;">${{vText}} 張</b></div>
                                        `;
                                    }}
                                    
                                    // 更新副圖 Legend
                                    const h1 = param.seriesPrices.get(seriesH1), m1 = param.seriesPrices.get(seriesMacd1), s1 = param.seriesPrices.get(seriesSig1);
                                    if(m1 !== undefined) legM1.innerHTML = `<b>MACD (短線)</b> | 柱: <span style="color:${{h1>=0?'#ef5350':'#26a69a'}}">${{h1.toFixed(2)}}</span> | 快: <span style="color:#FFD600">${{m1.toFixed(2)}}</span> | 慢: <span style="color:#00E676">${{s1.toFixed(2)}}</span>`;

                                    const h2 = param.seriesPrices.get(seriesH2), m2 = param.seriesPrices.get(seriesMacd2), s2 = param.seriesPrices.get(seriesSig2);
                                    if(m2 !== undefined) legM2.innerHTML = `<b>MACD (長線)</b> | 柱: <span style="color:${{h2>=0?'#ef5350':'#26a69a'}}">${{h2.toFixed(2)}}</span> | 快: <span style="color:#FFD600">${{m2.toFixed(2)}}</span> | 慢: <span style="color:#00E676">${{s2.toFixed(2)}}</span>`;
                                }});
                            }};
                            
                            const sMap = new Map([[chartMain, seriesK], [chartM1, seriesMacd1], [chartM2, seriesMacd2]]);
                            syncCrosshair(chartMain, [chartM1, chartM2], sMap); syncCrosshair(chartM1, [chartMain, chartM2], sMap); syncCrosshair(chartM2, [chartMain, chartM1], sMap);

                            chartMain.timeScale().fitContent(); chartM1.timeScale().fitContent(); chartM2.timeScale().fitContent();
                            window.addEventListener('resize', () => {{ chartMain.applyOptions({{width: document.getElementById('pane-main').clientWidth}}); chartM1.applyOptions({{width: document.getElementById('pane-macd1').clientWidth}}); chartM2.applyOptions({{width: document.getElementById('pane-macd2').clientWidth}}); }});
                        </script>
                    </body>
                    </html>
                    """
                    components.html(html_code, height=850)
            except Exception as e: st.error(f"發生錯誤: {e}")
