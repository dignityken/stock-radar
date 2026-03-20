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
# 🔒 進入門檻：訂閱制期限密碼權限控管系統
# ==========================================
def check_password():
    """判斷使用者是否輸入了正確且未過期的密碼"""
    
    # 防呆：如果後台沒設定 Secrets，預設給一組到 2099 年都不會過期的密碼方便測試
    if "passwords" not in st.secrets:
        valid_passwords = {"測試帳號": "120001|2099-12-31"} 
    else:
        valid_passwords = st.secrets["passwords"]

    def password_entered():
        user_pwd_input = st.session_state["pwd_input"].strip()
        
        match_found = False
        is_expired = False
        
        # 尋找是否有吻合的密碼
        for user, auth_string in valid_passwords.items():
            # 解析密碼與日期，格式為 "密碼|2024-12-31"
            parts = str(auth_string).split("|")
            pwd = parts[0].strip()
            # 如果沒有設定直線|與日期，預設為無限期
            exp_date_str = parts[1].strip() if len(parts) > 1 else "2099-12-31" 
            
            if user_pwd_input == pwd:
                match_found = True
                # 檢查期限
                try:
                    exp_date = datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                    today = datetime.date.today()
                    if today > exp_date:
                        is_expired = True
                except ValueError:
                    pass # 若日期格式寫錯，容錯處理直接當作未過期
                break # 找到密碼就跳出迴圈

        # 根據驗證結果設定狀態
        if match_found and not is_expired:
            st.session_state["password_correct"] = True
            del st.session_state["pwd_input"]  # 安全考量，清空輸入框
        elif match_found and is_expired:
            st.session_state["password_correct"] = "expired"
        else:
            st.session_state["password_correct"] = False

    # 如果已經驗證成功，直接放行
    if st.session_state.get("password_correct") == True:
        return True

    # 登入畫面 UI
    st.markdown("<br><br><h1 style='text-align: center;'>🔒 籌碼雷達 - 專屬權限登入</h1>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.text_input(
            "請輸入您的專屬通行密碼：", 
            type="password", 
            on_change=password_entered, 
            key="pwd_input",
            placeholder="輸入後請按 Enter"
        )
        
        status = st.session_state.get("password_correct")
        if status == False:
            st.error("🚫 密碼錯誤，請重新輸入。")
        elif status == "expired":
            st.warning("⚠️ 您的會員權限已到期，請洽管理員。")
            
    return False

# 🛑 如果密碼沒過或是已過期，程式就在這裡停止，不執行後方的機密程式碼！
if not check_password():
    st.stop()  

# ==========================================
# 通過驗證後，開始執行主程式
# ==========================================

HEADERS = {"User-Agent": "Mozilla/5.0"} 

# --- Google Drive 文件連結 ---
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drivesdk"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drivesdk"

# --- 初始化 Session State (用於跨分頁、繪圖與追蹤清單) ---
if 'jump_sid' not in st.session_state: st.session_state.jump_sid = "6488"
if 'jump_br_name' not in st.session_state: st.session_state.jump_br_name = "兆豐-忠孝"
if 'auto_draw' not in st.session_state: st.session_state.auto_draw = False
if 't4_drawn' not in st.session_state: st.session_state.t4_drawn = False
if 'locked_sid' not in st.session_state: st.session_state.locked_sid = "6488"
if 'locked_br_id' not in st.session_state: st.session_state.locked_br_id = "0037003000300061"
if 'locked_br_name' not in st.session_state: st.session_state.locked_br_name = "兆豐-忠孝"
if 'watchlist' not in st.session_state: st.session_state.watchlist = []

# --- 函數：從 Google Drive 連結下載內容 ---
@st.cache_data(ttl=3600) 
def download_google_drive_file(url):
    file_id = url.split('/')[-2] 
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        response = requests.get(download_url, stream=True, verify=False, timeout=10)
        response.raise_for_status() 
        return response.text
    except requests.exceptions.RequestException as e:
        st.error(f"從Google Drive下載文件失敗: {e}")
        return None

# --- 載入數據 ---
@st.cache_data(ttl=3600) 
def load_hq_data(url):
    content = download_google_drive_file(url)
    if not content: return {}
    hq_data = {}
    for line in content.strip().split('\n'):
        if "\t" in line and not line.startswith("證券商代號"): 
            parts = line.split('\t')
            if len(parts) == 2:
                hq_data[parts[0].strip()] = parts[1].strip()
    return hq_data

@st.cache_data(ttl=3600) 
def load_branch_data(url):
    content = download_google_drive_file(url)
    if not content: return ""
    return content.strip().lstrip("'").rstrip("'")

HQ_DATA = load_hq_data(GOOGLE_DRIVE_HQ_DATA_URL)
FINAL_RAW_DATA_CLEANED = load_branch_data(GOOGLE_DRIVE_BRANCH_DATA_URL)

# ==========================================
# 0. 完整數據庫建置
# ==========================================
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
                br_id = br_id.strip()
                br_name = br_name_raw.replace("亚","亞").strip()
                
                if br_name not in branches_processed:
                    branches_processed[br_name] = br_id
                    name_map[br_name] = {"hq_id": bid, "br_id": br_id, "hq_name": final_bname}
                
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

# ==========================================
# 0.5 建置「地緣/關鍵字」字典 
# ==========================================
GEO_MAP = {}
for br_name, br_info in BROKER_MAP.items():
    if "-" in br_name:
        loc_name = br_name.split("-")[-1].replace("(停)", "").strip()
        if loc_name not in GEO_MAP:
            GEO_MAP[loc_name] = {}
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

# --- 輔助函數：計算 MACD ---
def calculate_macd(df, fast, slow, signal):
    exp1 = df['Close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['Close'].ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist

# --- 輔助函數：自動判斷上市/上櫃並抓取長歷史 K 線 ---
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
tab1, tab2, tab3, tab4 = st.tabs(["🚀 券商分點查股票", "📊 股票代號查分點", "📍 地緣券商尋寶", "📊 主力 K 線圖"])

# --- Tab 1 ---
with tab1:
    st.markdown("### 🏦 追蹤特定分點進出")
    c1, c2 = st.columns(2)
    with c1: 
        sorted_hq_keys = sorted(UI_TREE.keys())
        default_index = sorted_hq_keys.index('(牛牛牛)亞證券') if '(牛牛牛)亞證券' in sorted_hq_keys else 0
        sel_hq = st.selectbox("選擇券商", sorted_hq_keys, index=default_index, key="t1_b_sel")
    with c2: 
        b_opts = UI_TREE[sel_hq]['branches']
        sorted_br_keys = sorted(b_opts.keys())
        default_br_index = 0
        if '總公司' in sorted_br_keys: default_br_index = sorted_br_keys.index('總公司')
        elif sel_hq in sorted_br_keys: default_br_index = sorted_br_keys.index(sel_hq)
        sel_br_l = st.selectbox("選擇分點", sorted_br_keys, index=default_br_index, key="t1_br_sel")
        sel_br_id = b_opts[sel_br_l]

    c3, c4, c5 = st.columns(3)
    with c3: t1_sd = st.date_input("區間起點", datetime.date.today()-datetime.timedelta(days=7), key="t1_sd")
    with c4: t1_ed = st.date_input("區間終點", datetime.date.today(), key="t1_ed")
    with c5: t1_u = st.radio("統計單位", ["張數", "金額"], horizontal=True, key="t1_unit")

    c6, c7, c8 = st.columns([1.5, 1, 1])
    with c6: t1_mode = st.radio("篩選條件", ["嚴格模式 (只買不賣)", "濾網模式 (自訂佔比)"], index=1, horizontal=True, key="t1_mode")
    with c7: t1_p = st.number_input("佔比 >= (%)", 0.0, 100.0, 95.0, step=1.0, key="t1_pct")
    with c8: st.write(""); show_full = st.checkbox("顯示完整清單", value=False, key="t1_full")

    if st.button("開始分點尋寶 🚀", key="t1_go"):
        sd_s, ed_s = t1_sd.strftime('%Y-%m-%d'), t1_ed.strftime('%Y-%m-%d')
        bid_hq = UI_TREE[sel_hq]['bid'] 

        is_amount = '金額' in t1_u
        c_param = "B" if is_amount else "E"
        col_buy = '買進金額' if is_amount else '買進張數'
        col_sell = '賣出金額' if is_amount else '賣出張數'
        
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={bid_hq}&b={sel_br_id}&c={c_param}&e={sd_s}&f={ed_s}"
        
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

                if '嚴格' in t1_mode:
                    only_buy = df_all[(df_all[col_buy] > 0) & (df_all[col_sell] == 0)].copy()
                    only_sell = df_all[(df_all[col_sell] > 0) & (df_all[col_buy] == 0)].copy()
                else:
                    only_buy = df_all[df_all['買%'] >= t1_p].copy()
                    only_sell = df_all[df_all['賣%'] >= t1_p].copy()

                def display_table_with_button(df_to_show, key_prefix):
                    if not df_to_show.empty:
                        df_show = df_to_show.copy()
                        df_show['extracted_stock_id'] = df_show['股票名稱'].apply(get_stock_id)
                        df_show['K線圖'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else "")
                        df_show['分點明細'] = df_show['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_br_id}&b={sel_br_id}&C=3" if sid else "")
                        df_show['帶入K線'] = False 
                        
                        df_show = df_show[['帶入K線', '股票名稱', 'K線圖', col_buy, col_sell, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                        col_config = {
                            "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈", help="外連富邦K線"),
                            "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦", help="外連富邦明細"),
                            "帶入K線": st.column_config.CheckboxColumn("送至 Tab4 繪圖", help="打勾後請手動切換至分頁四"),
                            "extracted_stock_id": None 
                        }
                        edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=f"editor_{key_prefix}")
                        clicked_rows = edited_df[edited_df['帶入K線'] == True]
                        if not clicked_rows.empty:
                            sid_clicked = clicked_rows.iloc[0]['extracted_stock_id']
                            st.session_state.jump_sid = sid_clicked
                            st.session_state.jump_br_name = sel_br_l
                            st.session_state.auto_draw = True
                            st.success(f"✅ 已將 {sid_clicked} 與 {sel_br_l} 參數送到 Tab4！請在最上方手動點擊「📊 主力 K 線圖」分頁。")

                st.subheader(f"🕵️ 分點尋寶結果：{sel_hq} - {sel_br_l}")
                st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t1_u}")

                st.markdown(f"### 🔴 大戶吃貨中 (極端買進) - 共 {len(only_buy)} 檔")
                display_table_with_button(only_buy.sort_values(by=col_buy, ascending=False).head(999 if show_full else 10), "t1_buy")
                st.markdown(f"### 🟢 大戶倒貨中 (極端賣出) - 共 {len(only_sell)} 檔")
                display_table_with_button(only_sell.sort_values(by=col_sell, ascending=False).head(999 if show_full else 10), "t1_sell")

            else: st.warning("抓取不到數據。請檢查股票代號或券商分點是否正確。")
        except requests.exceptions.Timeout: st.error("請求超時，請稍後再試。")
        except requests.exceptions.RequestException as e: st.error(f"網絡請求錯誤: {e}")
        except Exception as e: st.error(f"發生錯誤: {e}")

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
                        df_all = pd.concat([l, r])
                
                if not df_all.empty:
                    df_all = df_all.dropna()
                    df_all = df_all[~df_all['券商'].str.contains('券商|合計|平均|說明|註', na=False)]
                    for c in ['買','賣']: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                    df_all['合計'] = df_all['買'] + df_all['賣']
                    df_all['買進%'] = (df_all['買']/df_all['合計']*100).round(1)
                    df_all['賣出%'] = (df_all['賣']/df_all['合計']*100).round(1)

                    if t2_m == "嚴格模式":
                        b_df = df_all[(df_all['買'] >= t2_v) & (df_all['賣'] == 0)].copy()
                        s_df = df_all[(df_all['賣'] >= t2_v) & (df_all['買'] == 0)].copy()
                    else:
                        b_df = df_all[(df_all['買進%'] >= t2_p) & (df_all['買'] >= t2_v)].copy()
                        s_df = df_all[(df_all['賣出%'] >= t2_p) & (df_all['賣'] >= t2_v)].copy()

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
                            edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=f"editor_{key_prefix}")
                            clicked_rows = edited_df[edited_df['送至 Tab4 繪圖'] == True]
                            if not clicked_rows.empty:
                                br_clicked = clicked_rows.iloc[0]['券商']
                                st.session_state.jump_sid = t2_sid_clean
                                st.session_state.jump_br_name = br_clicked
                                st.session_state.auto_draw = True
                                st.success(f"✅ 已將 {t2_sid_clean} 與 {br_clicked} 參數送到 Tab4！請在最上方手動點擊「📊 主力 K 線圖」分頁。")

                    st.subheader("🔴 吃貨主力分點")
                    display_table_with_button_t2(b_df.sort_values('買', ascending=False).head(999 if show_full_t2 else 10), "t2_buy")
                    st.subheader("🟢 倒貨主力分點")
                    display_table_with_button_t2(s_df.sort_values('賣', ascending=False).head(999 if show_full_t2 else 10), "t2_sell")

                else: st.warning("未找到資料。請檢查股票代號或日期區間。")
            except Exception as e: st.error(f"發生錯誤: {e}")

# --- Tab 3 (地緣券商尋寶) ---
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
        sel_t3_br_l = st.selectbox("選擇該區特定分點", sorted_loc_br_keys, key="t3_br_sel")
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
                    only_buy = df_all[(df_all[col_buy] > 0) & (df_all[col_sell] == 0)].copy()
                    only_sell = df_all[(df_all[col_sell] > 0) & (df_all[col_buy] == 0)].copy()
                else:
                    only_buy = df_all[df_all['買%'] >= t3_p].copy()
                    only_sell = df_all[df_all['賣%'] >= t3_p].copy()
                
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
                        edited_df = st.data_editor(df_show, hide_index=True, column_config=col_config, use_container_width=True, key=f"editor_{key_prefix}")
                        clicked_rows = edited_df[edited_df['帶入K線'] == True]
                        if not clicked_rows.empty:
                            sid_clicked = clicked_rows.iloc[0]['extracted_stock_id']
                            st.session_state.jump_sid = sid_clicked
                            st.session_state.jump_br_name = sel_t3_br_l
                            st.session_state.auto_draw = True
                            st.success(f"✅ 已將 {sid_clicked} 與 {sel_t3_br_l} 參數送到 Tab4！請手動切換至「📊 主力 K 線圖」。")

                st.subheader(f"🕵️ 地緣雷達結果：{sel_t3_br_l}")
                st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t3_u}")
                st.markdown(f"### 🔴 該分點吃貨中 (極端買進) - 共 {len(only_buy)} 檔")
                display_table_with_button_t3(only_buy.sort_values(by=col_buy, ascending=False).head(999 if show_full_t3 else 10), "t3_buy")
                st.markdown(f"### 🟢 該分點倒貨中 (極端賣出) - 共 {len(only_sell)} 檔")
                display_table_with_button_t3(only_sell.sort_values(by=col_sell, ascending=False).head(999 if show_full_t3 else 10), "t3_sell")
            else: st.warning("抓取不到數據。請檢查股票代號或券商分點是否正確。")
        except Exception as e: st.error(f"發生錯誤: {e}")

# --- Tab 4 (專業主力 K 線圖) ---
with tab4:
    st.markdown("### 📊 K 線與分點進出圖")
    st.caption("支援歷史回溯、多週期切換與主力追蹤清單。")
    
    col1, col2, col3, col4, col5 = st.columns([1, 1.5, 1, 1, 1])
    with col1:
        t4_sid = st.text_input("股票代號", st.session_state.jump_sid, key="t4_sid")
    with col2:
        all_br_names = sorted(list(BROKER_MAP.keys()))
        passed_br = st.session_state.jump_br_name.replace("亚","亞").strip()
        default_br_idx = all_br_names.index(passed_br) if passed_br in all_br_names else 0
        t4_br_name = st.selectbox("搜尋分點", all_br_names, index=default_br_idx, key="t4_br")
    with col3:
        t4_period = st.radio("K線週期", ["日", "週", "月"], horizontal=True, key="t4_period")
    with col4:
        st.write("") 
        draw_btn = st.button("🎨 繪製圖表", use_container_width=True)
    with col5:
        st.write("")
        fav_btn = st.button("❤️ 加入追蹤清單", use_container_width=True)

    t4_sid_clean = t4_sid.strip().upper()
    
    if fav_btn:
        entry = {"股票代號": t4_sid_clean, "追蹤分點": t4_br_name}
        if entry not in st.session_state.watchlist:
            st.session_state.watchlist.append(entry)
            st.success(f"✅ 已將 {t4_sid_clean} - {t4_br_name} 加入追蹤清單！")
        else:
            st.warning("⚠️ 此組合已在您的追蹤清單中。")

    with st.expander("⚙️ 圖表與技術指標設定"):
        tc1, tc2 = st.columns([1,2])
        with tc1: 
            t4_days = st.number_input("顯示最近幾根K棒?", value=200, min_value=10, max_value=1000)
        
        st.markdown("---")
        sc1, sc2, sc3 = st.columns(3)
        with sc1: 
            st.markdown("**主圖 (布林通道)**")
            bb_w = st.number_input("BB-週期", value=52)
            bb_std = st.number_input("BB-標準差", value=2.0, step=0.1)
        with sc2: 
            st.markdown("**副圖 2 (短線 MACD)**")
            macd1_f = st.number_input("短線-快線", value=12)
            macd1_s = st.number_input("短線-慢線", value=26)
            macd1_sig = st.number_input("短線-訊號", value=9)
        with sc3: 
            st.markdown("**副圖 3 (長線 MACD)**")
            macd2_f = st.number_input("長線-快線", value=26)
            macd2_s = st.number_input("長線-慢線", value=52)
            macd2_sig = st.number_input("長線-訊號", value=18)

    if st.session_state.watchlist:
        with st.expander("⭐ 我的主力追蹤清單 (打勾可直接載入圖表)", expanded=False):
            wl_df = pd.DataFrame(st.session_state.watchlist)
            wl_df.insert(0, '載入繪圖', False)
            wl_df['刪除此筆'] = False
            
            wl_config = {
                "載入繪圖": st.column_config.CheckboxColumn("載入繪圖", help="打勾後自動更新上方圖表"),
                "刪除此筆": st.column_config.CheckboxColumn("刪除此筆", help="打勾後將從清單移除")
            }
            edited_wl = st.data_editor(wl_df, hide_index=True, column_config=wl_config, use_container_width=True, key="wl_editor")
            
            load_rows = edited_wl[edited_wl['載入繪圖'] == True]
            if not load_rows.empty:
                load_sid = load_rows.iloc[0]['股票代號']
                load_br = load_rows.iloc[0]['追蹤分點']
                st.session_state.jump_sid = load_sid
                st.session_state.jump_br_name = load_br
                st.session_state.auto_draw = True
                st.rerun() 
                
            del_rows = edited_wl[edited_wl['刪除此筆'] == True]
            if not del_rows.empty:
                del_sid = del_rows.iloc[0]['股票代號']
                del_br = del_rows.iloc[0]['追蹤分點']
                st.session_state.watchlist = [item for item in st.session_state.watchlist if not (item['股票代號'] == del_sid and item['追蹤分點'] == del_br)]
                st.rerun()

    if draw_btn or st.session_state.auto_draw:
        st.session_state.t4_drawn = True
        st.session_state.auto_draw = False 
        st.session_state.locked_sid = t4_sid_clean
        st.session_state.locked_br_name = t4_br_name
        st.session_state.locked_br_id = BROKER_MAP[t4_br_name]['br_id']

    if st.session_state.t4_drawn:
        sid_to_draw = st.session_state.locked_sid
        br_name_to_draw = st.session_state.locked_br_name
        br_id_to_draw = st.session_state.locked_br_id
        
        with st.spinner(f"正在調取 {sid_to_draw} 歷史資料並繪製圖表，請稍候..."):
            try:
                df_k = get_stock_kline(sid_to_draw)
                if df_k.empty:
                    st.error(f"找不到代號 {sid_to_draw} 的 K 線資料，請確認是否為台股且具備 yfinance 資料。")
                else:
                    df_broker = get_fubon_history(sid_to_draw, br_id_to_draw)
                    
                    if df_broker.empty:
                        st.info(f"備註：歷史紀錄中，{br_name_to_draw} 在 {sid_to_draw} 沒有交易紀錄。")

                    df_merged = pd.merge(df_k, df_broker[['Date', '買賣超']], on='Date', how='left')
                    df_merged['買賣超'] = df_merged['買賣超'].fillna(0) 
                    
                    df_merged.set_index('Date', inplace=True)
                    if t4_period == "週":
                        df_resampled = df_merged.resample('W-FRI').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', '買賣超':'sum'})
                    elif t4_period == "月":
                        df_resampled = df_merged.resample('M').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', '買賣超':'sum'})
                    else:
                        df_resampled = df_merged.copy()
                    
                    df_resampled = df_resampled.dropna(subset=['Close']).reset_index()
                    df_resampled['Date_str'] = df_resampled['Date'].dt.strftime('%Y-%m-%d') 

                    df_resampled['BB_mid'] = df_resampled['Close'].rolling(int(bb_w)).mean()
                    df_resampled['BB_std'] = df_resampled['Close'].rolling(int(bb_w)).std()
                    df_resampled['BB_up'] = df_resampled['BB_mid'] + bb_std * df_resampled['BB_std']
                    df_resampled['BB_dn'] = df_resampled['BB_mid'] - bb_std * df_resampled['BB_std']
                    
                    macd1, sig1, hist1 = calculate_macd(df_resampled, int(macd1_f), int(macd1_s), int(macd1_sig))
                    macd2, sig2, hist2 = calculate_macd(df_resampled, int(macd2_f), int(macd2_s), int(macd2_sig))
                    
                    df_plot = df_resampled.tail(int(t4_days)).copy()
                    hist1_plot = hist1.tail(int(t4_days))
                    macd1_plot = macd1.tail(int(t4_days))
                    sig1_plot = sig1.tail(int(t4_days))
                    hist2_plot = hist2.tail(int(t4_days))
                    macd2_plot = macd2.tail(int(t4_days))
                    sig2_plot = sig2.tail(int(t4_days))
                    
                    fig = make_subplots(
                        rows=4, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, 
                        row_heights=[0.5, 0.2, 0.15, 0.15],
                        subplot_titles=("K線與布林通道", f"分點買賣超 ({br_name_to_draw})", "MACD (短線)", "MACD (長線)")
                    )
                    
                    colors_k = ['#FF3333' if close >= open else '#00AA00' for close, open in zip(df_plot['Close'], df_plot['Open'])]
                    colors_vol = ['#FF3333' if val >= 0 else '#00AA00' for val in df_plot['買賣超']]
                    colors_macd1 = ['#FF3333' if val >= 0 else '#00AA00' for val in hist1_plot]
                    colors_macd2 = ['#FF3333' if val >= 0 else '#00AA00' for val in hist2_plot]

                    # 💎 終極 Hover 格式修正：強制黑底白字高對比，解決所有黑暗模式盲點
                    custom_hover = (
                        "<b>日期: %{x}</b><br><br>"
                        "<b>開盤: %{open:.2f}</b><br>"
                        "<b>最高: %{high:.2f}</b><br>"
                        "<b>最低: %{low:.2f}</b><br>"
                        "<b>收盤: %{close:.2f}</b><br>"
                        "<extra></extra>"
                    )

                    fig.add_trace(go.Candlestick(
                        x=df_plot['Date_str'], open=df_plot['Open'], high=df_plot['High'],
                        low=df_plot['Low'], close=df_plot['Close'], name='K線',
                        increasing_line_color='#FF3333', decreasing_line_color='#00AA00',
                        hovertemplate=custom_hover,
                        hoverlabel=dict(bgcolor="#222222", font=dict(color="white", size=14), bordercolor="#555555")
                    ), row=1, col=1)
                    
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=df_plot['BB_mid'], line=dict(color='rgba(255, 255, 255, 0.4)', width=1), name='BB中軌', hoverinfo='skip'), row=1, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=df_plot['BB_up'], line=dict(color='rgba(173, 216, 230, 0.5)', width=1, dash='dot'), name='BB上軌', hoverinfo='skip'), row=1, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=df_plot['BB_dn'], fill='tonexty', fillcolor='rgba(173, 216, 230, 0.1)', line=dict(color='rgba(173, 216, 230, 0.5)', width=1, dash='dot'), name='BB下軌', hoverinfo='skip'), row=1, col=1)

                    fig.add_trace(go.Bar(
                        x=df_plot['Date_str'], y=df_plot['買賣超'], name='買賣超',
                        marker_color=colors_vol, hovertemplate="<b>%{y} 張</b><extra></extra>"
                    ), row=2, col=1)

                    fig.add_trace(go.Bar(x=df_plot['Date_str'], y=hist1_plot, marker_color=colors_macd1, name='MACD柱', hovertemplate="<b>%{y:.2f}</b><extra></extra>"), row=3, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=macd1_plot, line=dict(color='yellow', width=1), name='MACD', hoverinfo='skip'), row=3, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=sig1_plot, line=dict(color='cyan', width=1), name='Signal', hoverinfo='skip'), row=3, col=1)

                    fig.add_trace(go.Bar(x=df_plot['Date_str'], y=hist2_plot, marker_color=colors_macd2, name='MACD柱', hovertemplate="<b>%{y:.2f}</b><extra></extra>"), row=4, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=macd2_plot, line=dict(color='yellow', width=1), name='MACD', hoverinfo='skip'), row=4, col=1)
                    fig.add_trace(go.Scatter(x=df_plot['Date_str'], y=sig2_plot, line=dict(color='cyan', width=1), name='Signal', hoverinfo='skip'), row=4, col=1)

                    fig.update_layout(
                        height=900,
                        margin=dict(l=10, r=10, t=30, b=10),
                        plot_bgcolor='#131722', paper_bgcolor='#131722',
                        font=dict(color='#d1d4dc'),
                        showlegend=False,
                        xaxis_rangeslider_visible=False,
                        modebar_add=['drawline', 'drawhline', 'drawrect', 'drawcircle', 'eraseshape'],
                        dragmode='pan'
                    )
                    fig.update_xaxes(showgrid=False, zeroline=False, type='category', showspikes=True, spikemode='across', spikethickness=1, spikedash='dot', spikecolor='#555555') 
                    fig.update_yaxes(showgrid=False, zeroline=False)

                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'scrollZoom': True})

            except Exception as e:
                st.error(f"繪圖發生錯誤: {e}")
