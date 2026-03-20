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

HEADERS = {"User-Agent": "Mozilla/5.0"} 

# --- Google Drive 文件連結 ---
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drivesdk"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drivesdk"

# --- 初始化 Session State (用於跨分頁傳遞參數) ---
if 'jump_sid' not in st.session_state:
    st.session_state.jump_sid = "6488"
if 'jump_br_name' not in st.session_state:
    st.session_state.jump_br_name = "兆豐-忠孝"
if 'auto_draw' not in st.session_state:
    st.session_state.auto_draw = False

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
# 0.5 建置「地緣/關鍵字」字典 (新功能)
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

# --- 輔助函數：自動判斷上市/上櫃並抓取 K 線 (已修正 MultiIndex 報錯) ---
@st.cache_data(ttl=3600)
def get_stock_kline(stock_id, days=120):
    end_date = datetime.date.today() + datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=days)
    
    for suffix in ['.TW', '.TWO']:
        ticker = f"{stock_id}{suffix}"
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if not df.empty:
            # 解決 yfinance 新版 MultiIndex 合併報錯問題：強制扁平化欄位
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            df.reset_index(inplace=True)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            return df
    return pd.DataFrame()

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

                # --- 結合 st.data_editor 產生可點擊按鈕來傳遞參數 ---
                for d in [only_buy, only_sell]:
                    if not d.empty:
                        d['extracted_stock_id'] = d['股票名稱'].apply(get_stock_id)
                        d['K線圖'] = d['extracted_stock_id'].apply(
                            lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else ""
                        )
                        d['分點明細'] = d['extracted_stock_id'].apply(
                            lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_br_id}&b={sel_br_id}&C=3" if sid else ""
                        )
                        # 加入一個按鈕連動的假欄位
                        d['帶入K線'] = "📊 帶入參數" 

                st.subheader(f"🕵️ 分點尋寶結果：{sel_hq} - {sel_br_l}")
                st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t1_u}")

                # 用 data_editor 攔截點擊事件
                def display_table_with_button(df_to_show, key_prefix):
                    if not df_to_show.empty:
                        df_show = df_to_show.copy()
                        df_show = df_show[['帶入K線', '股票名稱', 'K線圖', col_buy, col_sell, '總額', '買%', '賣%', '分點明細', 'extracted_stock_id']]
                        
                        col_config = {
                            "K線圖": st.column_config.LinkColumn("網頁K線", display_text="📈", help="外連富邦K線"),
                            "分點明細": st.column_config.LinkColumn("網頁明細", display_text="🏦", help="外連富邦明細"),
                            "帶入K線": st.column_config.TextColumn("自繪圖表", help="將參數傳送至 Tab4"),
                            "extracted_stock_id": None # 隱藏代碼欄位
                        }
                        
                        # 擷取使用者有沒有勾選/點擊
                        df_show['帶入K線'] = False # 轉成 Checkbox 讓使用者點擊
                        col_config["帶入K線"] = st.column_config.CheckboxColumn("送至 Tab4 繪圖", help="打勾後請手動切換至分頁四")

                        edited_df = st.data_editor(
                            df_show, 
                            hide_index=True, 
                            column_config=col_config, 
                            use_container_width=True,
                            key=f"editor_{key_prefix}"
                        )
                        
                        # 檢查哪一列被勾選了
                        clicked_rows = edited_df[edited_df['帶入K線'] == True]
                        if not clicked_rows.empty:
                            sid_clicked = clicked_rows.iloc[0]['extracted_stock_id']
                            st.session_state.jump_sid = sid_clicked
                            st.session_state.jump_br_name = sel_br_l
                            st.session_state.auto_draw = True
                            st.success(f"✅ 已將 {sid_clicked} 與 {sel_br_l} 參數送到 Tab4！請在最上方手動點擊「📊 主力 K 線圖」分頁即可直接繪圖。")

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
                        if info:
                            return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid_clean}&BHID={info['br_id']}&b={info['br_id']}&C=3"
                        
                        for k, v in BROKER_MAP.items():
                            if name_cleaned in k or k in name_cleaned:
                                return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid_clean}&BHID={v['br_id']}&b={v['br_id']}&C=3"
                        return ""

                    for d in [b_df, s_df]: 
                        d['網頁明細'] = d['券商'].apply(get_link_t2)
                        d['送至 Tab4 繪圖'] = False # 建立勾選框

                    def display_table_with_button_t2(df_to_show, key_prefix):
                        if not df_to_show.empty:
                            df_show = df_to_show.copy()
                            df_show = df_show[['送至 Tab4 繪圖', '券商', '買', '賣', '合計', '買進%', '賣出%', '網頁明細']]
                            col_config = {
                                "網頁明細": st.column_config.LinkColumn("網頁明細", display_text="🏦", help="外連富邦明細"),
                                "送至 Tab4 繪圖": st.column_config.CheckboxColumn("送至 Tab4 繪圖", help="打勾後請手動切換至分頁四")
                            }
                            edited_df = st.data_editor(
                                df_show, hide_index=True, column_config=col_config, 
                                use_container_width=True, key=f"editor_{key_prefix}"
                            )
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
            except requests.exceptions.Timeout: st.error("請求超時，請稍後再試。")
            except requests.exceptions.RequestException as e: st.error(f"網絡請求錯誤: {e}")
            except Exception as e: st.error(f"發生錯誤: {e}")


# --- Tab 3 (地緣券商尋寶) ---
with tab3:
    st.markdown("### 📍 尋找地緣/同名分點進出")
    st.caption("透過分點名稱後綴（例如：城中、三重、信義）跨券商尋找特定地區的買賣神人。")
    # (此分頁維持原樣，保留您探索地緣分點的功能，未加入跳轉按鈕保持畫面簡潔)
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
                for d in [only_buy, only_sell]:
                    if not d.empty:
                        d['extracted_stock_id'] = d['股票名稱'].apply(get_stock_id)
                        d['K線圖'] = d['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else "")
                        d['分點明細'] = d['extracted_stock_id'].apply(lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_t3_br_id}&b={sel_t3_br_id}&C=3" if sid else "")
                        d.drop(columns=['extracted_stock_id'], inplace=True)

                st.subheader(f"🕵️ 地緣雷達結果：{sel_t3_br_l}")
                st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t3_u}")
                display_cols = ['股票名稱', 'K線圖', col_buy, col_sell, '總額', '買%', '賣%', '分點明細']
                col_config = {"K線圖": st.column_config.LinkColumn("K線圖", display_text="📈 看圖"), "分點明細": st.column_config.LinkColumn("分點明細", display_text="🏦 看分點")}
                st.markdown(f"### 🔴 該分點吃貨中 (極端買進) - 共 {len(only_buy)} 檔")
                if not only_buy.empty: st.dataframe(only_buy.sort_values(by=col_buy, ascending=False).head(999 if show_full_t3 else 10)[display_cols], hide_index=True, column_config=col_config, use_container_width=True)
                else: st.info("無符合條件之股票")
                st.markdown(f"### 🟢 該分點倒貨中 (極端賣出) - 共 {len(only_sell)} 檔")
                if not only_sell.empty: st.dataframe(only_sell.sort_values(by=col_sell, ascending=False).head(999 if show_full_t3 else 10)[display_cols], hide_index=True, column_config=col_config, use_container_width=True)
                else: st.info("無符合條件之股票")
            else: st.warning("抓取不到數據。請檢查股票代號或券商分點是否正確。")
        except Exception as e: st.error(f"發生錯誤: {e}")

# --- Tab 4 (專業主力 K 線圖) ---
with tab4:
    st.markdown("### 📊 專業主力 K 線與分點進出圖")
    st.caption("將醜陋的網頁轉化為專業 TradingView 質感的分析圖表。")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        t4_sid = st.text_input("輸入股票代號 (如: 6488)", st.session_state.jump_sid, key="t4_sid")
    with col2:
        all_br_names = sorted(list(BROKER_MAP.keys()))
        
        # 處理來自 Session State 傳遞的券商名稱，如果不在列表內就預設選第一個
        passed_br = st.session_state.jump_br_name
        # 由於 Tab2 傳過來的券商可能帶有 "券商總公司" 的名字，做個簡單的容錯
        cleaned_passed_br = passed_br.replace("亞","亞").strip()
        default_br_idx = all_br_names.index(cleaned_passed_br) if cleaned_passed_br in all_br_names else 0
        
        t4_br_name = st.selectbox("搜尋/選擇分點", all_br_names, index=default_br_idx, key="t4_br")
    with col3:
        st.write("") 
        draw_btn = st.button("🎨 繪製專業圖表", use_container_width=True)

    with st.expander("⚙️ MACD 參數設定"):
        mc1, mc2, mc3 = st.columns(3)
        with mc1: st.markdown("**副圖 2 (短線 MACD)**")
        with mc2: st.markdown("**副圖 3 (長線 MACD)**")
        with mc3: st.write("")
        
        mc1_1, mc1_2, mc1_3 = st.columns(3)
        with mc1_1: macd1_f = st.number_input("短線-快線", value=12)
        with mc1_2: macd1_s = st.number_input("短線-慢線", value=26)
        with mc1_3: macd1_sig = st.number_input("短線-訊號", value=9)
        
        mc2_1, mc2_2, mc2_3 = st.columns(3)
        with mc2_1: macd2_f = st.number_input("長線-快線", value=26)
        with mc2_2: macd2_s = st.number_input("長線-慢線", value=52)
        with mc2_3: macd2_sig = st.number_input("長線-訊號", value=18)

    # 觸發繪圖：手動點擊按鈕，或是從 Session State 接到自動繪圖的指令
    if draw_btn or st.session_state.auto_draw:
        # 重置自動繪圖旗標，避免下次切換分頁又自動畫
        st.session_state.auto_draw = False 
        
        t4_sid_clean = t4_sid.strip().upper()
        t4_br_info = BROKER_MAP[t4_br_name]
        t4_br_id = t4_br_info['br_id']
        
        with st.spinner("正在為您繪製專業圖表，請稍候... (初次繪製或抓取新標的時可能需要 5-10 秒)"):
            try:
                df_k = get_stock_kline(t4_sid_clean, days=180)
                if df_k.empty:
                    st.error(f"找不到代號 {t4_sid_clean} 的 K 線資料，目前本繪圖功能暫不支援 ETF 等無 yfinance 資料之標的。")
                else:
                    url_history = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t4_sid_clean}&BHID={t4_br_id}&b={t4_br_id}&C=3"
                    res_hist = requests.get(url_history, headers=HEADERS, verify=False, timeout=15)
                    res_hist.encoding = 'big5'
                    
                    df_broker = pd.DataFrame()
                    tables = pd.read_html(StringIO(res_hist.text))
                    for tb in tables:
                        if tb.shape[1] == 5 and '日期' in str(tb.iloc[0].values):
                            df_broker = tb.copy()
                            df_broker.columns = ['Date', '買進', '賣出', '總額', '買賣超']
                            df_broker = df_broker.drop(0) 
                            break
                    
                    if df_broker.empty:
                        st.warning(f"近期內，{t4_br_name} 在 {t4_sid_clean} 沒有交易紀錄。")
                    else:
                        df_broker = df_broker[~df_broker['Date'].str.contains('日期|合計|說明', na=False)].copy()
                        df_broker['Date'] = pd.to_datetime(df_broker['Date'].astype(str).str.replace(' ', ''))
                        df_broker['買賣超'] = pd.to_numeric(df_broker['買賣超'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                        
                        df_merged = pd.merge(df_k, df_broker[['Date', '買賣超']], on='Date', how='left')
                        df_merged['買賣超'] = df_merged['買賣超'].fillna(0) 
                        
                        df_plot = df_merged.tail(60).copy()
                        
                        macd1, sig1, hist1 = calculate_macd(df_plot, macd1_f, macd1_s, macd1_sig)
                        macd2, sig2, hist2 = calculate_macd(df_plot, macd2_f, macd2_s, macd2_sig)
                        
                        fig = make_subplots(
                            rows=4, cols=1, shared_xaxes=True, 
                            vertical_spacing=0.03, 
                            row_heights=[0.5, 0.2, 0.15, 0.15],
                            subplot_titles=("K線圖", f"分點買賣超 ({t4_br_name})", "MACD (短線)", "MACD (長線)")
                        )
                        
                        colors_k = ['#FF3333' if close >= open else '#00AA00' for close, open in zip(df_plot['Close'], df_plot['Open'])]
                        colors_vol = ['#FF3333' if val >= 0 else '#00AA00' for val in df_plot['買賣超']]
                        colors_macd1 = ['#FF3333' if val >= 0 else '#00AA00' for val in hist1]
                        colors_macd2 = ['#FF3333' if val >= 0 else '#00AA00' for val in hist2]

                        fig.add_trace(go.Candlestick(
                            x=df_plot['Date'], open=df_plot['Open'], high=df_plot['High'],
                            low=df_plot['Low'], close=df_plot['Close'], name='K線',
                            increasing_line_color='#FF3333', decreasing_line_color='#00AA00'
                        ), row=1, col=1)
                        
                        fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['Close'].rolling(5).mean(), line=dict(color='orange', width=1), name='5MA'), row=1, col=1)
                        fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['Close'].rolling(10).mean(), line=dict(color='blue', width=1), name='10MA'), row=1, col=1)

                        fig.add_trace(go.Bar(
                            x=df_plot['Date'], y=df_plot['買賣超'], name='買賣超(張)',
                            marker_color=colors_vol
                        ), row=2, col=1)

                        fig.add_trace(go.Bar(x=df_plot['Date'], y=hist1, marker_color=colors_macd1, name='MACD 柱'), row=3, col=1)
                        fig.add_trace(go.Scatter(x=df_plot['Date'], y=macd1, line=dict(color='yellow', width=1), name='MACD'), row=3, col=1)
                        fig.add_trace(go.Scatter(x=df_plot['Date'], y=sig1, line=dict(color='cyan', width=1), name='Signal'), row=3, col=1)

                        fig.add_trace(go.Bar(x=df_plot['Date'], y=hist2, marker_color=colors_macd2, name='MACD 柱'), row=4, col=1)
                        fig.add_trace(go.Scatter(x=df_plot['Date'], y=macd2, line=dict(color='yellow', width=1), name='MACD'), row=4, col=1)
                        fig.add_trace(go.Scatter(x=df_plot['Date'], y=sig2, line=dict(color='cyan', width=1), name='Signal'), row=4, col=1)

                        fig.update_layout(
                            height=900,
                            margin=dict(l=10, r=10, t=30, b=10),
                            plot_bgcolor='#1E1E1E', paper_bgcolor='#1E1E1E',
                            font=dict(color='white'),
                            showlegend=False,
                            xaxis_rangeslider_visible=False 
                        )
                        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#333333', type='category') 
                        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#333333')

                        st.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"繪圖發生錯誤: {e}")
