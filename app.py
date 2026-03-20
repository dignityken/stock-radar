import streamlit as st
import pandas as pd
import requests
from io import StringIO
import re
import datetime
import urllib3

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="籌碼雷達", layout="wide")

HEADERS = {"User-Agent": "Mozilla/5.0"} # 確保 HEADERS 在這裡被定義！

# --- Google Drive 文件連結 ---
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drivesdk"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drivesdk"

# --- 函數：從 Google Drive 連結下載內容 ---
@st.cache_data(ttl=3600) # 快取數據，每小時更新一次
def download_google_drive_file(url):
    """從Google Drive公開連結下載文件內容"""
    file_id = url.split('/')[-2] # 從URL中提取文件ID
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        response = requests.get(download_url, stream=True, verify=False, timeout=10)
        response.raise_for_status() # 檢查請求是否成功
        return response.text
    except requests.exceptions.RequestException as e:
        st.error(f"從Google Drive下載文件失敗: {e}")
        return None

# --- 載入數據 ---
@st.cache_data(ttl=3600) # 快取解析後的 HQ_DATA
def load_hq_data(url):
    content = download_google_drive_file(url)
    if not content:
        return {}
    
    hq_data = {}
    # 假設總公司.txt 格式是 "證券商代號\t證券商名稱" (tab 分隔)
    for line in content.strip().split('\n'):
        if "\t" in line and not line.startswith("證券商代號"): # 跳過標題行
            parts = line.split('\t')
            if len(parts) == 2:
                hq_data[parts[0].strip()] = parts[1].strip()
    return hq_data

@st.cache_data(ttl=3600) # 快取解析後的 FINAL_RAW_DATA_CLEANED
def load_branch_data(url):
    content = download_google_drive_file(url)
    if not content:
        return ""
    # 分公司.txt 內容就是 FINAL_RAW_DATA_CLEANED 所需的格式
    # 注意：您的分公司.txt 開頭多了一個單引號，這裡需要處理掉
    return content.strip().lstrip("'").rstrip("'")

# 載入 HQ_DATA 和 FINAL_RAW_DATA_CLEANED
HQ_DATA = load_hq_data(GOOGLE_DRIVE_HQ_DATA_URL)
FINAL_RAW_DATA_CLEANED = load_branch_data(GOOGLE_DRIVE_BRANCH_DATA_URL)

# ==========================================
# 0. 完整數據庫建置
# ==========================================

# --- 修正後的數據庫解析邏輯 ---
def build_full_broker_db_structure(raw_data_string, hq_data_map):
    tree = {}; name_map = {}
    for group_str in raw_data_string.strip().split(';'):
        if not group_str: continue

        parts = group_str.split('!')
        if not parts: continue

        # 解析總公司資訊
        head_info = parts[0].split(',')
        if len(head_info) != 2: continue # 確保有代碼和名稱
        bid, bname = head_info[0].strip(), head_info[1].replace("亚","亞").strip()

        # 使用 HQ_DATA 優先獲取標準總公司名稱
        final_bname = hq_data_map.get(bid, bname)
        
        # 初始化分公司列表，用於去重
        branches_processed = {}
        
        # 解析分公司資訊
        for p_str in parts[1:]: # 從第二個元素開始解析分公司
            if ',' in p_str:
                br_id, br_name_raw = p_str.split(',', 1) # 只分割一次，避免名稱中有多個逗號
                br_id = br_id.strip()
                br_name = br_name_raw.replace("亚","亞").strip()
                
                # 去重邏輯：如果分公司名稱已存在，則跳過或更新 (此處選擇跳過以保留第一個出現的)
                if br_name not in branches_processed:
                    branches_processed[br_name] = br_id
                    name_map[br_name] = {"hq_id": bid, "br_id": br_id, "hq_name": final_bname}
                
        # 將總公司本身作為一個分點也加入列表，如果它不在裡面且名稱合理
        # 這裡判斷如果總公司名稱與分公司名稱相同，則視為一個分點
        # 修正：不應該是 branches_processed.get(bname) 而是檢查 hq_data_map.get(bid) 是否已存在
        if final_bname not in branches_processed and bid not in [br_id for br_id in branches_processed.values()]:
             branches_processed[final_bname] = bid
             name_map[final_bname] = {"hq_id": bid, "br_id": bid, "hq_name": final_bname}

        tree[final_bname] = {"bid": bid, "branches": branches_processed}
    
    # 處理分公司名稱重複的問題 (例如 "北城" 和 "北城證券" 是一樣的)
    # 這裡假設如果名稱完全相同，只保留一個
    final_tree = {}
    for hq_name, hq_data in tree.items():
        unique_branches = {}
        seen_names = set()
        for br_name, br_id in hq_data['branches'].items():
            if br_name not in seen_names:
                unique_branches[br_name] = br_id
                seen_names.add(br_name)
        final_tree[hq_name] = {"bid": hq_data['bid'], "branches": unique_branches}

    # 針對 "北城" 和 "北城證券" 這種特殊情況進行手動調整或更精細去重
    # 例如，如果 "北城證券" 存在，就移除 "北城" (如果它們的 bid 相同)
    # 修正：這段邏輯在解析大量數據時會變得很慢且可能不夠全面
    # 更好的做法是在原始數據中就確保唯一性，或者將 "北城" 視為 "北城證券" 的別名。
    # 暫時保留，但如果遇到性能問題可以考慮移除或優化。
    if '北城證券' in final_tree and '北城' in final_tree:
        if final_tree['北城證券']['bid'] == final_tree['北城']['bid']:
            del final_tree['北城']
            if '北城' in name_map: del name_map['北城']
    
    return final_tree, name_map


UI_TREE, BROKER_MAP = build_full_broker_db_structure(FINAL_RAW_DATA_CLEANED, HQ_DATA)


def get_stock_id(name_str):
    """
    從股票名稱字串中提取股票代號。
    支援純數字代號 (至少4位) 和字母數字混合代號 (如 ETF)。
    """
    s = str(name_str).strip()

    # 1. 嘗試匹配開頭為數字且長度為4位或以上的代號
    match_num = re.match(r'^(\d{4,})', s)
    if match_num:
        return match_num.group(1)

    # 2. 嘗試匹配開頭為數字或字母，且後面跟著數字或字母的組合 (適用於 ETF, 如 00981A, 0050)
    match_alpha_num = re.match(r'^([a-zA-Z0-9]+)', s)
    if match_alpha_num:
        return match_alpha_num.group(1)
        
    return None

# ==========================================
# 1. UI 介面設定
# ==========================================
tab1, tab2 = st.tabs(["🚀 券商分點查股票", "📊 股票代號查分點"])

# --- Tab 1 核心邏輯 (完全鏡像 Colab + 超連結) ---
with tab1:
    st.markdown("### 🏦 追蹤特定分點進出")
    c1, c2 = st.columns(2)
    with c1: 
        sorted_hq_keys = sorted(UI_TREE.keys())
        default_index = sorted_hq_keys.index('(牛牛牛)亞證券') if '(牛牛牛)亞證券' in sorted_hq_keys else 0
        sel_hq = st.selectbox("選擇券商", sorted_hq_keys, index=default_index, key="t1_b_sel")
    with c2: 
        b_opts = UI_TREE[sel_hq]['branches']
        # 確保分點列表也是排序的
        sorted_br_keys = sorted(b_opts.keys())
        # 檢查 selected_branch_label 是否在 sorted_br_keys 中
        default_br_index = 0
        if '總公司' in sorted_br_keys: # 嘗試預設選擇總公司，如果存在
            default_br_index = sorted_br_keys.index('總公司')
        elif sel_hq in sorted_br_keys: # 如果券商名稱也是一個分點名稱，嘗試預設選擇它
            default_br_index = sorted_br_keys.index(sel_hq)

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
        bid_hq = UI_TREE[sel_hq]['bid'] # 總公司代碼

        is_amount = '金額' in t1_u
        c_param = "B" if is_amount else "E"
        col_buy = '買進金額' if is_amount else '買進張數'
        col_sell = '賣出金額' if is_amount else '賣出張數'
        
        # 使用 sel_br_id (分點代碼) 來查詢
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
                
                df_all = df_all[df_all['股票名稱'].apply(lambda x: bool(get_stock_id(x)))].copy() # 確保有有效的股票代碼
                
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

                # --- 核心連結生成 (修正股票代號處理與連結顯示) ---
                if not only_buy.empty:
                    only_buy['extracted_stock_id'] = only_buy['股票名稱'].apply(get_stock_id)
                    only_buy['點我看圖'] = only_buy.apply(
                        lambda row: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{row['extracted_stock_id']}.djhtm" 
                                    if row['extracted_stock_id'] else "", 
                        axis=1
                    )
                    only_buy.drop(columns=['extracted_stock_id'], inplace=True)

                if not only_sell.empty:
                    only_sell['extracted_stock_id'] = only_sell['股票名稱'].apply(get_stock_id)
                    only_sell['點我看圖'] = only_sell.apply(
                        lambda row: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{row['extracted_stock_id']}.djhtm" 
                                    if row['extracted_stock_id'] else "", 
                        axis=1
                    )
                    only_sell.drop(columns=['extracted_stock_id'], inplace=True)

                st.subheader(f"🕵️ 分點尋寶結果：{sel_hq} - {sel_br_l}")
                st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t1_u}")

                st.markdown(f"### 🔴 大戶吃貨中 (極端買進) - 共 {len(only_buy)} 檔")
                if not only_buy.empty:
                    only_buy = only_buy.sort_values(by=col_buy, ascending=False)
                    final_b = only_buy if show_full else only_buy.head(10)
                    st.dataframe(final_b[['股票名稱', col_buy, col_sell, '總額', '買%', '賣%', '點我看圖']], hide_index=True, column_config={"點我看圖": st.column_config.LinkColumn("查看連結", help="點擊查看個股K線圖")}, use_container_width=True)
                else: st.info("無符合條件之股票")

                st.markdown(f"### 🟢 大戶倒貨中 (極端賣出) - 共 {len(only_sell)} 檔")
                if not only_sell.empty:
                    only_sell = only_sell.sort_values(by=col_sell, ascending=False)
                    final_s = only_sell if show_full else only_sell.head(10)
                    st.dataframe(final_s[['股票名稱', col_buy, col_sell, '總額', '買%', '賣%', '點我看圖']], hide_index=True, column_config={"點我看圖": st.column_config.LinkColumn("查看連結", help="點擊查看個股K線圖")}, use_container_width=True)
                else: st.info("無符合條件之股票")
            else: st.warning("抓取不到數據。請檢查股票代號或券商分點是否正確。")
        except requests.exceptions.Timeout: st.error("請求超時，請稍後再試。")
        except requests.exceptions.RequestException as e: st.error(f"網絡請求錯誤: {e}")
        except Exception as e: st.error(f"發生錯誤: {e}")

# --- Tab 2: (修復超連結 + 維持濾網邏輯) ---
with tab2:
    st.markdown("### 📈 誰在買賣這檔股票？")
    c1, c2, c3 = st.columns(3)
    with c1: t2_sid = st.text_input("股票代號", "2408", key="t2_s")
    with c2: t2_sd = st.date_input("開始", datetime.date.today()-datetime.timedelta(days=7), key="t2_sd_in")
    with c3: t2_ed = st.date_input("結束", datetime.date.today(), key="t2_ed_in")
    c4, c5, c6 = st.columns([2, 1, 1])
    with c4: t2_m = st.radio("模式", ["嚴格模式", "濾網模式"], index=1, horizontal=True, key="t2_mode")
    with c5: t2_p = st.number_input("門檻佔比%", 0, 100, 95, step=1, key="t2_p_in")
    with c6: t2_v = st.number_input("最低張數", 0, 1000000, 10, step=1, key="t2_v_in")

    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
        if not t2_sid.strip().replace(" ", "").isalnum(): # 修正：允許股票代號包含字母數字
            st.error("股票代號必須是字母數字組合。")
        else:
            url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={t2_sid}&e={t2_sd.strftime('%Y-%m-%d')}&f={t2_ed.strftime('%Y-%m-%d')}"
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
                            return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid}&BHID={info['br_id']}&b={info['br_id']}&C=3"
                        
                        for k, v in BROKER_MAP.items():
                            if name_cleaned in k or k in name_cleaned:
                                return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid}&BHID={v['br_id']}&b={v['br_id']}&C=3"
                        return ""

                    for d in [b_df, s_df]: d['查看詳情'] = d['券商'].apply(get_link_t2)

                    st.subheader("🔴 吃貨主力分點")
                    st.dataframe(b_df.sort_values('買', ascending=False), hide_index=True, column_config={"查看詳情": st.column_config.LinkColumn("查看詳情", help="點擊查看分點進出明細")}, use_container_width=True)
                    st.subheader("🟢 倒貨主力分點")
                    st.dataframe(s_df.sort_values('賣', ascending=False), hide_index=True, column_config={"查看詳情": st.column_config.LinkColumn("查看詳情", help="點擊查看分點進出明細")}, use_container_width=True)
                else: st.warning("未找到資料。請檢查股票代號或日期區間。")
            except requests.exceptions.Timeout: st.error("請求超時，請稍後再試。")
            except requests.exceptions.RequestException as e: st.error(f"網絡請求錯誤: {e}")
            except Exception as e: st.error(f"發生錯誤: {e}")
