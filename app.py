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

HEADERS = {"User-Agent": "Mozilla/5.0"} 

# --- Google Drive 文件連結 ---
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drivesdk"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drivesdk"

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

def get_stock_id(name_str):
    """
    從股票名稱字串中提取股票代號。
    修正：直接抓取開頭連續的「英文字母與數字」組合，解決 00984B 遺漏 B 的問題。
    """
    s = str(name_str).strip()
    match = re.match(r'^([a-zA-Z0-9]{4,})', s)
    if match:
        return match.group(1)
    return None

# ==========================================
# 1. UI 介面設定
# ==========================================
tab1, tab2 = st.tabs(["🚀 券商分點查股票", "📊 股票代號查分點"])

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

                # --- 核心連結生成 (加入K線圖與分點明細連結) ---
                for d in [only_buy, only_sell]:
                    if not d.empty:
                        d['extracted_stock_id'] = d['股票名稱'].apply(get_stock_id)
                        
                        # K線圖連結 
                        d['K線圖'] = d['extracted_stock_id'].apply(
                            lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm" if sid else ""
                        )
                        # 分點明細連結 
                        d['分點明細'] = d['extracted_stock_id'].apply(
                            lambda sid: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={sid}&BHID={sel_br_id}&b={sel_br_id}&C=3" if sid else ""
                        )
                        d.drop(columns=['extracted_stock_id'], inplace=True)

                st.subheader(f"🕵️ 分點尋寶結果：{sel_hq} - {sel_br_l}")
                st.caption(f"📌 區間：{sd_s} ~ {ed_s} | 單位：{t1_u}")

                display_cols = ['股票名稱', 'K線圖', col_buy, col_sell, '總額', '買%', '賣%', '分點明細']
                col_config = {
                    "K線圖": st.column_config.LinkColumn("K線圖", display_text="📈 看圖", help="點擊查看個股技術線圖"),
                    "分點明細": st.column_config.LinkColumn("分點明細", display_text="🏦 看分點", help="點擊查看此分點在該檔股票的進出明細")
                }

                st.markdown(f"### 🔴 大戶吃貨中 (極端買進) - 共 {len(only_buy)} 檔")
                if not only_buy.empty:
                    only_buy = only_buy.sort_values(by=col_buy, ascending=False)
                    final_b = only_buy if show_full else only_buy.head(10)
                    st.dataframe(final_b[display_cols], hide_index=True, column_config=col_config, use_container_width=True)
                else: st.info("無符合條件之股票")

                st.markdown(f"### 🟢 大戶倒貨中 (極端賣出) - 共 {len(only_sell)} 檔")
                if not only_sell.empty:
                    only_sell = only_sell.sort_values(by=col_sell, ascending=False)
                    final_s = only_sell if show_full else only_sell.head(10)
                    st.dataframe(final_s[display_cols], hide_index=True, column_config=col_config, use_container_width=True)
                else: st.info("無符合條件之股票")
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
        t2_sid_clean = t2_sid.strip().replace(" ", "")
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

                    for d in [b_df, s_df]: d['查看詳情'] = d['券商'].apply(get_link_t2)

                    st.subheader("🔴 吃貨主力分點")
                    b_df_sorted = b_df.sort_values('買', ascending=False)
                    final_b_t2 = b_df_sorted if show_full_t2 else b_df_sorted.head(10)
                    st.dataframe(final_b_t2, hide_index=True, column_config={"查看詳情": st.column_config.LinkColumn("查看詳情", display_text="🏦 看分點", help="點擊查看分點進出明細")}, use_container_width=True)
                    
                    st.subheader("🟢 倒貨主力分點")
                    s_df_sorted = s_df.sort_values('賣', ascending=False)
                    final_s_t2 = s_df_sorted if show_full_t2 else s_df_sorted.head(10)
                    st.dataframe(final_s_t2, hide_index=True, column_config={"查看詳情": st.column_config.LinkColumn("查看詳情", display_text="🏦 看分點", help="點擊查看分點進出明細")}, use_container_width=True)
                else: st.warning("未找到資料。請檢查股票代號或日期區間。")
            except requests.exceptions.Timeout: st.error("請求超時，請稍後再試。")
            except requests.exceptions.RequestException as e: st.error(f"網絡請求錯誤: {e}")
            except Exception as e: st.error(f"發生錯誤: {e}")
