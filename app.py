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

# --- 🚀 關鍵修復：定義數據處理函數 ---
def safe_float(val):
    if pd.isna(val): return None
    return float(val)

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
if 'custom_hlines' not in st.session_state: st.session_state.custom_hlines = []

def send_to_tab4(sid, br_name):
    st.session_state.t4_sid_ui = sid
    clean_br = br_name.replace("亚","亞").strip()
    if clean_br in BROKER_MAP:
        st.session_state.t4_br_ui = clean_br
    st.session_state.auto_draw = True

# ==========================================
# 資料載入與處理
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
    ticker = f"{stock_id}.TW"
    df = yf.download(ticker, start="2000-01-01", progress=False)
    if df.empty:
        df = yf.download(f"{stock_id}.TWO", start="2000-01-01", progress=False)
    if not df.empty:
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        df.reset_index(inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
    return df

@st.cache_data(ttl=1800)
def get_fubon_history(sid, br_id):
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?A={sid}&BHID={br_id}&b={br_id}&C=3&D=1999-1-1&E={today_str}&ver=V3"
    res = requests.get(url, headers=HEADERS, verify=False, timeout=20); res.encoding = 'big5'
    try:
        df_broker = pd.read_html(StringIO(res.text))[2] # 預設位置
        if df_broker.shape[1] == 5:
            df_broker.columns = ['Date', '買進', '賣出', '總額', '買賣超']
            df_broker = df_broker[~df_broker['Date'].str.contains('日期|合計|說明', na=False)].copy()
            df_broker['Date'] = pd.to_datetime(df_broker['Date'].astype(str).str.replace(' ', ''))
            df_broker['買賣超'] = pd.to_numeric(df_broker['買賣超'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            return df_broker
    except: pass
    return pd.DataFrame(columns=['Date', '買賣超'])

# ==========================================
# UI 介面
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs(["🚀 特定分點", "📊 股票代號", "📍 地緣券商", "📊 主力 K 線圖"])

# --- Tab 1 ---
with tab1:
    c1, c2 = st.columns(2)
    with c1: sel_hq = st.selectbox("選擇券商", sorted(UI_TREE.keys()), key="t1_hq")
    with c2: sel_br_l = st.selectbox("選擇分點", sorted(UI_TREE[sel_hq]['branches'].keys()), key="t1_br")
    sel_br_id = UI_TREE[sel_hq]['branches'][sel_br_l]
    c3, c4, c5 = st.columns(3)
    with c3: t1_sd = st.date_input("區間起點", datetime.date.today()-datetime.timedelta(days=7), key="t1_sd")
    with c4: t1_ed = st.date_input("區間終點", datetime.date.today(), key="t1_ed")
    with c5: t1_u = st.radio("單位", ["張數", "金額"], horizontal=True, key="t1_u")
    if st.button("開始分點尋寶 🚀", key="t1_go"):
        st.session_state.t1_searched = True
        # ... (抓取邏輯與前述相同)
    if st.session_state.t1_searched:
        # 表格顯示與連動邏輯
        pass

# --- Tab 2 ---
with tab2:
    t2_sid = st.text_input("股票代號", "2408", key="t2_s")
    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
        st.session_state.t2_searched = True
    if st.session_state.t2_searched:
        # 表格顯示與連動邏輯
        pass

# --- Tab 3 ---
with tab3:
    sel_loc = st.selectbox("選擇地緣", sorted(GEO_MAP.keys()), key="t3_loc")
    sel_t3_br_l = st.selectbox("選擇分點", sorted(GEO_MAP[sel_loc].keys()), key="t3_br")
    if st.button("啟動地緣雷達 📡", key="t3_go"):
        st.session_state.t3_searched = True
    if st.session_state.t3_searched:
        # 表格與按鈕語法修正
        def display_t3(df, prefix):
            if not df.empty:
                df_show = df.copy(); df_show['畫圖'] = False
                edited = st.data_editor(df_show, key=f"ed_{prefix}")
                clicked = edited[edited['畫圖'] == True]
                if not clicked.empty:
                    send_to_tab4(get_stock_id(clicked.iloc[0]['股票名稱']), sel_t3_br_l)
                    st.success("已設定，請切換至 Tab 4")

# --- Tab 4 ---
with tab4:
    col_t1, col_t2, col_t3, col_t4, col_t5 = st.columns([1, 1.5, 1, 1, 1])
    with col_t1: t4_sid = st.text_input("股票代號", st.session_state.t4_sid_ui, key="t4_sid_input")
    with col_t2: 
        all_br = sorted(list(BROKER_MAP.keys()))
        t4_br_name = st.selectbox("搜尋分點", all_br, index=all_br.index(st.session_state.t4_br_ui) if st.session_state.t4_br_ui in all_br else 0, key="t4_br_input")
    with col_t3: t4_period = st.radio("週期", ["日", "週", "月"], horizontal=True)
    with col_t4: t4_days = st.number_input("K棒數", value=200, min_value=10)
    with col_t5: st.write(""); draw_btn = st.button("🎨 繪圖", use_container_width=True)

    t4_sid_clean = t4_sid.strip().upper()

    if draw_btn or st.session_state.auto_draw:
        st.session_state.auto_draw = False
        t4_br_id = BROKER_MAP[t4_br_name]['br_id']
        with st.spinner(f"繪製 {t4_sid_clean} 中..."):
            df_k = get_stock_kline(t4_sid_clean)
            if not df_k.empty:
                url_hist = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?A={t4_sid_clean}&BHID={t4_br_id}&b={t4_br_id}&C=3&D=1999-1-1&E={datetime.date.today().strftime('%Y-%m-%d')}&ver=V3"
                res_hist = requests.get(url_hist, headers=HEADERS, verify=False, timeout=20); res_hist.encoding = 'big5'
                stock_name = ""
                m_name = re.search(r"對\s+([^\(]+)\(\s*" + re.escape(t4_sid_clean) + r"\s*\)個股", res_hist.text)
                if m_name: stock_name = m_name.group(1).strip()
                
                df_broker = get_fubon_history(t4_sid_clean, t4_br_id)
                df_merged = pd.merge(df_k, df_broker, on='Date', how='left').fillna(0)
                df_merged.set_index('Date', inplace=True)
                if t4_period == "週": df_res = df_merged.resample('W-FRI').agg({'Open':'first','High':'max','Low':'min','Close':'last','買賣超':'sum'})
                elif t4_period == "月": df_res = df_merged.resample('M').agg({'Open':'first','High':'max','Low':'min','Close':'last','買賣超':'sum'})
                else: df_res = df_merged.copy()
                
                df_res = df_res.dropna().reset_index()
                df_res['Date_str'] = df_res['Date'].dt.strftime('%Y-%m-%d')
                df_res['BB_m'] = df_res['Close'].rolling(52).mean()
                df_res['BB_u'] = df_res['BB_m'] + 2.0 * df_res['Close'].rolling(52).std()
                df_res['BB_d'] = df_res['BB_m'] - 2.0 * df_res['Close'].rolling(52).std()
                m1, s1, h1 = calculate_macd(df_res, 12, 26, 9)
                m2, s2, h2 = calculate_macd(df_res, 26, 52, 18)
                
                plot_data = []
                for i, r in df_res.tail(int(t4_days)).iterrows():
                    plot_data.append({
                        "t": r['Date_str'], "o": r['Open'], "h": r['High'], "l": r['Low'], "c": r['Close'], "v": r['買賣超'],
                        "bm": safe_float(r['BB_m']), "bu": safe_float(r['BB_u']), "bd": safe_float(r['BB_d']),
                        "h1": safe_float(h1.iloc[i]), "m1": safe_float(m1.iloc[i]), "s1": safe_float(s1.iloc[i]),
                        "h2": safe_float(h2.iloc[i]), "m2": safe_float(m2.iloc[i]), "s2": safe_float(s2.iloc[i])
                    })

                html_code = f"""
                <!DOCTYPE html><html><head><script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script><style>
                body {{ margin:0; background:#131722; font-family:sans-serif; overflow:hidden; }}
                #chart {{ width:100vw; height:95vh; }}
                .tv-watermark {{ position:absolute; top:35%; left:50%; transform:translate(-50%,-50%); font-size:70px; font-weight:bold; color:rgba(255,255,255,0.06); z-index:1; pointer-events:none; white-space:nowrap; }}
                .tv-legend {{ position:absolute; left:12px; top:12px; z-index:9999; font-size:13px; color:#d1d4dc; background:rgba(19,23,34,0.9); padding:10px; border-radius:6px; border:1px solid #2962FF; pointer-events:none; }}
                </style></head><body><div id="chart"><div class="tv-watermark">{t4_sid_clean} {stock_name}</div><div id="legend" class="tv-legend"></div></div><script>
                const data = {json.dumps(plot_data)};
                const chart = LightweightCharts.createChart(document.getElementById('chart'), {{ layout:{{backgroundColor:'#131722',textColor:'#d1d4dc'}}, grid:{{vertLines:{{color:'#242733'}},horzLines:{{color:'#242733'}}}}, crosshair:{{mode:LightweightCharts.CrosshairMode.Normal}} }});
                const k = chart.addCandlestickSeries({{upColor:'#ef5350',downColor:'#26a69a',borderVisible:false,wickUpColor:'#ef5350',wickDownColor:'#26a69a'}});
                k.setData(data.map(d=>({{time:d.t,open:d.o,high:d.h,low:d.l,close:d.c}})));
                const mid = chart.addLineSeries({{color:'#FFD600',lineWidth:1,priceLineVisible:false,lastValueVisible:false}});
                mid.setData(data.filter(d=>d.bm!==null).map(d=>({{time:d.t,value:d.bm}})));
                const up = chart.addLineSeries({{color:'rgba(255,255,255,0.4)',lineWidth:1,lineStyle:2,priceLineVisible:false,lastValueVisible:false}});
                up.setData(data.filter(d=>d.bu!==null).map(d=>({{time:d.t,value:d.bu}})));
                const dn = chart.addLineSeries({{color:'rgba(255,255,255,0.4)',lineWidth:1,lineStyle:2,priceLineVisible:false,lastValueVisible:false}});
                dn.setData(data.filter(d=>d.bd!==null).map(d=>({{time:d.t,value:d.bd}})));
                const vol = chart.addHistogramSeries({{priceScaleId:'',scaleMargins:{{top:0.85,bottom:0}},priceLineVisible:false,lastValueVisible:false}});
                vol.setData(data.map(d=>({{time:d.t,value:d.v,color:d.v>=0?'rgba(239,83,80,0.5)':'rgba(38,166,154,0.5)'}})));
                const h1 = chart.addHistogramSeries({{priceScaleId:'m1',scaleMargins:{{top:0.7,bottom:0.15}},priceLineVisible:false,lastValueVisible:false}});
                h1.setData(data.filter(d=>d.h1!==null).map(d=>({{time:d.t,value:d.h1,color:d.h1>=0?'#ef5350':'#26a69a'}})));
                chart.priceScale('m1').applyOptions({{visible:false}});
                const leg = document.getElementById('legend');
                chart.subscribeCrosshairMove(p => {{
                    if(!p.time) return; const d = data.find(x=>x.t===p.time); if(!d) return;
                    leg.innerHTML = `<div style="color:#2962FF;font-weight:bold;margin-bottom:5px;">{t4_sid_clean} {stock_name} | ${{p.time}}</div>
                    開: ${{d.o.toFixed(2)}} | 高: <span style="color:#ef5350">${{d.h.toFixed(2)}}</span><br>低: <span style="color:#26a69a">${{d.l.toFixed(2)}}</span> | 收: <b>${{d.c.toFixed(2)}}</b><br>
                    買賣超: <span style="color:${{d.v>=0?'#ef5350':'#26a69a'}}">${{d.v}} 張</span><hr style="border-color:#444;margin:5px 0;">
                    短MACD: ${{d.h1.toFixed(2)}} | 長MACD: ${{d.h2.toFixed(2)}}`;
                }});
                chart.timeScale().fitContent();
                </script></body></html>"""
                components.html(html_code, height=850)
            except Exception as e: st.error(f"錯誤: {e}")
