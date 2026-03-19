import streamlit as st
import pandas as pd
import requests
from io import StringIO
import re
import datetime
import urllib3
import streamlit.components.v1 as components

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="籌碼尋寶雷達 Pro", layout="wide")

# ==========================================
# 0. 完整數據庫建置 (分段組合，確保從 1020 到 5460 完整)
# ==========================================
B1 = '1020,合庫!1020,合庫;1030,土銀!1030,土銀!1039,土銀-士林!1031,土銀-台中!1032,土銀-台南!1036,土銀-玉里!0031003000330043,土銀-白河!1038,土銀-和平!1037,土銀-花蓮!0031003000330046,土銀-南港!0031003000330041,土銀-建國!1033,土銀-高雄!1035,土銀-新竹!1034,土銀-嘉義!0031003000330042,土銀-彰化;1040,臺銀證券!1040,臺銀證券!1043,臺銀-民權!0031003000340044,臺銀-金山!0031003000340043,臺銀-高雄!1045,臺銀-新竹!0031003000340041,臺銀-臺中!1042,臺銀-臺南!1041,臺銀-鳳山(停);1110,台灣企銀!1110,台灣企銀!1113,台灣企銀-九如!0031003100310043,台灣企銀-三民(停)!1115,台灣企銀-太平!0031003100310046,台灣企銀-北高雄!1111,台灣企銀-台中!1112,台灣企銀-台南!0031003100310041,台灣企銀-民雄!1117,台灣企銀-竹北!1119,台灣企銀-岡山!1116,台灣企銀-屏東!0031003100310042,台灣企銀-建成!0031003100310047,台灣企銀-埔墘!0031003100310045,台灣企銀-桃園!1114,台灣企銀-嘉義!1118,台灣企銀-豐原;1230,彰銀!1230,彰銀!1232,彰銀-七賢!1233,彰銀-台中;1260,宏遠!1260,宏遠!1360,港商麥格理!1360,港商麥格理;1440,美林!1440,美林;1470,台灣摩根士丹利!1470,台灣摩根士丹利;1480,美商高盛!1480,美商高盛;1560,港商野村!1560,港商野村;1590,花旗環球!1590,花旗環球;1650,新加坡商瑞銀!1650,新加坡商瑞銀;2180,亞東!2180,亞東!2187,亞東-台中!2185,亞東-台南!2181,亞東-板橋!2186,亞東-高雄!0032003100380041,亞東-國際!2184,亞東-新竹;'
B2 = '2200,元大期貨!2200,元大期貨;2210,群益期貨!2210,群益期貨;2300,國票期貨!2300,國票期貨;5050,大展!5050,大展!5058,大展-台南;5110,富隆!5110,富隆;5260,美好!5260,美好!0035003200360041,美好-中和!5266,美好-中壢!5269,美好-台中(停)!5268,美好-台南!003500320036004d,美好-市政!5265,美好-苗栗!5263,美好-泰山!5264,美好-高雄!5267,美好-基隆!003500320036004b,美好-富順!5262,美好-楊梅!5261,美好-蘆洲;5320,高橋!5320,高橋!5322,高橋-中壢!5323,高橋-內壢!5321,高橋-龍潭;5380,第一金!5380,第一金!5389,第一金-中山!5382,第一金-台中!5384,第一金-台南!5381,第一金-員林!5385,第一金-桃園!5383,第一金-高雄!5386,第一金-彰化!5387,第一金-澎湖;5460,寶盛!5460,寶盛;5600,永興!5600,永興!5604,永興-大墩!5602,永興-水湳!5603,永興-台中;5660,日進!5660,日進;'
B3 = '5850,統一!5850,統一!5856,統一-台中!5855,統一-台南!5854,統一-城中!5859,統一-屏東!5851,統一-高雄!5852,統一-敦南!5857,統一-新竹!5858,統一-嘉義;5860,盈溢!5860,盈溢;5960,日茂!5960,日茂!5962,日茂-南投!5961,日茂-埔里;6010,犇亞證券!6010,(牛牛牛)亞證券!6012,(牛牛牛)亞-網路!0036003000310064,(牛牛牛)亞-鑫豐;6110,台中銀!6110,台中銀;6160,中國信託!6160,中國信託!6161,中國信託-三重!6164,中國信託-文心!6163,中國信託-永康!6162,中國信託-忠孝!6167,中國信託-松江!6165,中國信託-新竹!6168,中國信託-嘉義;6210,新百王!6210,新百王;6380,光和!6380,光和;6450,永全!6450,永全;6460,大昌!6460,大昌!6462,大昌-安康!6465,大昌-桃園!6463,大昌-新竹!6464,大昌-新店!6461,大昌-樹林;6480,福邦!6480,福邦證券!6489,福邦-新竹;6620,口袋!6620,口袋證券;6910,德信!6910,德信!6912,德信-中正!6915,德信-和平!6913,德信-新營;6950,福勝!6950,福勝;7000,兆豐!7000,兆豐證券!7008,兆豐-三重!7003,兆豐-台中!7005,兆豐-台中港!7006,兆豐-台南!7007,兆豐-竹北!7009,兆豐-景美;7030,致和!7030,致和;7080,石橋!7080,石橋;7750,北城!7750,北城;'
B4 = '7790,國票!7790,國票證券;8150,台新!8150,台新證券!8156,台新-三民!8159,台新-台南!8157,台新-左楠!8158,台新-松江!8151,台新-建北!8152,台新-新莊;8380,安泰!8380,安泰;8440,摩根大通!8440,摩根大通;8450,康和!8450,康和!8455,康和-台中!8458,康和-台南!8451,康和-延平!8456,康和-新竹!8459,康和-嘉義;8490,京城!8490,京城;8520,中農!8520,中農;8560,新光!8560,新光!8561,新光-台中!8564,新光-台南!8565,新光-桃園!8562,新光-高雄!8563,新光-新竹;8580,聯邦商銀!8580,聯邦商銀;8710,陽信!8710,陽信;8840,玉山!8840,玉山證券;8880,國泰!8880,國泰證券!8882,國泰-台中!8884,國泰-台南!8887,國泰-忠孝!8883,國泰-松江!8885,國泰-桃園!8881,國泰-高雄!8888,國泰-敦南!8886,國泰-新莊;8890,大和國泰!8890,大和國泰;8900,法銀巴黎!8900,法銀巴黎;8960,香港上海匯豐!8960,香港上海匯豐;9100,群益金鼎!9100,群益金鼎;9200,凱基!9200,凱基;9300,華南永昌!9300,華南永昌!9600,富邦!9600,富邦證券;9800,元大!9800,元大證券;9A00,永豐金!9A00,永豐金;9B00,元富!9B00,元富'

FULL_RAW_DATA = B1 + B2 + B3 + B4

# --- 數據庫解析邏輯 ---
def build_db(raw):
    tree = {}; name_map = {}
    for group in raw.split(';'):
        if not group: continue
        parts = group.split('!')
        head = parts[0].split(',')
        bid, bname = head[0], head[1].replace("亚","亞")
        branches = {}
        for p in parts:
            if ',' in p:
                br_id, br_name = p.split(',')[0], p.split(',')[1].replace("亚","亞")
                branches[br_name] = br_id
                name_map[br_name] = {"a": bid, "b": br_id}
    return tree, name_map

# 重新建立清單
def parse_for_ui(raw):
    t = {}
    for group in raw.split(';'):
        if not group: continue
        parts = group.split('!')
        head = parts[0].split(',')
        bid, bname = head[0], head[1].replace("亚","亞")
        branches = {p.split(',')[1].replace("亚","亞"): p.split(',')[0] for p in parts if ',' in p}
        t[bname] = {"bid": bid, "branches": branches}
    return t

UI_TREE = parse_for_ui(FULL_RAW_DATA)
_, BROKER_MAP = build_db(FULL_RAW_DATA)

def get_stock_id(name_str):
    match = re.search(r'\d{4,}', str(name_str))
    return match.group(0) if match else None

# TradingView 互動圖表
def render_tradingview(symbol):
    if not symbol: return
    tv_code = f"""
    <div id="tradingview_chart" style="height:600px; width:100%;"></div>
    <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
    <script type="text/javascript">
    new TradingView.widget({{
      "autosize": true, "symbol": "TWSE:{symbol}", "interval": "D", "timezone": "Asia/Taipei",
      "theme": "dark", "style": "1", "locale": "zh_TW", "toolbar_bg": "#f1f3f6",
      "enable_publishing": false, "container_id": "tradingview_chart"
    }});
    </script>
    """
    components.html(tv_code, height=620)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}

# ==========================================
# 1. 介面設定
# ==========================================
tab1, tab2 = st.tabs(["🚀 券商分點查股票", "📊 股票代號查分點"])

# --- Tab 1: 券商查股票 (套用 Colab 邏輯) ---
with tab1:
    st.markdown("### 🏦 追蹤特定分點進出")
    c1, c2 = st.columns(2)
    with c1: sel_broker = st.selectbox("選擇券商", sorted(UI_TREE.keys()), key="t1_broker_sel")
    with c2: 
        b_opts = UI_TREE[sel_broker]['branches']
        sel_br_label = st.selectbox("選擇分點", list(b_opts.keys()), key="t1_branch_sel")
        sel_br_id = b_opts[sel_br_label]

    c3, c4, c5 = st.columns(3)
    with c3: t1_sd = st.date_input("開始日期", datetime.date.today()-datetime.timedelta(days=1), key="t1_sd_in")
    with c4: t1_ed = st.date_input("結束日期", datetime.date.today(), key="t1_ed_in")
    with c5: t1_unit = st.radio("單位", ["張數", "金額"], horizontal=True, key="t1_u_rad")

    c6, c7, c8 = st.columns([2, 1, 1])
    with c6: t1_mode = st.radio("模式", ["嚴格模式", "濾網模式"], horizontal=True, key="t1_m_rad")
    with c7: t1_th_pct = st.number_input("買進佔比門檻%", 0.0, 100.0, 95.0, key="t1_p_in")
    with c8: t1_th_val = st.number_input(f"最低成交門檻", 0, 1000000, 10, key="t1_v_in")

    if st.button("開始分點尋寶 🚀", key="t1_go"):
        sd_s, ed_s = t1_sd.strftime('%Y-%m-%d'), t1_ed.strftime('%Y-%m-%d')
        bid = UI_TREE[sel_broker]['bid']
        c_p = "B" if t1_unit == "金額" else "E"
        
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={bid}&b={sel_br_id}&c={c_p}&e={sd_s}&f={ed_s}"
        
        try:
            res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
            res.encoding = 'big5'
            html = re.sub(r"<script[^>]*>.*?GenLink2stk\s*\(\s*['\"](?:AS)?([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\).*?</script>", r"\1\2", res.text, flags=re.I|re.S)
            tables = pd.read_html(StringIO(html))
            
            df_all = pd.DataFrame()
            # 【Colab 核心邏輯】
            for tb in tables:
                if tb.shape[1] >= 3 and any(x in str(tb) for x in ['買進','賣出','張數','金額']):
                    if tb.shape[1] >= 8:
                        l_df = tb.iloc[:, [0, 1, 2]].copy(); l_df.columns = ['股票名稱', '買', '賣']
                        r_df = tb.iloc[:, [5, 6, 7]].copy(); r_df.columns = ['股票名稱', '買', '賣']
                        df_all = pd.concat([df_all, l_df, r_df], ignore_index=True)
                    else:
                        temp = tb.iloc[:, [0, 1, 2]].copy(); temp.columns = ['股票名稱', '買', '賣']
                        df_all = pd.concat([df_all, temp], ignore_index=True)

            if not df_all.empty:
                df_all['股票名稱'] = df_all['股票名稱'].astype(str).str.strip()
                df_all = df_all[~df_all['股票名稱'].str.contains('名稱|買進|賣出|合計|註|說明|請選擇', na=False)]
                for c in ['買','賣']: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
                df_all['總額'] = df_all['買'] + df_all['賣']
                df_all = df_all[df_all['總額'] > 0].copy()
                df_all['買佔%'] = (df_all['買'] / df_all['總額'] * 100).round(1)
                df_all['賣佔%'] = (df_all['賣'] / df_all['總額'] * 100).round(1)

                # --- 篩選邏輯分離 ---
                if "嚴格" in t1_mode:
                    only_buy = df_all[(df_all['買'] >= t1_th_val) & (df_all['賣'] == 0)]
                    only_sell = df_all[(df_all['賣'] >= t1_th_val) & (df_all['買'] == 0)]
                else:
                    only_buy = df_all[(df_all['買佔%'] >= t1_th_pct) & (df_all['買'] >= t1_th_val)]
                    only_sell = df_all[(df_all['賣佔%'] >= t1_th_pct) & (df_all['賣'] >= t1_th_val)]

                only_buy = only_buy.sort_values('買', ascending=False).reset_index(drop=True)
                only_sell = only_sell.sort_values('賣', ascending=False).reset_index(drop=True)
                
                for d in [only_buy, only_sell]:
                    d['點我看圖'] = d['股票名稱'].apply(lambda x: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{get_stock_id(x)}.djhtm" if get_stock_id(x) else "")

                st.subheader(f"🔴 分點吃貨 (買進) - {len(only_buy)} 檔")
                st.dataframe(only_buy.rename(columns={'買':f'買進{t1_unit}','賣':f'賣出{t1_unit}','總額':f'合計{t1_unit}'}), hide_index=True, column_config={"點我看圖": st.column_config.LinkColumn("外部頁面")}, use_container_width=True)
                
                st.subheader(f"🟢 分點倒貨 (賣出) - {len(only_sell)} 檔")
                st.dataframe(only_sell.rename(columns={'買':f'買進{t1_unit}','賣':f'賣出{t1_unit}','總額':f'合計{t1_unit}'}), hide_index=True, column_config={"點我看圖": st.column_config.LinkColumn("外部頁面")}, use_container_width=True)
                
                if not only_buy.empty:
                    pick = st.selectbox("⚡ 選擇股票預覽即時線圖", only_buy['股票名稱'].tolist(), key="t1_picker")
                    sid = get_stock_id(pick)
                    if sid: render_tradingview(sid)
            else: st.warning("無符合資料")
        except Exception as e: st.error(f"錯誤: {e}")

# --- Tab 2: 股票查分點 (不更動邏輯) ---
with tab2:
    st.markdown("### 📈 誰在買賣這檔股票？")
    c1, c2, c3 = st.columns(3)
    with c1: t2_sid = st.text_input("股票代號", "3702", key="t2_stock_in")
    with c2: t2_sd = st.date_input("開始日期", datetime.date.today()-datetime.timedelta(days=7), key="t2_sd_in")
    with c3: t2_ed = st.date_input("結束日期", datetime.date.today(), key="t2_ed_in")

    c4, c5, c6 = st.columns([2, 1, 1])
    with c4: t2_m = st.radio("模式", ["嚴格模式", "濾網模式"], horizontal=True, key="t2_m_rad")
    with c5: t2_th_p = st.number_input("門檻佔比%", 0, 100, 95, key="t2_p_in")
    with c6: t2_th_v = st.number_input("最低成交張數", 0, 1000000, 10, key="t2_v_in")

    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
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
                for c in ['買','賣']: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)
                df_all['總計'] = df_all['買'] + df_all['賣']
                df_all = df_all[df_all['總計'] > 0].copy()
                df_all['買%'] = (df_all['買']/df_all['總計']*100).round(1)
                df_all['賣%'] = (df_all['賣']/df_all['總計']*100).round(1)

                if "嚴格" in t2_m:
                    b_df = df_all[(df_all['買'] >= t2_th_v) & (df_all['賣'] == 0)]
                    s_df = df_all[(df_all['賣'] >= t2_th_v) & (df_all['買'] == 0)]
                else:
                    b_df = df_all[(df_all['買%'] >= t2_th_p) & (df_all['買'] >= t2_th_v)]
                    s_df = df_all[(df_all['賣%'] >= t2_th_p) & (df_all['賣'] >= t2_th_v)]
                
                b_df = b_df.sort_values('買', ascending=False).reset_index(drop=True)
                s_df = s_df.sort_values('賣', ascending=False).reset_index(drop=True)

                def get_link(n):
                    name = n.replace("亚","亞").strip()
                    info = BROKER_MAP.get(name)
                    if not info:
                        for k, v in BROKER_MAP.items():
                            if name in k: info = v; break
                    return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid}&BHID={info['b']}&b={info['b']}&C=3" if info else ""

                for d in [b_df, s_df]: d['前往明細'] = d['券商'].apply(get_link)
                
                st.subheader(f"🔴 吃貨主力分點 - {len(b_df)} 家")
                st.dataframe(b_df.rename(columns={'買':'買進張數','賣':'賣出張數'}), hide_index=True, column_config={"前往明細": st.column_config.LinkColumn("查看分點詳情")}, use_container_width=True)
                
                st.subheader(f"🟢 倒貨主力分點 - {len(s_df)} 家")
                st.dataframe(s_df.rename(columns={'買':'買進張數','賣':'賣出張數'}), hide_index=True, column_config={"前往明細": st.column_config.LinkColumn("查看分點詳情")}, use_container_width=True)
            else: st.warning("未找到資料。")
        except Exception as e: st.error(f"錯誤: {e}")
