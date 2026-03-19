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

st.set_page_config(page_title="籌碼尋寶 Pro", layout="wide")

# ==========================================
# 0. 完整數據庫 (分段定義，確保不被截斷)
# ==========================================
B1 = '1020,合庫!1020,合庫;1030,土銀!1030,土銀!1039,土銀-士林!1031,土銀-台中!1032,土銀-台南!1036,土銀-玉里!0031003000330043,土銀-白河!1038,土銀-和平!1037,土銀-花蓮!0031003000330046,土銀-南港!0031003000330041,土銀-建國!1033,土銀-高雄!1035,土銀-新竹!1034,土銀-嘉義!0031003000330042,土銀-彰化;1040,臺銀!1040,臺銀!1043,臺銀-民權!0031003000340044,臺銀-金山!0031003000340043,臺銀-高雄!1045,臺銀-新竹!0031003000340041,臺銀-臺中!1042,臺銀-臺南!1041,臺銀-鳳山(停);1110,台灣企銀!1110,台灣企銀!1113,台灣企銀-九如!0031003100310043,台灣企銀-三民(停)!1115,台灣企銀-太平!0031003100310046,台灣企銀-北高雄!1111,台灣企銀-台中!1112,台灣企銀-台南!0031003100310041,台灣企銀-民雄!1117,台灣企銀-竹北!1119,台灣企銀-岡山!1116,台灣企銀-屏東!0031003100310042,台灣企銀-建成!0031003100310047,台灣企銀-埔墘!0031003100310045,台灣企銀-桃園!1114,台灣企銀-嘉義!1118,台灣企銀-豐原;1230,彰銀!1230,彰銀!1232,彰銀-七賢!1233,彰銀-台中;1260,宏遠!1260,宏遠!0031003200360048,宏遠-中和!003100320036004c,宏遠-台中!0031003200360044,宏遠-台南!1261,宏遠-民生!0031003200360069,宏遠-光隆!1262,宏遠-桃園!0031003200360051,宏遠-高雄!0031003200360058,宏遠-新化!0031003200360055,宏遠-館前;1360,麥格理!1360,麥格理;1440,美林!1440,美林;1470,摩根士丹利!1470,摩根士丹利;1480,高盛!1480,高盛;1560,野村!1560,野村;1590,花旗環球!1590,花旗環球;1650,瑞銀!1650,瑞銀;2180,亞東!2180,亞東!2187,亞東-台中!2185,亞東-台南!2181,亞東-板橋!2186,亞東-高雄!0032003100380041,亞東-國際!2184,亞東-新竹;2200,元大期貨!2200,元大期貨;2210,群益期貨!2210,群益期貨;2300,國票期貨!2300,國票期貨;5050,大展!5050,大展!5058,大展-台南;5110,富隆!5110,富隆;'
B2 = '5260,美好!5260,美好!0035003200360041,美好-中和!5266,美好-中壢!5269,美好-台中(停)!5268,美好-台南!003500320036004d,美好-市政!5265,美好-苗栗!5263,美好-泰山!5264,美好-高雄!5267,美好-基隆!003500320036004b,美好-富順!5262,美好-楊梅!5261,美好-蘆洲;5320,高橋!5320,高橋!5322,高橋-中壢!5323,高橋-內壢!5321,高橋-龍潭;5380,第一金!5380,第一金!0035003300380046,第一金-大稻埕!5389,第一金-中山!0035003300380059,第一金-中壢!5382,第一金-台中!5384,第一金-台南!0035003300380043,第一金-光復!003500330038006a,第一金-安和!0035003300380045,第一金-自由!0035003300380044,第一金-忠孝!5381,第一金-员林!5385,第一金-桃園!5383,第一金-高雄!0035003300380061,第一金-國際!0035003300380057,第一金-華江!0035003300380042,第一金-新竹!0035003300380041,第一金-新興!003500330038004c,第一金-經紀部!0035003300380050,第一金-路竹!003500330038004d,第一金-嘉義!5386,第一金-彰化!5387,第一金-澎湖!0035003300380049,第一金-頭份!003500330038004e,第一金-豐原;5460,寶盛!5460,寶盛;5600,永興!5600,永興!5604,永興-大墩!5602,永興-水湳!5603,永興-台中;5660,日進!5660,日進;5850,統一!5850,統一!0035003800350051,統一-三多!003500380035004a,統一-三重!0035003800350059,統一-土城!003500380035004d,統一-士林!5853,統一-中壢!0035003800350063,統一-仁愛!0035003800350062,統一-內湖!5856,統一-台中!5855,統一-台南!0035003800350067,統一-平鎮!0035003800350042,統一-永和!003500380035006d,統一-竹南!0035003800350053,統一-宜蘭!0035003800350052,統一-東湖!0035003800350050,統一-板橋!003500380035005a,統一-松江!0035003800350057,統一-金門!0035003800350055,統一-南京!5854,統一-城中!5859,統一-屏東!0035003800350049,統一-員林!0035003800350048,統一-桃園!5851,統一-高雄!0035003800350073,統一-國際!0035003800350041,統一-基隆!5852,統一-敦南!0035003800350044,統一-新台中!5857,統一-新竹!0035003800350045,統一-新營!5858,統一-嘉義!0035003800350046,統一-彰化;5860,盈溢!5860,盈溢;5960,日茂!5960,日茂!5962,日茂-南投!5961,日茂-埔里;'
B3 = '6010,(牛牛牛)亞證券!6010,(牛牛牛)亞證券!6012,(牛牛牛)亞-網路!0036003000310064,(牛牛牛)亞-鑫豐;6110,台中銀!6110,台中銀;6160,中國信託!6160,中國信託!6161,中國信託-三重!0036003100360041,中國信託-中壢!6164,中國信託-文心!6163,中國信託-永康!6162,中國信託-忠孝!6167,中國信託-松江!003600310036004b,中國信託-高雄!0036003100360058,中國信託-國際!6165,中國信託-新竹!6168,中國信託-嘉義;6210,新百王!6210,新百王;6380,光和!6380,光和;6450,永全!6450,永全;6460,大昌!6460,大昌!6462,大昌-安康!6465,大昌-桃園!6463,大昌-新竹!6464,大昌-新店!6461,大昌-樹林;6480,福邦!6480,福邦證券!6489,福邦-新竹;6620,口袋!6620,口袋證券;6910,德信!6910,德信!6912,德信-中正!6915,德信-和平!6913,德信-新營;6950,福勝!6950,福勝;7000,兆豐!7000,兆豐證券!0037003000300068,兆豐-三民!7008,兆豐-三重!0037003000300053,兆豐-大同!003700300030006a,兆豐-大安!0037003000300052,兆豐-小港!0037003000300062,兆豐-中壢!0037003000300071,兆豐-內湖(停)!003700300030004a,兆豐-公益!003700300030004c,兆豐-天母!0037003000300049,兆豐-北高雄!7003,兆豐-台中!7005,兆豐-台中港!7006,兆豐-台南!0037003000300063,兆豐-民生!0037003000300058,兆豐-永和!7007,兆豐-竹北!0037003000300044,兆豐-西螺!0037003000300043,兆豐-來福!0037003000300073,兆豐-岡山!0037003000300061,兆豐-忠孝!0037003000300048,兆豐-東門!0037003000300042,兆豐-板橋!0037003000300069,兆豐-松德!0037003000300046,兆豐-虎尾!0037003000300050,兆豐-南京!0037003000300067,兆豐-南門!0037003000300057,兆豐-城中!003700300030006b,兆豐-員林!003700300030004e,兆豐-埔墘!003700300030004d,兆豐-桃園!0037003000300055,兆豐-桃鶯!003700300030005a,兆豐-高雄!003700300030006d,兆豐-國際!0037003000300066,兆豐-鹿港!0037003000300047,兆豐-麻豆!0037003000300070,兆豐-復興!7009,兆豐-景美(停)!0037003000300056,兆豐-新竹!0037003000300077,兆豐-新莊!003700300030004b,兆豐-新營!7001,兆豐-嘉義!0037003000300064,兆豐-彰化!0037003000300072,兆豐-寶成;7030,致和!7030,致和;7080,石橋!7080,石橋;7750,北城!7750,北城;7790,國票!7790,國票;'
B4 = '8150,台新!8150,台新證券!8156,台新-三民!0038003100350053,台新-中壢!0038003100350042,台新-台中!8159,台新-台南!8157,台新-左楠!8158,台新-松江!0038003100350048,台新-屏東!8151,台新-建北!0038003100350041,台新-高雄!003800310035005a,台新-國際!8152,台新-新莊!0038003100350059,台新-新營;8380,安泰!8380,安泰;8440,摩根大通!8440,摩根大通;8450,康和!8450,康和;8490,京城!8490,京城;8520,中農!8520,中農;8560,新光!8560,新光!8561,新光-台中!8564,新光-台南!8565,新光-桃園!8562,新光-高雄!8563,新光-新竹;8580,聯邦!8580,聯邦;8710,陽信!8710,陽信;8840,玉山!8840,玉山;8880,國泰!8880,國泰;8890,大和國泰!8890,大和國泰;8900,法銀巴黎!8900,法銀巴黎;8960,匯豐!8960,匯豐;9100,群益金鼎!9100,群益金鼎!9182,群益金鼎-總公司;9200,凱基!9200,凱基!9205,凱基-三重!9204,凱基-台中;9300,華南永昌!9300,華南永昌!9369,華南永昌-三重!9315,華南永昌-大甲!9307,華南永昌-大安!9323,華南永昌-小港!9359,華南永昌-中正!9337,華南永昌-內壢!9327,華南永昌-斗六!9334,華南永昌-世貿!9309,華南永昌-古亭!9302,華南永昌-台中!9306,華南永昌-台南!9312,華南永昌-民權!9329,華南永昌-朴子!9339,華南永昌-竹北!9324,華南永昌-岡山!9325,華南永昌-忠孝!9386,華南永昌-東昇!9358,華南永昌-東勢!9317,華南永昌-林口!9349,華南永昌-板橋!9377,華南永昌-虎尾!9333,華南永昌-長虹!9326,華南永昌-南京!9362,華南永昌-苗栗!9352,華南永昌-桃園!9303,華南永昌-高雄!9399,華南永昌-國際!9322,華南永昌-基隆!9316,華南永昌-淡水!9308,華南永昌-麻豆!9347,華南永昌-敦南!9305,華南永昌-新莊!9332,華南永昌-楠梓!9314,華南永昌-嘉義!9363,華南永昌-彰化!9331,華南永昌-鳳山!9328,華南永昌-潮州!9343,華南永昌-頭份!9366,華南永昌-豐原!9319,華南永昌-鶯歌;9600,富邦!9600,富邦證券!9677,富邦-三重!9636,富邦-中壢!9672,富邦-員林;9800,元大!9800,元大證券!9891,元大-永和!9852,元大-永春!9829,元大-永康!9824,元大-向上;9A00,永豐金!9A00,永豐金;9B00,元富!9B00,元富;5460,寶盛!5460,寶盛'

RAW_DATA_STR = B1 + B2 + B3 + B4

# --- 數據解析邏輯 ---
def parse_broker_logic(raw):
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
        tree[bname] = {"bid": bid, "branches": branches}
    return tree, name_map

FULL_TREE, BROKER_NAME_MAP = parse_broker_logic(RAW_DATA_STR)

def get_stock_id(name_str):
    match = re.search(r'\d{4,}', str(name_str))
    return match.group(0) if match else None

# TradingView 互動圖表元件
def render_tradingview(symbol):
    if not symbol: return
    tv_code = f"""
    <div class="tradingview-widget-container" style="height:550px; width:100%;">
      <div id="tradingview_chart"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
      <script type="text/javascript">
      new TradingView.widget({{
        "autosize": true, "symbol": "TWSE:{symbol}", "interval": "D", "timezone": "Asia/Taipei",
        "theme": "dark", "style": "1", "locale": "zh_TW", "toolbar_bg": "#f1f3f6",
        "enable_publishing": false, "container_id": "tradingview_chart"
      }});
      </script>
    </div>
    """
    components.html(tv_code, height=560)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}

# ==========================================
# 1. UI 介面設計 (分頁)
# ==========================================
tab1, tab2 = st.tabs(["🚀 券商分點查股票 (尋寶雷達)", "📊 股票代號查分點 (籌碼追蹤)"])

# --- Tab 1: 券商查股票 ---
with tab1:
    st.markdown("### 🏦 追蹤特定分點進出")
    c1, c2 = st.columns(2)
    with c1: sel_broker = st.selectbox("選擇券商", sorted(FULL_TREE.keys()), key="t1_broker")
    with c2: 
        b_opts = FULL_TREE[sel_broker]['branches']
        sel_br_label = st.selectbox("選擇分點", list(b_opts.keys()), key="t1_branch")
        sel_br_id = b_opts[sel_br_label]

    c3, c4, c5 = st.columns(3)
    with c3: t1_sd = st.date_input("開始日期", datetime.date.today()-datetime.timedelta(days=1), key="t1_sd")
    with c4: t1_ed = st.date_input("結束日期", datetime.date.today(), key="t1_ed")
    with c5: t1_unit = st.radio("單位", ["張數", "金額"], horizontal=True, key="t1_u")

    c6, c7, c8 = st.columns([2, 1, 1])
    with c6: t1_mode = st.radio("篩選模式", ["嚴格模式 (絕對持股)", "濾網模式 (佔比篩選)"], horizontal=True, key="t1_m")
    with c7: t1_th_pct = st.number_input("門檻佔比%", 0, 100, 95, key="t1_p")
    with c8: t1_th_val = st.number_input(f"最低成交門檻", 0, 1000000, 10, key="t1_v")

    if st.button("開始分點尋寶 🚀", key="t1_btn"):
        sd_s, ed_s = t1_sd.strftime('%Y-%m-%d'), t1_ed.strftime('%Y-%m-%d')
        bid = FULL_TREE[sel_broker]['bid']
        c_p = "B" if t1_unit == "金額" else "E"
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={bid}&b={sel_br_id}&c={c_p}&e={sd_s}&f={ed_s}"
        
        try:
            res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
            res.encoding = 'big5'
            html = re.sub(r"<script[^>]*>.*?GenLink2stk\s*\(\s*['\"](?:AS)?([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\).*?</script>", r"\1\2", res.text, flags=re.I|re.S)
            tables = pd.read_html(StringIO(html))
            df_raw = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] >= 3 and any(x in str(tb) for x in ['買進','賣出']):
                    if tb.shape[1] >= 8:
                        l = tb.iloc[:,[0,1,2]].copy(); l.columns=['股票','買','賣']
                        r = tb.iloc[:,[5,6,7]].copy(); r.columns=['股票','買','賣']
                        df_raw = pd.concat([df_raw, l, r])
                    else:
                        t = tb.iloc[:,[0,1,2]].copy(); t.columns=['股票','買','賣']
                        df_raw = pd.concat([df_raw, t])

            if not df_raw.empty:
                df_raw['股票'] = df_raw['股票'].astype(str).str.strip()
                df_raw = df_raw[~df_raw['股票'].str.contains('名稱|買進|賣出|合計|註|說明', na=False)]
                for c in ['買','賣']: df_raw[c] = pd.to_numeric(df_raw[c].astype(str).str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)
                df_raw['總'] = df_raw['買'] + df_raw['賣']
                df_raw = df_raw[df_raw['總'] > 0].copy()
                df_raw['買%'] = (df_raw['買']/df_raw['總']*100).round(1)
                df_raw['賣%'] = (df_raw['賣']/df_raw['總']*100).round(1)

                if t1_mode == "嚴格模式 (絕對持股)":
                    b_final = df_raw[(df_raw['買'] >= t1_th_val) & (df_raw['賣'] == 0)]
                    s_final = df_raw[(df_raw['賣'] >= t1_th_val) & (df_raw['買'] == 0)]
                else:
                    b_final = df_raw[(df_raw['買%'] >= t1_th_pct) & (df_raw['買'] >= t1_th_val)]
                    s_final = df_raw[(df_raw['賣%'] >= t1_th_pct) & (df_raw['賣'] >= t1_th_val)]

                b_final = b_final.sort_values('買', ascending=False).reset_index(drop=True)
                s_final = s_final.sort_values('賣', ascending=False).reset_index(drop=True)
                
                # 添加連結
                for d in [b_final, s_final]:
                    d['前往富邦分析'] = d['股票'].apply(lambda x: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{get_stock_id(x)}.djhtm" if get_stock_id(x) else "")

                st.subheader(f"🔴 分點吃貨 (買進) - {len(b_final)} 檔")
                st.dataframe(b_final.rename(columns={'買':f'買進{t1_unit}','賣':f'賣出{t1_unit}','總':f'合計{t1_unit}'}), 
                             hide_index=True, column_config={"前往富邦分析": st.column_config.LinkColumn("點我看圖")}, use_container_width=True)
                
                st.subheader(f"🟢 分點出貨 (賣出) - {len(s_final)} 檔")
                st.dataframe(s_final.rename(columns={'買':f'買進{t1_unit}','賣':f'賣出{t1_unit}','總':f'合計{t1_unit}'}), 
                             hide_index=True, column_config={"前往富邦分析": st.column_config.LinkColumn("點我看圖")}, use_container_width=True)
                
                if not b_final.empty:
                    pick = st.selectbox("⚡ 選擇股票預覽即時線圖", b_final['股票'].tolist(), key="t1_pick")
                    sid = get_stock_id(pick)
                    if sid: render_tradingview(sid)
            else: st.warning("無符合資料")
        except Exception as e: st.error(f"查詢錯誤: {e}")

# --- Tab 2: 股票查分點 ---
with tab2:
    st.markdown("### 📈 這檔股票是誰在買賣？")
    c1, c2, c3 = st.columns(3)
    with c1: t2_sid = st.text_input("股票代號", "3702", key="t2_sid")
    with c2: t2_sd = st.date_input("開始日期", datetime.date.today()-datetime.timedelta(days=7), key="t2_sd")
    with c3: t2_ed = st.date_input("結束日期", datetime.date.today(), key="t2_ed")

    c4, c5, c6 = st.columns([2, 1, 1])
    with c4: t2_m = st.radio("模式", ["嚴格模式 (區間全持有)", "濾網模式 (佔比篩選)"], horizontal=True, key="t2_m")
    with c5: t2_th_p = st.number_input("成交佔比門檻%", 0, 100, 95, key="t2_p")
    with c6: t2_th_v = st.number_input("最低成交張數", 0, 1000000, 10, key="t2_v")

    if st.button("開始籌碼追蹤 🚀", key="t2_btn"):
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={t2_sid}&e={t2_sd.strftime('%Y-%m-%d')}&f={t2_ed.strftime('%Y-%m-%d')}"
        try:
            res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
            res.encoding = 'big5'
            tables = pd.read_html(StringIO(res.text))
            df_all = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] == 10:
                    l = tb.iloc[:,[0,1,2]].copy(); l.columns=['券商名稱','買','賣']
                    r = tb.iloc[:,[5,6,7]].copy(); r.columns=['券商名稱','買','賣']
                    df_all = pd.concat([l, r])
            
            if not df_all.empty:
                df_all = df_all.dropna()
                df_all = df_all[~df_all['券商名稱'].str.contains('券商|合計|平均|說明|註', na=False)]
                for c in ['買','賣']: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)
                df_all['合計'] = df_all['買'] + df_all['賣']
                df_all = df_all[df_all['合計'] > 0].copy()
                df_all['買進%'] = (df_all['買']/df_all['合計']*100).round(1)
                df_all['賣出%'] = (df_all['賣']/df_all['合計']*100).round(1)

                if t2_m == "嚴格模式 (區間全持有)":
                    b_df = df_all[(df_all['買'] >= t2_th_v) & (df_all['賣'] == 0)]
                    s_df = df_all[(df_all['賣'] >= t2_th_v) & (df_all['買'] == 0)]
                else:
                    b_df = df_all[(df_all['買進%'] >= t2_th_p) & (df_all['買'] >= t2_th_v)]
                    s_df = df_all[(df_all['賣出%'] >= t2_th_p) & (df_all['賣'] >= t2_th_v)]
                
                b_df = b_df.sort_values('買', ascending=False).reset_index(drop=True)
                s_df = s_df.sort_values('賣', ascending=False).reset_index(drop=True)

                def get_fubon_url(n):
                    info = BROKER_NAME_MAP.get(n.replace("亚","亞"))
                    return f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?a={t2_sid}&BHID={info['b']}&b={info['b']}&C=3" if info else ""

                for d in [b_df, s_df]: d['前往明細'] = d['券商名稱'].apply(get_fubon_url)
                
                st.subheader(f"🔴 吃貨主力分點 - 共 {len(b_df)} 家")
                st.dataframe(b_df.rename(columns={'買':'買進張數','賣':'賣出張數'}), hide_index=True, column_config={"前往明細": st.column_config.LinkColumn("查看明細")}, use_container_width=True)
                
                st.subheader(f"🟢 倒貨主力分點 - {len(s_df)} 家")
                st.dataframe(s_df.rename(columns={'買':'買進張數','賣':'賣出張數'}), hide_index=True, column_config={"前往明細": st.column_config.LinkColumn("查看明細")}, use_container_width=True)
            else: st.warning("未找到資料。")
        except Exception as e: st.error(f"錯誤: {e}")
