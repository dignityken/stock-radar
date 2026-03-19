import streamlit as st
import pandas as pd
import requests
from io import StringIO
import re
import datetime
import urllib3
import streamlit.components.v1 as components

# 修正：忽略 SSL 憑證警告訊息
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定網頁標題與圖示
st.set_page_config(page_title="籌碼尋寶雷達 Pro", layout="wide")

# ==========================================
# 0. 共用數據與解析邏輯
# ==========================================
RAW_DATA_STR = '6010,(牛牛牛)亞證券!6010,(牛牛牛)亞證券!6012,(牛牛牛)亞-網路!0036003000310064,(牛牛牛)亞-鑫豐;6620,口袋證券!6620,口袋證券;1030,土銀!1030,土銀!1039,土銀-士林!1031,土銀-台中!1032,土銀-台南!1036,土銀-玉里!0031003000330043,土銀-白河!1038,土銀-和平!1037,土銀-花蓮!0031003000330046,土銀-南港!0031003000330041,土銀-建國!1033,土銀-高雄!1035,土銀-新竹!1034,土銀-嘉義!0031003000330042,土銀-彰化;8890,大和國泰!8890,大和國泰;6460,大昌!6460,大昌!6462,大昌-安康!6465,大昌-桃園!6463,大昌-新竹!6464,大昌-新店!6461,大昌-樹林;5050,大展!5050,大展!5058,大展-台南;8770,大鼎(停)!8770,大鼎(停);6160,中國信託!6160,中國信託!6161,中國信託-三重!0036003100360041,中國信託-中壢!6164,中國信託-文心!6163,中國信託-永康!6162,中國信託-忠孝!6167,中國信託-松江!003600310036004b,中國信託-高雄!0036003100360058,中國信託-國際證券!6165,中國信託-新竹!6168,中國信託-嘉義;8520,中農!8520,中農;9800,元大!9800,元大證券!9813,元大-八德!0039003800390046,元大-三民!0039003800380043,元大-三重!0039003800310041,元大-三峽!0039003800330047,元大-上新莊!9897,元大-土城!9875,元大-土城永寧!9822,元大-土城學府!003900380031006a,元大-士林!9893,元大-大天母!0039003800390055,元大-大甲!0039003800390047,元大-大同!003900380033004e,元大-大安!0039003800310067,元大-大里!0039003800330057,元大-大里德芳!9846,元大-大直!9814,元大-大益!003900380031007a,元大-大統!0039003800330069,元大-大雅!9815,元大-大灣!003900380039005a,元大-小港!0039003800300061,元大-中山北路!0039003800390065,元大-中和!9834,元大-中壢!003900380033006a,元大-仁愛!003900380039004e,元大-內湖!9867,元大-內湖民權!0039003800320043,元大-六合!0039003800310049,元大-天母!0039003800310051,元大-太平!9868,元大-文心!9854,元大-文心興安!0039003800300043,元大-斗六!9884,元大-斗信!003900380033004d,元大-木柵!0039003800390056,元大-北三重!0039003800390064,元大-北屯!0039003800390041,元大-北投!0039003800300044,元大-北府!9837,元大-北港!003900380039006b,元大-古亭!9812,元大-台中!0039003800390042,元大-台中中港!0039003800300068,元大-台北!9896,元大-台南!9831,元大-四維!0039003800390044,元大-左營!0039003800390058,元大-民生三民!0039003800310056,元大-民雄!9891,元大-永和!9852,元大-永春!9829,元大-永康!9824,元大-向上!003900380039004c,元大-成功!0039003800390051,元大-汐止!0039003800310052,元大-竹山!0039003800310042,元大-竹北!0039003800330066,元大-竹東!0039003800330056,元大-竹南!003900380030004b,元大-竹科!0039003800300077,元大-西屯!9873,元大-西門(停)!0039003800310069,元大-西螺!0039003800300064,元大-沙鹿!0039003800310065,元大-佳里!9863,元大-和平!0039003800350046,元大-岡山!0039003800300075,元大-府城!9871,元大-忠孝!0039003800320046,元大-忠孝鼎富!9817,元大-東泰!0039003800390066,元大-東港!0039003800330042,元大-林森!003900380031004b,元大-林園!0039003800390043,元大-板橋!9879,元大-板橋三民!0039003800310070,元大-松山!9801,元大-松江!9856,元大-花蓮!003900380030006c,元大-虎尾!003900380031004e,元大-金門!9878,元大-金華!0039003800380056,元大-長庚!003900380039006d,元大-信義!9853,元大-南屯!0039003800310044,元大-南投!9862,元大-南京!0039003800300051,元大-南崁!0039003800300052,元大-南海!0039003800340045,元大-南勢角!9892,元大-屏東!0039003800310068,元大-屏東民生!0039003800310072,元大-屏南!003900380031004d,元大-苗栗!0039003800390061,元大-苑裡!9889,元大-員林!9835,元大-員林中山!0039003800380042,元大-桃園!9894,元大-桃興!0039003800390045,元大-草屯!003900380031006d,元大-高雄!003900380039007a,元大-國際證券!0039003800390059,元大-基隆!003900380036004b,元大-基隆孝二!0039003800310053,元大-崇德!9899,元大-淡水!0039003800300065,元大-清水!003900380031004c,元大-莒光!0039003800390053,元大-鹿港!0039003800320042,元大-博愛!9872,元大-復北!9833,元大-敦化!0039003800390050,元大-敦南!0039003800340048,元大-景美!0039003800310045,元大-發財!0039003800310079,元大-善化!0039003800310055,元大-華山!0039003800310058,元大-新生!9816,元大-新竹!9859,元大-新竹經國!0039003800340043,元大-新店中正!9869,元大-新盛!9898,元大-新莊!9825,元大-新營!0039003800370041,元大-楊梅!0039003800310071,元大-萬華!9887,元大-經紀部!0039003800310043,元大-路竹!0039003800390067,嘉義!0039003800310047,元大-彰化!003900380039004a,元大-彰化民生!003900380031006e,元大-旗山!0039003800310046,元大-福營!003900380033005a,元大-鳳山!0039003800350043,元大-鳳中!003900380031005a,元大-潮州!003900380033004b,元大-學甲!003900380039006a,元大-樹林!0039003800330055,元大-頭份!003900380034004b,元大-館前!9858,元大-龍潭!0039003800310061,元大-歸仁!9838,元大-豐原!0039003800390057,元大-豐原站前!9874,元大-雙和!0039003800320041,元大-羅東!9857,元大-蘆洲!0039003800390049,元大-蘆洲中正!9888,元大-鑫永和'

def parse_broker_tree(raw_str):
    tree = {}
    name_to_id = {}
    for group in raw_str.split(';'):
        if not group: continue
        parts = group.split('!')
        head = parts[0].split(',')
        if len(head) < 2: continue
        bid, bname = head[0], head[1]
        branches = {p.split(',')[1]: p.split(',')[0] for p in parts if len(p.split(',')) == 2}
        tree[bname] = {"bid": bid, "branches": branches}
        # 建立反向查詢 (名稱 -> ID)
        for br_name, br_id in branches.items():
            name_to_id[br_name] = {"a": bid, "b": br_id}
    return tree, name_to_id

DATA_TREE, BROKER_NAME_MAP = parse_broker_tree(RAW_DATA_STR)

# --- 輔助函式：取得股票代號 ---
def get_stock_id(name_str):
    match = re.search(r'\d{4,}', name_str)
    return match.group(0) if match else None

# ==========================================
# 1. 介面規劃
# ==========================================
tab1, tab2 = st.tabs(["🚀 券商分點查股票", "📊 股票代號查分點"])

# --- Tab 1: 券商查股票 ---
with tab1:
    st.markdown("### 🏦 追蹤特定分點的進出")
    c1, c2 = st.columns(2)
    with c1: sel_broker = st.selectbox("選擇券商", sorted(DATA_TREE.keys()), key="t1_b")
    with c2: 
        b_opts = DATA_TREE[sel_broker]['branches']
        sel_br_label = st.selectbox("選擇分點", list(b_opts.keys()), key="t1_br")
        sel_br_id = b_opts[sel_br_label]

    c3, c4, c5 = st.columns(3)
    with c3: t1_sd = st.date_input("開始日期", datetime.date.today()-datetime.timedelta(days=1), key="t1_sd")
    with c4: t1_ed = st.date_input("結束日期", datetime.date.today(), key="t1_ed")
    with c5: t1_unit = st.radio("單位", ["張數", "金額"], horizontal=True)

    c6, c7, c8 = st.columns([2, 1, 1])
    with c6: t1_mode = st.radio("模式", ["嚴格模式", "濾網模式"], horizontal=True)
    with c7: t1_th_pct = st.number_input("佔比門檻%", 0, 100, 95)
    with c8: t1_th_val = st.number_input(f"最低{t1_unit}門檻", 0, 1000000, 10)

    if st.button("查詢分點動向 🚀"):
        sd_s, ed_s = t1_sd.strftime('%Y-%m-%d'), t1_ed.strftime('%Y-%m-%d')
        bid = DATA_TREE[sel_broker]['bid']
        c_param = "B" if t1_unit == "金額" else "E"
        col_v = f"買進{t1_unit}"
        
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={bid}&b={sel_br_id}&c={c_param}&e={sd_s}&f={ed_s}"
        
        try:
            res = requests.get(url, verify=False, timeout=10)
            res.encoding = 'big5'
            html = re.sub(r"<script[^>]*>.*?GenLink2stk\s*\(\s*['\"](?:AS)?([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\).*?</script>", r"\1\2", res.text, flags=re.I|re.S)
            tables = pd.read_html(StringIO(html))
            
            df_res = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] >= 3 and any(x in str(tb) for x in ['買進','賣出']):
                    if tb.shape[1] >= 8:
                        l = tb.iloc[:,[0,1,2]].copy(); l.columns=['股票','買','賣']
                        r = tb.iloc[:,[5,6,7]].copy(); r.columns=['股票','買','賣']
                        df_res = pd.concat([df_res, l, r])
                    else:
                        t = tb.iloc[:,[0,1,2]].copy(); t.columns=['股票','買','賣']
                        df_res = pd.concat([df_res, t])

            if not df_res.empty:
                df_res['股票'] = df_res['股票'].astype(str).str.strip()
                df_res = df_res[~df_res['股票'].str.contains('名稱|買進|賣出|合計|註|說明', na=False)]
                for c in ['買','賣']: df_res[c] = pd.to_numeric(df_res[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
                df_res['總'] = df_res['買'] + df_res['賣']
                df_res = df_res[df_res['總'] > 0].copy()
                df_all = df_res.rename(columns={'買':f'買進{t1_unit}','賣':f'賣出{t1_unit}','總':f'總{t1_unit}'})
                df_all['買進佔比%'] = (df_all[f'買進{t1_unit}']/df_all[f'總{t1_unit}']*100).round(1)
                
                # 篩選
                if t1_mode == "嚴格模式":
                    final = df_all[(df_all[f'買進{t1_unit}'] >= t1_th_val) & (df_all[f'賣出{t1_unit}'] == 0)]
                else:
                    final = df_all[(df_all['買進佔比%'] >= t1_th_pct) & (df_all[f'買進{t1_unit}'] >= t1_th_val)]
                
                final = final.sort_values(f'買進{t1_unit}', ascending=False)
                
                # 增加網址列
                final['查看線圖連結'] = final['股票'].apply(lambda x: f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{get_stock_id(x)}.djhtm" if get_stock_id(x) else "")
                
                st.subheader(f"📊 篩選結果 - 共 {len(final)} 檔")
                st.dataframe(final, hide_index=True, column_config={"查看線圖連結": st.column_config.LinkColumn("點我看圖")}, use_container_width=True)
                
                # 崁入區塊
                if len(final) > 0:
                    pick = st.selectbox("選擇要預覽線圖的股票", final['股票'].tolist())
                    sid = get_stock_id(pick)
                    if sid:
                        chart_url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{sid}.djhtm"
                        st.write(f"正在顯示 {pick} 的分析頁面：")
                        components.iframe(chart_url, height=600, scrolling=True)
            else: st.info("查無資料")
        except Exception as e: st.error(f"錯誤: {e}")

# --- Tab 2: 股票查分點 ---
with tab2:
    st.markdown("### 🔍 看看這檔股票是誰在買？")
    c1, c2, c3 = st.columns(3)
    with c1: t2_sid = st.text_input("股票代號", "3702")
    with c2: t2_sd = st.date_input("開始", datetime.date.today()-datetime.timedelta(days=7), key="t2_sd")
    with c3: t2_ed = st.date_input("結束", datetime.date.today(), key="t2_ed")

    c4, c5, c6 = st.columns([2, 1, 1])
    with c4: t2_mode = st.radio("模式", ["嚴格(只買不賣)", "濾網(自訂佔比)"], horizontal=True, key="t2_m")
    with c5: t2_th_pct = st.number_input("買進佔比門檻%", 0, 100, 95, key="t2_th_p")
    with c6: t2_th_val = st.number_input("最低買進張數", 0, 1000000, 50, key="t2_th_v")

    if st.button("開始籌碼追蹤 🚀"):
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={t2_sid}&e={t2_sd.strftime('%Y-%m-%d')}&f={t2_ed.strftime('%Y-%m-%d')}"
        try:
            res = requests.get(url, verify=False, timeout=10)
            res.encoding = 'big5'
            tables = pd.read_html(StringIO(res.text))
            
            df_res = pd.DataFrame()
            for tb in tables:
                if tb.shape[1] == 10:
                    l = tb.iloc[:,[0,1,2]].copy(); l.columns=['券商','買','賣']
                    r = tb.iloc[:,[5,6,7]].copy(); r.columns=['券商','買','賣']
                    df_res = pd.concat([l, r])
            
            if not df_res.empty:
                df_res = df_res.dropna()
                df_res = df_res[~df_res['券商'].str.contains('券商|合計|平均|說明|註', na=False)]
                for c in ['買','賣']: df_res[c] = pd.to_numeric(df_res[c], errors='coerce').fillna(0)
                
                df_res['總'] = df_res['買'] + df_res['賣']
                df_res = df_res[df_res['總'] > 0].copy()
                df_res['買%'] = (df_res['買']/df_res['總']*100).round(1)

                if "嚴格" in t2_mode:
                    final = df_res[(df_res['買'] >= t2_th_val) & (df_res['賣'] == 0)]
                else:
                    final = df_res[(df_res['買%'] >= t2_th_pct) & (df_res['買'] >= t2_th_val)]
                
                final = final.sort_values('買', ascending=False).rename(columns={'買':'買進張數','賣':'賣出張數','總':'成交總量'})
                
                # 券商連結邏輯
                def get_broker_url(name):
                    info = BROKER_NAME_MAP.get(name)
                    if info: return f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={info['a']}&b={info['b']}&c=E"
                    return ""
                
                final['分點明細連結'] = final['券商'].apply(get_broker_url)
                
                st.subheader(f"🔴 波段吃貨主力 - 共 {len(final)} 家")
                st.dataframe(final, hide_index=True, column_config={"分點明細連結": st.column_config.LinkColumn("前往查看分點")}, use_container_width=True)
                
                st.divider()
                st.write(f"💡 本標的 ({t2_sid}) 走勢參考：")
                components.iframe(f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcw/zcw1_{t2_sid}.djhtm", height=600)
            else: st.warning("未找到資料")
        except Exception as e: st.error(f"錯誤: {e}")
