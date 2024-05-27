from dbConfig import conn,cur
import requests
from bs4 import BeautifulSoup
import time
import datetime
import sys
import logging

def financial_statement(stockid,year,season,month):
    # Ks
    data = {} # original data
    output = {} # porcessed data
    
    # 撈今年 ks original data
    sql = 'select * from ks_original where stockid=%s and year=%s and season=%s;'
    cur.execute(sql,(stockid,year,season))
    ks = cur.fetchall()[0]
    data["TA"] = ks[4]
    data["TL"] = ks[5]
    data["LL"] = ks[6]
    data["I"] = ks[7]
    data["T"] = ks[8]
    data["EAIT"] = ks[9]
    data["EBIT"] = ks[10]
    data["AD"] = ks[11]
    data["Div"] = ks[12]
    data["AR"] = ks[13]
    data["Inv"] = ks[14]
    data["Prep"] = ks[15]
    data["CA"] = ks[16]
    data["FA"] = ks[17]
    data["AP"] = ks[18]
    data["CL"] = ks[19]
    data["RE"] = ks[20]
    data["GP"] = ks[21]
    data["C"] = ks[22]
    
    # 撈去年末 ks original data
    sql = 'select * from ks_original where stockid=%s and year=%s and season=%s;'
    cur.execute(sql,(stockid,year-1,4))
    ks = cur.fetchall()[0]
    data["TA-1"] = ks[4]
    data["TL-1"] = ks[5]
    output["TE-1"] = data["TA-1"]-data["TL-1"]
    data["I-1"] = ks[7]
    data["T-1"] = ks[8]
    data["EAIT-1"] = ks[9]
    data["EBIT-1"] = ks[10]
    data["AD-1"] = ks[11]
    data["AR-1"] = ks[13]
    data["Inv-1"] = ks[14]
    data["AP-1"] = ks[18]
    data["RE-1"] = ks[20]
    data["GP-1"] = ks[21]
    
    # K-1, N-1
    sql = 'select * from stock_app_k where stockid=%s and year=%s and season=%s;'
    cur.execute(sql,(stockid,year-1,4))
    data["K-1"] = cur.fetchall()[0][4]
    output["N-1"] = data["K-1"]/10
    
    # K, K1, K2, K3, K4, N1, N2, N3, N4, N, N_now
    sql = 'select * from stock_app_k where stockid=%s and year=%s order by season;'
    cur.execute(sql,(stockid,year))
    K_this_year = cur.fetchall()
    data["K"] = K_this_year[-1][4] # 最新一期股本
    output["N"] = 0 # 今年平均股數
    output["N_now"] = 0 # 當期股數
    for i in range(len(K_this_year)):
        data["K"+str(i+1)] = K_this_year[i][4]
        output["N"+str(i+1)] = K_this_year[i][4]/10
        output["N"] += output["N"+str(i+1)]
        if i == len(K_this_year)-1:
            output["N_now"] = K_this_year[i][4]/10
    output["N"] = output["N"]/len(K_this_year) # 今年平均股數(有幾期就除以幾)

    # EPS 改到 start_finstate 執行
    #output["EPS"] = data["EAIT"] / output["N"]
    #sql = 'insert into stock_app_eps (stockid,year,season,eps) values (%s,%s,%s,%s);'
    #cur.execute(sql,(stockid,year,season,output["EPS"]))
    #conn.commit()

    # sm 今年
    sql = 'select * from stock_app_sm where stockid=%s and year=%s order by month;'
    cur.execute(sql,(stockid,year))
    Sm_now = cur.fetchall()
    
    # S1, S2, S3, S4, S12
    output["S1"] = 0
    output["S2"] = 0
    output["S3"] = 0
    output["S4"] = 0
    output["S12"] = 0
    for i in range(len(Sm_now)):
        data["Sm"+str(i+1)] = Sm_now[i][4]
        output["S"+str(i//3+1)] += Sm_now[i][4]
        output["S12"] += Sm_now[i][4]
    
    # sm 去年
    sql = 'select * from stock_app_sm where stockid=%s and year=%s order by month;'
    cur.execute(sql,(stockid,year-1))
    Sm_now = cur.fetchall()
    
    # S-1,1 / S-1,2 / S-1,3 / S-1,4 / S-1,12
    output["S-1,1"] = 0
    output["S-1,2"] = 0
    output["S-1,3"] = 0
    output["S-1,4"] = 0
    output["S-1,12"] = 0
    for i in range(len(Sm_now)):
        data["S-1,m"+str(i+1)] = Sm_now[i][4]
        output["S-1,"+str(i//3+1)] += Sm_now[i][4]
        output["S-1,12"] += Sm_now[i][4]
    
    # S~12 最近12個月
    # 現在月份
    output["S~12"] = output["S12"]
    for i in range(12,month,-1):
        output["S~12"] += data["S-1,m"+str(i)]

    # TE
    output["TE"] = data["TA"]-data["TL"]
    # NW
    output["NW"] = output["TE"]/output["N"] # NW 每股淨值
    
    # DR1, DR2, AVG
    output["DR1"] = output["TE"]/data["K"] # DR1 負債比例1
    output["DR2"] = data["LL"]/data["K"] # DR2 負債比例2
    output["AVG"] = output["DR1"]*0.4+output["DR2"]*0.6 # AVG
    # ROA, ROE, DR
    output["ROA"] = data["EBIT"]/((data["TA"]+data["TA-1"])/2)#*100
    output["ROE"] = data["EAIT"]/((output["TE"]+output["TE-1"])/2)#*100
    
    # DR...
    # 需要 FLI(在下面)

    # P 股價要拿甚麼時候 (先拿計算當天)
    #date = datetime.datetime.now().strftime("%Y-%m-%d")
    date = "2024-04-01"
    sql = 'select * from stock_app_allprice where stockid=%s and date=%s;'
    cur.execute(sql,(stockid,date))
    data["P"] = float(cur.fetchall()[0][4])
    
    # TE(MV)
    output["TE(MV)"] = data["P"]*output["N"] # 要用今年平均股數
    # TA(MV)
    output["TA(MV)"] = data["TL"]+output["TE(MV)"]
    # Ro
    output["Ro"] = data["EBIT"]/output["TA(MV)"]
    
    # rf, cdi, inf, opi 
    rf = 0.01159
    cdi = 0.01575
    inf = 0.0272
    output["opi"] = cdi+inf 
    # Ks
    # 需要 DR, t(在下面)

    # FRV
    # 需要 Ks, EPS(f) (在下面)

    # R
    # 需要 FRV(在下面)

    
    # EPS
    # 從DB撈
    sql = 'select * from stock_app_eps where stockid=%s and year=%s;'
    cur.execute(sql,(stockid,year))
    EPS_now = cur.fetchall()
    # 今年累積 EPS
    output["EPS"] = 0
    for i in range(len(EPS_now)):
        data["EPS"+str(i+1)] = float(EPS_now[i][4])
        output["EPS"] += data["EPS"+str(i+1)]
    sql = 'select * from stock_app_eps where stockid=%s and year=%s;'
    cur.execute(sql,(stockid,year-1))
    EPS_last = cur.fetchall()
    # 去年 EPS
    output["EPS-1"] = 0
    for i in range(len(EPS_last)):
        data["EPS-1,"+str(i+1)] = float(EPS_last[i][4])
        output["EPS-1"] += data["EPS-1,"+str(i+1)]
    # 最近四季EPS
    output["EPS~4"] = output["EPS"]
    for i in range(4,season,-1):
        output["EPS~4"] += float(EPS_last[i-1][4])
        # 要再確認

    # EAIT-1, EBIT-1
    output["EAIT-1"] = output["EPS-1"]*output["N"]
    output["EBIT-1"] = output["EAIT-1"]+data["I-1"]+data["T-1"]
    # ROA-1, ROE-1
    output["ROA-1"] = output["EBIT-1"]/data["TA"]
    output["ROE-1"] = output["EAIT-1"]/output["TE"]
    # t
    output["t"] = data["T-1"]/output["EBIT-1"]
    ###
    if output["t"] >= 0.17:
        output["t"] = 0.17
    elif output["t"] < 0.045:
        output["t"] = 0.045
    ###
    # FLI
    output["FLI"] = output["ROE-1"]/output["ROA-1"]
    
    # DR
    output["DR"] = 0 # DR
    if output["ROA"] >= 0.075:
        output["DR"] = output["AVG"]/3
    elif (output["ROA"] < 0.075) and (output["ROA"] >= 0.05) and (output["ROE"] > output["ROA"]):
        output["DR"] = output["AVG"]*output["FLI"]
    elif output["ROA"] < 0.05:
        output["DR"] = output["AVG"]
    
    # Ks
    output["Ks"] = rf+(output["Ro"]-rf)*(1-output["t"])+(output["Ro"]-output["opi"])*output["DR"]*(1-output["t"])
    
    # EPS(f1)
    output["EPS(f1)"] = output["EPS~4"]*0.65+output["EPS"]*0.35
    # EAIT(f1)
    output["EAIT(f1)"] = output["EPS(f1)"]*output["N"]
    
    # NPR(f1) 需要 S12,S~12, m
    # EAIT(f1)/(S12*m/12+S~12*(12-m)/12)
    output["NPR(f1)"] = output["EAIT(f1)"]/(output["S12"]*month/12+output["S~12"]*(12-month)/12)
    
    # EAIT-1 又出現了

    # NPR-1 需要 S-1
    # S-1 先假設是去年營收總和
    output["S-1"] = output["S-1,12"]
    # EAIT-1/S-1
    output["NPR-1"] = output["EAIT-1"]/output["S-1"]
    
    # NPR(f)
    output["NPR(f)"] = 0
    check = output["NPR-1"]/output["NPR(f1)"]
    if check >= 0.8 and check <= 1.2:
        output["NPR(f)"] = output["NPR-1"]*0.5+output["NPR(f1)"]*0.5
    else:
        output["NPR(f)"] = output["NPR-1"]*0.35+output["NPR(f1)"]*0.65
    
    # EPS(f2) 需要 S12
    # S12*NPR(f)/N
    output["EPS(f2)"] = output["S12"]*output["NPR(f)"]/output["N"]

    # EPS(f3) 需要 S~12
    # S~12*NPR(f)/N
    output["EPS(f3)"] = output["S~12"]*output["NPR(f)"]/output["N"]
    
    # EPS(ff) 需要 m
    # EPS(f2)*m/12+EPS(f3)*(12-m)/12
    output["EPS(ff)"] = output["EPS(f2)"]*month/12+output["EPS(f3)"]*(12-month)/12

    # g 用最近12月比去年
    data["g"] = (output["S~12"]-output["S-1"])/output["S-1"]

    # EPS(g)
    # S-1*(1+g)*NPR(f)/N
    output["EPS(g)"] = output["S-1"]*(1+data["g"])*output["NPR(f)"]/output["N"]

    # EPS(np) 人工輸入
    data["EPS(np)"] = 0

    # EPS(f)
    output["EPS(f)"] = output["EPS-1"]*0.1+output["EPS(ff)"]*0.35+output["EPS(g)"]*0.55+0.2*data["EPS(np)"]

    # FRV
    output["FRV"] = 0
    check = (output["EPS(f)"]/output["Ks"])/output["NW"]
    if check <= 0.65:
        output["FRV"] = output["NW"]*0.65
    elif check <= 0.75:
        output["FRV"] = output["NW"]*0.75
    elif checek <= 0.85:
        output["FRV"] = output["NW"]*0.85
    elif check <= 1:
        output["FRV"] = output["NW"]*1
    elif check > 1:
        output["FRV"] = output["EPS(f)"]/output["Ks"]

    # R
    output["R"] = data["P"]/output["FRV"]

    # -----------------------------------------------
    # Risk
    # A|R
    output["A|R"] = data["AR"] - data["AR-1"]
    # In|v
    output["In|v"] = data["Inv"] - data["Inv-1"]
    # T|A
    output["T|A"] = data["TA"] - data["TA-1"]
    # A|P
    output["A|P"] = data["AP"] - data["AP-1"]
    # T|L
    output["T|L"] = data["TL"] - data["TL-1"]
    # R|E
    output["R|E"] = data["RE"] - data["RE-1"]
    # S|
    output["S|"] = output["S12"] - output["S-1"]
    # G|P
    output["G|P"] = data["GP"] - data["GP-1"]
    # EB|IT
    output["EB|IT"] = data["EBIT"]- data["EBIT-1"]
    # EA|IT
    output["EA|IT"] = data["EAIT"] - data["EAIT-1"]
    # EPS
    output["EPS_now"] = data["EAIT"] / output["N_now"] 
    # Depr
    output["Depr"] = data["AD"] - data["AD-1"]
    # 現金股利殖利率
    output["現金股利殖利率"] = data["Div"] / data["P"]
    # 發放率
    output["發放率"] = data["Div"] / output["EPS_now"]
    # P/E
    output["P/E"] = data["P"] / output["EPS_now"]
    # 股價報酬率
    output["股價報酬率"] = 1/output["P/E"]
    # 流動比率
    if data["CL"] == 0:
        output["流動比率"] = -1 # 設-1表示例外
    else:
        output["流動比率"] = data["CA"] / data["CL"]
        if output["流動比率"] < 2.0:
            output["流動比率"] = 2.0
    # 速動比率
    if data["CL"] == 0:
        output["速動比率"] = -1 # 設-1表示例外
    else:
        output["速動比率"] = (data["CA"]-data["Inv"]-data["Prep"]) / data["CL"]
        if output["速動比率"] < 1.5:
            output["速動比率"] = 1.5
    # 應收應付比率
    output["應收應付比率"] = data["AR"] / data["AP"]
    if output["應收應付比率"] < 1.0:
        output["應收應付比率"] = 1.0
    # 固定比率
    output["固定比率"] = data["FA"] / output["TE"]
    if output["固定比率"] > 0.8:
        output["固定比率"] = 0.8
    # 固定長期適合率
    output["固定長期適合率"] = data["FA"] / (data["LL"]+output["TE"])
    if output["固定長期適合率"] > 0.6:
        output["固定長期適合率"] = 0.6
    # 長期權益適合率
    output["長期權益適合率"] = (data["CA"]+data["FA"]-data["CL"]) / output["TE"]
    if output["長期權益適合率"] > 1.0:
        output["長期權益適合率"] = 1.0
    # 自有資本比
    output["自有資本比"] = output["TE"] / data["TA"]
    if output["自有資本比"] < 0.7:
        output["自有資本比"] = 0.7
    # 負債比率
    output["負債比率"] = data["LL"] / output["TE"]
    if output["負債比率"] > 0.25:
        output["負債比率"] = 0.25
    # 每股營收
    output["每股營收"] = output["S12"] / output["N_now"] # N
    # 資產週轉率
    output["資產週轉率"] = output["S12"] / ((data["TA-1"]+data["TA"])/2)
    # 應收週轉率
    output["應收週轉率"] = output["S12"] / ((data["AR-1"]+data["AR"])/2)
    # 存貨週轉率
    output["存貨週轉率"] = output["S12"] / ((data["Inv-1"]+data["Inv"])/2)
    # 營業週期
    output["營業週期"] = 365*(output["S12"]/((data["AR-1"]+data["Inv-1"]+data["AR"]+data["Inv"])/2))
    # 資金負擔率
    output["資金負擔率"] = (data["AR"]+data["Inv"]-data["AP"])/output["S12"]*100
    # 毛利率
    output["毛利率"] = (data["GP"]/output["S12"])*100
    # 營業利益率
    output["營業利益率"] = (data["EBIT"]/output["S12"])*100
    # 淨利率
    output["淨利率"] = (data["EAIT"]/output["S12"])*100
    # ROA ROE EPS上面算過了
    # 現金股利發放率
    output["現金股利發放率"] = (data["Div"]/output["EPS_now"])*100
    
    # 現金股利殖利率
    # 現金股利發放率
    # 股價報酬率
    # 有重複問題
    
    # 每股現金
    output["每股現金"] = data["C"] / output["N_now"]
    # 現金股本比
    output["現金股本比"] = data["C"] / data["K"]
    # 現金回收率
    output["現金回收率"] = (output["每股現金"] / data["P"])*100
    # 現金比率
    output["現金比率"] = ((data["EBIT"]+output["Depr"]+output["A|P"]-output["A|R"]-output["In|v"]) / data["EBIT"])*100
    # 內部資金貢獻率
    output["內部資金貢獻率"] = (output["R|E"]+output["Depr"]+output["A|P"]-output["A|R"]-output["In|v"]) / output["T|A"]
    if output["內部資金貢獻率"] < 1.00:
        output["內部資金貢獻率"] = 1.00
    # 營運資金穩定比
    output["營運資金穩定比"] = (data["CA"]-data["CL"]) / (output["TE"]-data["FA"])
    if output["營運資金穩定比"] > 1.0:
        output["營運資金穩定比"] = 1.0
    # 保留盈餘成長率
    output["保留盈餘成長率"] = (output["R|E"]/data["RE"])*100
    # 稅後淨利成長率
    output["稅後淨利成長率"] = (output["EA|IT"]/data["EAIT"])*100
    # 營業淨利成長率
    output["營業淨利成長率"] = (output["EB|IT"]/data["EBIT"])*100
    # 毛利成長率
    #data["GP"] = 1 # 暫時設
    output["毛利成長率"] = (output["G|P"]/data["GP"])*100
    # 營收成長率
    output["營收成長率"] = (output["S|"]/output["S12"])*100
    # 總資產成長率
    output["總資產成長率"] = (output["T|A"]/data["TA"])*100
    # 總負債成長率
    output["總負債成長率"] = (output["T|L"]/data["TL"])*100
    # 負債額度比
    output["負債額度比"] = data["LL"]/((data["EBIT"]+output["Depr"])*0.25/0.04)
    if output["負債額度比"] > 1.00:
        output["負債額度"] = 1.00
    # 利息保障倍數
    output["利息保障倍數"] = (data["EAIT"]+data["I"]+data["T"]) / data["I"]
    '''
    print(output)
    while True:
        a = input()
        try:
            print(output[a])
        except Exception as e:
            print(e)
    '''
    # 存DB
    sql = 'insert into stock_app_processed_data (stockid,year,season,N,TE,NW,DR,DR1,DR2,AVG,TA_MV,TE_MV,Ro,opi,Ks,FRV,R,EPS,EPS_4,EPS_1,EAIT_1,EBIT_1,ROA_1,ROE_1,t,FLI,EPS_f1,EAIT_f1,NPR_f1,NPR_1,NPR_f,EPS_f2,EPS_f3,EPS_ff,EPS_g,EPS_f,S12,S_12,A_R,In_v,T_A,A_P,T_L,R_E,_S,G_P,EA_IT,EB_IT,Depr,DY,PR,P_E,SRR,CAR,QR,RPR,FR,LTFASR,LTESR,SOCR,LER,EPR,ATR,RTR,ITR,OC,FBR,GMR,OPMR,NPMR,CPS,CKR,CRR,CR,IFCR,WCSR,REGR,EAITGR,EBITGR,GPR,SR,TAR,TLR,DBR,ICR) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    cur.execute(sql,(stockid,year,season,output["N"],output["TE"],output["NW"],output["DR"],output["DR1"],output["DR2"],output["AVG"],output["TA(MV)"],output["TE(MV)"],output["Ro"],output["opi"],output["Ks"],output["FRV"],output["R"],output["EPS"],output["EPS~4"],output["EPS-1"],output["EAIT-1"],output["EBIT-1"],output["ROA-1"],output["ROE-1"],output["t"],output["FLI"],output["EPS(f1)"],output["EAIT(f1)"],output["NPR(f1)"],output["NPR-1"],output["NPR(f)"],output["EPS(f2)"],output["EPS(f3)"],output["EPS(ff)"],output["EPS(g)"],output["EPS(f)"],output["S12"],output["S~12"],output["A|R"],output["In|v"],output["T|A"],output["A|P"],output["T|L"],output["R|E"],output["S|"],output["G|P"],output["EA|IT"],output["EB|IT"],output["Depr"],output["現金股利殖利率"],output["發放率"],output["P/E"],output["股價報酬率"],output["流動比率"],output["速動比率"],output["應收應付比率"],output["固定比率"],output["固定長期適合率"],output["長期權益適合率"],output["自有資本比"],output["負債比率"],output["每股營收"],output["資產週轉率"],output["應收週轉率"],output["存貨週轉率"],output["營業週期"],output["資金負擔率"],output["毛利率"],output["營業利益率"],output["淨利率"],output["每股現金"],output["現金股本比"],output["現金回收率"],output["現金比率"],output["內部資金貢獻率"],output["營運資金穩定比"],output["保留盈餘成長率"],output["稅後淨利成長率"],output["營業淨利成長率"],output["毛利成長率"],output["營收成長率"],output["總資產成長率"],output["總負債成長率"],output["負債額度比"],output["利息保障倍數"]))
    conn.commit()


def main():
    stockid = sys.argv[1]
    year = int(sys.argv[2])
    season = int(sys.argv[3])
    month = int(sys.argv[4])
    #stockid = "2330"
    #year = 112
    #season = 2
    financial_statement(stockid,year,season,month)

main()

