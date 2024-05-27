import subprocess
from dbConfig import conn, cur
import datetime
import time

# 開始爬財報
#def main1(year,season):
def main():
    sql = "select * from stock_app_allstock;"
    cur.execute(sql,())
    record = cur.fetchall()
    date = datetime.datetime.now().strftime("%Y-%m-%d").split("-")
    year = str(int(date[0])-1911)
    month = int(date[1])-3 # 預估晚3個月爬報表
    if month <= 0:
        month += 12
        year -= str(int(year)-1)
    season = str((month-1)//3+1)
    #year = "112"
    #season = "4"
    #month = "12"
    
    # 還原成初始狀態
    sql = "update original_log set assest=0, income=0;"
    cur.execute(sql,())
    conn.commit()
    #record = [["1231"]]
    for i in range(len(record)):
        stockid = record[i][0]
        #stockid = record[i]
        print("股票代號: "+stockid+" 開始")
        subprocess.run(["python3","original_finstate.py",stockid,year,season,"0,0,0"])
        print("股票代號: "+stockid+" 結束")
        break

    
    # 重複檢查
    while True:
        sql = 'select * from original_log;'
        #sql = 'select * from original_log where stockid="2330";'
        cur.execute(sql,())
        record = cur.fetchall()
        do = True # 都請求成功
        for i in range(len(record)):
            stockid = record[i][0]
            if record[i][1] == 0: # 請求資產負債表失敗
                subprocess.run(["python3","original_finstate.py",stockid,year,season,"0,1,1"])
                do = False # 有請求失敗
            if record[i][2] == 0: # 請求綜合損益表失敗
                subprocess.run(["python3","original_finstate.py",stockid,year,season,"1,0,1"])
                do = False # 有請求失敗
            if record[i][3] == 0: # 請求現金流量表失敗
                subprocess.run(["python3","original_finstate.py",stockid,year,season,"1,1,0"])
                do = False
        if do == True: # 請求都成功
            break
    
    # 計算 EPS
    #record = [["1231"]]
    for i in range(len(record)):
        stockid = record[i][0]
        # K, K1, K2, K3, K4, N1, N2, N3, N4, N, N_now
        sql = 'select * from stock_app_k where stockid=%s and year=%s order by season;'
        cur.execute(sql,(stockid,year))
        K_this_year = cur.fetchall()
        data = {}
        output = {}
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
        # EPS
        sql = "select * from ks_original where stockid=%s and year=%s and season=%s;"
        cur.execute(sql,(stockid,year,season))
        data["EAIT"] = cur.fetchall()[0][9]
        output["EPS"] = data["EAIT"] / output["N"]
        sql = 'insert into stock_app_eps (stockid,year,season,eps) values (%s,%s,%s,%s);'
        cur.execute(sql,(stockid,year,season,output["EPS"]))
        conn.commit()
    

    # 計算公式 (後處理)
    for i in range(len(record)):
        stockid = record[i][0]
    #stockid = "1231"
        subprocess.run(["python3","processed_finstate.py",stockid,year,season,month])

'''
def main():
    for i in range(1,5):
        main1("110",str(i))
'''

main()
