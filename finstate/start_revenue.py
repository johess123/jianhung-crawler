import subprocess
from dbConfig import conn, cur
import datetime
import time

#def main1(year,month):
def main():
    sql = "select * from stock_app_allstock;"
    cur.execute(sql,())
    record = cur.fetchall()
    date = datetime.datetime.now().strftime("%Y-%m-%d").split("-")
    year = str(int(date[0])-1911)
    month = int(date[1])-2
    if month <= 0:
        month += 12
        year = str(int(year)-1)
    month = str(month)
    
    #year = "112"
    #month = "9"
    # 還原成初始狀態
    sql = "update Sm_log set sm=0;"
    cur.execute(sql,())
    conn.commit()
    #record = [["9110"]]
    for i in range(len(record)):
        stockid = record[i][0]
        print("股票代號: "+stockid+" 開始")
        subprocess.run(["python3","original_revenue.py",stockid,year,month,"0"])
        print("股票代號: "+stockid+" 結束")
    
    # 重複檢查
    while True:
        sql = "select * from Sm_log;"
        cur.execute(sql,())
        record = cur.fetchall()
        do = True # 都請求成功
        for i in range(len(record)):
            stockid = record[i][0]
            if record[i][1] == 0: # 請求營收表失敗
                subprocess.run(["python3","original_revenue.py",stockid,year,month,"0"])
                do = False # 有請求失敗
        if do == True: # 請求都成功
            break

'''
def main():
    sql = "select * from stock_app_allstock;"
    cur.execute(sql,())
    record = cur.fetchall()
    for i in range(len(record)):
        sql = "insert into Sm_log (stockid,sm) values (%s,0);"
        cur.execute(sql,(record[i][0],))
    conn.commit()
    #main1("112","12")
    #for i in range(1,13):
        #main1("112",str(i))
'''

main()
