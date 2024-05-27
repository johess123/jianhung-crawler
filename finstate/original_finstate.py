from dbConfig import conn,cur
import requests
from bs4 import BeautifulSoup
import time
import datetime
import sys
import logging

def send_request(url,year,season,stockid): # 發送請求
    logging.basicConfig(filename='/home/stock/stock_project/crawler/finstate/original_error.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') # 記錄log
    try:
        r = requests.post(url, {
            'encodeURIComponent':1,
            'step':1,
            'firstin':1,
            'off':1,
            'TYPEK':'all',
            'year':str(year),
            'season':"0"+str(season),
            'co_id':str(stockid),
            'queryName':str(stockid),
            'inputType':str(stockid),
            'isnew':False,
        })
        soup = BeautifulSoup(r.text,'html.parser')
        result = soup.findAll(True, {'class':['odd', 'even']})
        if url == "https://mops.twse.com.tw/mops/web/ajax_t164sb05":
            period = len(soup.findAll(True,{'class':['tblHead']}))
            period = (period-4)/2
            div = period+1
        else:
            period = len(soup.findAll(True,{'class':['tblHead']})) # 計算期數(有時候報表是2期或3期)
            period = (period-4)/3 # 扣掉基本標頭後/3=期數
            div = 2*period+1 # 每項都是標頭+(該期數字+百分比)*期數 => 只要取最新一期 (餘1是數值)
        data = {}
        for j in range(len(result)):
            try:
                if j%div == 0:
                    subject = result[j].getText().strip()
                    num = float(result[j+1].getText().strip().replace(",",""))
                    data[subject] = num
            except: # 過濾掉沒數字的欄位(分類的標題)
                pass
        time.sleep(5)
        # 處理例外狀況
        if url=="https://mops.twse.com.tw/mops/web/ajax_t164sb03" and ('股本合計' not in data and '股本' not in data):
            r = requests.post(url, {
                'encodeURIComponent':1,
                'step':2,
                'firstin':1,
                'TYPEK':'sii',
                'year':str(year),
                'season':"0"+str(season),
                'co_id':str(stockid),
            })
            soup = BeautifulSoup(r.text,'html.parser')
            result = soup.findAll(True, {'class':['odd', 'even']})
            period = len(soup.findAll(True,{'class':['tblHead']})) # 計算期數(有時>候報表是2期或3期)
            period = (period-4)/3 # 扣掉基本標頭後/3=期數
            div = 2*period+1 # 每項都是標頭+(該期數字+百分比)*期數 => 只要取最新一>期 (餘1和餘2)
            data = {}
            for j in range(len(result)):
                try:
                    if j%div == 0:
                        subject = result[j].getText().strip()
                        num = float(result[j+1].getText().strip().replace(",",""))
                        data[subject] = num
                except: # 過濾掉沒數字的欄位(分類的標題)
                    pass
            time.sleep(5)
        if url=="https://mops.twse.com.tw/mops/web/ajax_t164sb04" and '基本每股盈餘' not in data:
            r = requests.post(url, {
                'encodeURIComponent':1,
                'step':2,
                'firstin':1,
                'TYPEK':'sii',
                'year':str(year),
                'season':"0"+str(season),
                'co_id':str(stockid),
            })
            soup = BeautifulSoup(r.text,'html.parser')
            result = soup.findAll(True, {'class':['odd', 'even']})
            period = len(soup.findAll(True,{'class':['tblHead']})) # 計算期數(有時>候報表是2期或3期)
            if (period-5)%3 == 0:
                period = (period-5)/3 # 扣掉基本標頭後/3=期數 # 多一列變動百分比
                div = 2*period+2 # 每項都是標頭+(該期數字+百分比)*期數+變動百分比 => 只要取最新一期 (餘1和餘2)
            else: # 有些例外
                period = (period-4)/3
                div = 2*period+1
            data = {}
            for j in range(len(result)):
                try:
                    if j%div == 0:
                        subject = result[j].getText().strip()
                        num = float(result[j+1].getText().strip().replace(",",""))
                        data[subject] = num
                except: # 過濾掉沒數字的欄位(分類的標題)
                    pass
            time.sleep(5)
        if url == "https://mops.twse.com.tw/mops/web/ajax_t164sb05" and '折舊費用' not in data:
            r = requests.post(url, {
                'encodeURIComponent':1,
                'step':2,
                'firstin':1,
                'TYPEK':'sii',
                'year':str(year),
                'season':"0"+str(season),
                'co_id':str(stockid),
            })
            soup = BeautifulSoup(r.text,'html.parser')
            result = soup.findAll(True, {'class':['odd', 'even']})
            period = len(soup.findAll(True,{'class':['tblHead']})) # 計算期數(有時>候報表是2期或3期)
            if (period-3)%3 == 0:
                period = (period-3)/3 # 扣掉基本標頭後/3=期數 # 多一列變動百分比
                div = 2*period+2 # 每項都是標頭+(該期數字+百分比)*期數+變動百分比 => 只要取最新一期 (餘1和餘2)
            else: # 有些例外
                period = (period-2)/3
                div = 2*period+1
            data = {}
            for j in range(len(result)):
                try:
                    if j%div == 0:
                        subject = result[j].getText().strip()
                        num = float(result[j+1].getText().strip().replace(",",""))
                        data[subject] = num
                except: # 過濾掉沒數字的欄位(分類的標題)
                    pass
            time.sleep(5)
        return data
    except Exception as e:
        print("掛了")
        logging.error("error: %s", str(stockid)+" "+str(e))
        

def financial_statement(stockid,year,season,log): # 處理original data, 存db
    url1 = "https://mops.twse.com.tw/mops/web/ajax_t164sb03" # 資產負債表
    url2 = "https://mops.twse.com.tw/mops/web/ajax_t164sb04" # 綜合損益表
    url3 = "https://mops.twse.com.tw/mops/web/ajax_t164sb05" # 現金流量表
    url4 = "https://mops.twse.com.tw/mops/web/ajax_t164sb06" # 權益變動表
    output = {}
    if log[0] == 0: # 資產負債表請求尚未成功
        assest_now = send_request(url1,year,season,stockid) # 現在資產負債表
        if assest_now == None:
            # 請求失敗
            print(stockid,"請求資產負債表失敗")
        else:
            # 請求成功
            sql = "update original_log set assest=1 where stockid=%s;"
            cur.execute(sql,(stockid,))
            conn.commit()
            print(assest_now)
            # 股本
            if '股本合計' in assest_now:
                output["K"] = assest_now["股本合計"]
            elif '股本' in assest_now:
                output["K"] = assest_now["股本"]
            else:
                output["K"] = 0
            # TA 總資產
            if '資產總額' in assest_now:
                output["TA"] = assest_now["資產總額"]
            elif '資產總計' in assest_now:
                output["TA"] = assest_now["資產總計"]
            else:
                output["TA"] = 0
            # TL 總負債
            if '負債總額' in assest_now:
                output["TL"] = assest_now["負債總額"]
            elif '負債總計' in assest_now:
                output["TL"] = assest_now["負債總計"]
            else:
                output["TL"] = 0
            # LL 長期負債
            if '長期借款' in assest_now:
                output["長期借款"] = assest_now["長期借款"]
            else:
                output["長期借款"] = 0

            if '租賃負債－非流動' in assest_now:
                output["租賃負債－非流動"] = assest_now["租賃負債－非流動"]
            else:
                output["租賃負債－非流動"] = 0
            if '應付公司債' in assest_now:
                output["應付公司債"] = assest_now["應付公司債"]
            else:
                output["應付公司債"] = 0
            output["LL"] = output["長期借款"]+output["租賃負債－非流動"]+output["應付公司債"]
            # C 現金
            if '現金及約當現金' in assest_now:
                output["C"] = assest_now["現金及約當現金"]
            else:
                output["C"] = 0
            # AR 應收帳款
            if '應收帳款淨額' in assest_now:
                output["AR"] = assest_now["應收帳款淨額"]
            else:
                output["AR"] = 0
            # Inv 庫存
            if '存貨' in assest_now:
                output["Inv"] = assest_now["存貨"]
            else:
                output["Inv"] = 0
            # Prep 預付款
            # 不知道
            output["Prep"] = 0
            # CA 流動資產
            if '流動資產合計' in assest_now:
                output["CA"] = assest_now["流動資產合計"]
            else:
                output["CA"] = 0
            # FA 固定資產
            if '非流動資產合計' in assest_now:
                output["FA"] = assest_now["非流動資產合計"]
            else:   
                output["FA"] = 0
            # AP 應付帳款
            if '應付帳款' in assest_now:
                output["AP"] = assest_now["應付帳款"]
            else:
                output["AP"] = 0
            # CL 短期負債
            # 不知道
            if '短期借款' in assest_now:
                output["CL"] = assest_now["短期借款"]
            else:
                output["CL"] = 0
            # RE 保留盈餘
            if '保留盈餘合計' in assest_now:
                output["RE"] = assest_now["保留盈餘合計"]
            else:
                output["RE"] = 0

            # 存db
            # 確認是否已有新增過
            sql = 'select * from ks_original where stockid=%s and year=%s and season=%s;'
            cur.execute(sql,(stockid,year,season))
            record = cur.fetchall()
            if len(record) == 0: # 還沒新增過
                sql = 'insert into ks_original (stockid,year,season,TA,TL,LL,C,AR,Inv,Prep,CA,FA,AP,CL,RE) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                cur.execute(sql,(stockid,year,season,output["TA"],output["TL"],output["LL"],output["C"],output["AR"],output["Inv"],output["Prep"],output["CA"],output["FA"],output["AP"],output["CL"],output["RE"]))
                conn.commit()
            else: # 已經新增過
                sql = 'update ks_original set TA=%s, TL=%s, LL=%s, C=%s, AR=%s, Inv=%s, Prep=%s, CA=%s, FA=%s, AP=%s, CL=%s, RE=%s where stockid=%s and year=%s and season=%s;'
                cur.execute(sql,(output["TA"],output["TL"],output["LL"],output["C"],output["AR"],output["Inv"],output["Prep"],output["CA"],output["FA"],output["AP"],output["CL"],output["RE"],stockid,year,season))
                conn.commit()
            # 存股本(K)
            sql = 'insert into stock_app_k (stockid,year,season,k) values (%s,%s,%s,%s);'
            cur.execute(sql,(stockid,year,season,output["K"]))
            conn.commit()

    if log[1] == 0:
        income_now = send_request(url2,year,season,stockid) # 現在綜合損益表
        if income_now == None:
            # 請求失敗
            print(stockid,"請求綜合損益表失敗")
        else:
            # 請求成功
            sql = "update original_log set income=1 where stockid=%s;"
            cur.execute(sql,(stockid,))
            conn.commit()
            # 去年利息
            if '財務成本淨額' in income_now:
                output["I"] = income_now["財務成本淨額"]
            elif '利息費用' in income_now:
                output["I"] = income_now["利息費用"] # 例外
            else:
                output["I"] = 0
            # 去年稅
            if '所得稅費用（利益）合計' in income_now:
                output["T"] = income_now["所得稅費用（利益）合計"]
            elif '所得稅費用（利益）' in income_now:
                output["T"] = income_now["所得稅費用（利益）"] # 例外
            else:
                output["T"] = 0
            # EBIT 稅前淨利
            if '稅前淨利（淨損）' in income_now:
                output["EBIT"] = income_now["稅前淨利（淨損）"]
            else:
                output["EBIT"] = 0
            # EAIT 稅後淨利
            if '本期淨利（淨損）' in income_now:
                output["EAIT"] = income_now["本期淨利（淨損）"]
            else:
                output["EAIT"] = 0
            # S營收 (營收程式負責記錄)
            # GP 毛利
            if '營業毛利（毛損）淨額' in income_now:
                output["GP"] = income_now["營業毛利（毛損）淨額"]
            else:
                output["GP"] = 0
            # 存db
            # 確認是否已有新增過
            sql = 'select * from ks_original where stockid=%s and year=%s and season=%s;'
            cur.execute(sql,(stockid,year,season))
            record = cur.fetchall()
            if len(record) == 0: # 還沒新增過
                sql = 'insert into ks_original (stockid,year,season,I,T,EBIT,EAIT,GP) values (%s,%s,%s,%s,%s,%s,%s,%s);'
                cur.execute(sql,(stockid,year,season,output["I"],output["T"],output["EBIT"],output["EAIT"],output["GP"]))
                conn.commit()
            else: # 已經新增過
                sql = 'update ks_original set I=%s, T=%s, EBIT=%s, EAIT=%s, GP=%s where stockid=%s and year=%s and season=%s;'
                cur.execute(sql,(output["I"],output["T"],output["EBIT"],output["EAIT"],output["GP"],stockid,year,season))
                conn.commit()
    if log[2] == 0:
        cash_now = send_request(url3,year,season,stockid) # 現在現金流量表
        if cash_now == None:
            # 請求失敗
            print(stockid,"請求現金流量表失敗")
        else:
            # 請求成功
            sql = "update original_log set cash=1 where stockid=%s;"
            cur.execute(sql,(stockid,))
            conn.commit()
            # AD 累積折舊
            if '折舊費用' in cash_now:
                output["AD"] = cash_now["折舊費用"]
            else:
                output["AD"] = 0
            # Div 現金股利
            if '發放現金股利' in cash_now:
                output["Div"] = cash_now["發放現金股利"]
            else:
                output["Div"] = 0
            # 存db
            # 確認是否已有新增過
            sql = 'select * from ks_original where stockid=%s and year=%s and season=%s;'
            cur.execute(sql,(stockid,year,season))
            record = cur.fetchall()
            if len(record) == 0: # 還沒新增過
                sql = 'insert into ks_original (stockid,year,season,AD,Div_) values (%s,%s,%s,%s,%s);'
                cur.execute(sql,(stockid,year,season,output["AD"],output["Div"]))
                conn.commit()
            else: # 已經新增過
                sql = 'update ks_original set AD=%s, Div_=%s where stockid=%s and year=%s and season=%s;'
                cur.execute(sql,(output["AD"],output["Div"],stockid,year,season))
                conn.commit()

    print(stockid,"正常")
    ''' 
    print(output)
    print("輸入")
    while True:
        a = input()
        try:
            print(output[a])
        except Exception as e:
            print(e)
    '''
    '''
    except Exception as e:
        print("掛了")
        logging.error("error: %s", str(stockid)+" "+str(e))
    '''

def main():
    start = time.time()
    stockid = int(sys.argv[1])
    year = int(sys.argv[2])
    season = int(sys.argv[3])
    log = list(map(int,sys.argv[4].split(',')))
    financial_statement(stockid,year,season,log)
    end = time.time()
    print(end-start)

main()

