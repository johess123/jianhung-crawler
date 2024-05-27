from dbConfig import conn, cur
import requests
from bs4 import BeautifulSoup
import time
import sys
import logging

def sendRequest(url,stockid,year,month):
    logging.basicConfig(filename='/home/stock/stock_project/crawler/finstate/sm_error.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') # 記錄log
    try:
        r = requests.post(url, {
            'encodeURIComponent':1,
            'step':1,
            'firstin':1,
            'off':1,
            'TYPEK':'all',
            'year':str(year),
            'month': month,
            'co_id':str(stockid),
            'queryName':str(stockid),
            'inputType':str(stockid),
            'isnew':False,
        })
        soup = BeautifulSoup(r.text,'html.parser')
        is_voluntary = soup.find('h3').getText()
        if is_voluntary != "查詢彙總報表": # 例外情況(屬自願申報)
            return 0
        else:
            result = soup.find(class_='odd')
            time.sleep(5)
            try:
                return int(result.getText().strip().replace(",",""))
            except:
                r = requests.post(url, {
                    'step':0,
                    'firstin':True,
                    'off':1,
                    'yearmonth':str(year)+month,
                    'TYPEK':'sii',
                    'year':str(year),
                    'month': month,
                    'co_id':str(stockid),
                })
                soup = BeautifulSoup(r.text,'html.parser')
                result = soup.find(class_='odd')
                time.sleep(5)
                return int(result.getText().strip().replace(",",""))

    except Exception as e:
        print("掛了")
        logging.error("error: %s", str(stockid)+" "+str(e))

def financial_statement(stockid,year,month,log):
    if log == 0: # 發送失敗
        url = 'https://mops.twse.com.tw/mops/web/ajax_t05st10_ifrs'
        sm = sendRequest(url,stockid,year,month)
        if sm == None:
            # 請求失敗
            print(stockid,"請求營收失敗")
        else:
            # 請求成功
            sql = "update Sm_log set sm=1 where stockid=%s;"
            cur.execute(sql,(stockid,))
            conn.commit()
            # 存營收
            sql = 'insert into stock_app_sm (stockid,year,month,sm) values (%s,%s,%s,%s);'
            month = int(month)
            cur.execute(sql,(stockid,year,month,sm))
            conn.commit()

def main():
    start = time.time()
    stockid = int(sys.argv[1])
    year = int(sys.argv[2])
    month = sys.argv[3]
    if len(month) < 2:
        month = "0"+month
    log = int(sys.argv[4])
    financial_statement(stockid,year,month,log)
    end = time.time()
    print(end-start)

main()
