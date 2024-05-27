import requests
import datetime
import logging
from dbConfig import conn,cur
import time

def main():
    # 今天日期
    date = datetime.datetime.now().strftime("%Y%m%d")
    #date= "20230729"
    
    # 爬取股票資訊
    #print("爬取股票資訊...")
    
    
    # 重複爬取直到成功
    logging.basicConfig(filename='/home/stock/stock_project/crawler/price/price_error1.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    while True:
        try:
            url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date="+date+"&type=ALL&response=csv"
            r = requests.get(url)
            #print(r.status_code)
            if r.status_code != 200:
                logging.error("error: status_code is not 200")
                time.sleep(5)
            else:
                file = r.text.split("\n")
                #logging.error("股價爬蟲 %s", file)
                break
        except Exception as e:
            logging.error("error: %s", str(e))
            time.sleep(5) # 失敗間隔5秒
    
    date = date[0:4]+"-"+date[4:6]+"-"+date[6:] # 調整時間格式
    # 檢查今天是否有開盤
    if file != ['']: # 有
        #print("開始寫入各股收盤價...")
        stockStart = False
        for i in range(len(file)):
            stock = file[i].split('","')
            if stockStart == True:
                if "備註" in stock[0]:
                    break
                if stock[8] != "--":
                    sql = "INSERT INTO stock_app_allprice(stockid,stockname,date,price) values (%s,%s,%s,%s);"
                    cur.execute(sql,(stock[0].replace("=","").replace('"',""),stock[1],date,float(stock[8].replace(",",""))*100))
            elif "證券代號" in stock[0]:
                stockStart = True
        conn.commit()
        loggin.info("success: %s", "success to crawl today stock")
        #print("寫入完成!")
    # 沒有
    else:
        logging.info("error: %s", "today stock not open")

        #print("今日沒有開盤")
main()
