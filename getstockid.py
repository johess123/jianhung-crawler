import requests
from dbConfig import conn,cur
import datetime

def main():
    date = datetime.datetime.now().strftime("%Y%m%d")
    date = "20230818"
    while True:
        # 重複爬取直到成功
        try:
            url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date="+date+"&type=ALL&response=csv"
            file = requests.get(url).text.split("\n")
            break
        except:
            pass
    if file != ['']:
        stockStart = False
        for i in range(len(file)):
            stock = file[i].split('","')
            if stockStart == True:
                if "備註" in stock[0]:
                    break
                if len(stock[0].replace("=","").replace('"',"")) == 4 and stock[0].replace("=","").replace('"',"")[0] != "0":
                    print(stock[0].replace("=","").replace('"',""),stock[1],stock[8].replace(",",""))
                    sql = "INSERT INTO stock_app_allstock(stockid,stockname) values (%s,%s);"
                    cur.execute(sql,(stock[0].replace("=","").replace('"',""),stock[1]))
            elif "證券代號" in stock[0]:
                stockStart = True
        conn.commit()
        print("寫入完成!")
    else:
        print("今日沒有開盤")

main()
