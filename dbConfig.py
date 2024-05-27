import mysql.connector 

try:
    conn = mysql.connector.connect(
        user="stock",
        password="stock!Q@W",
        host="127.0.0.1",
        port=3306,
        database="stock"
    )
except:
    print("Error connecting to DB")
    exit(1)
cur=conn.cursor()
