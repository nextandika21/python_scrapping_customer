#Import our modules or packages that we will need to scrape a website
from bs4 import BeautifulSoup
import requests
import csv
import time
import re

import psycopg2
from config import config
from connect import connect

conn = None
try:
    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Waiting for connect to database...')
    conn = psycopg2.connect(**params)
            
    # create a cursor
    cur = conn.cursor()

    # Update Monitoring Schedule
    sqlupdate1 = """ UPDATE monitoring_schedules SET description = %s WHERE task = %s """
    cur.execute(sqlupdate1, ('Running Process','Mining Information'))
    
    # execute a statement
    cur.execute("SELECT * FROM customers WHERE status_mining = '1' AND prioritas > 0")

    # display the PostgreSQL database server version
    result = cur.fetchall()
    
    for r in result:
        id_customer = r[0]
        
        cur.execute("SELECT * FROM customer_sources WHERE customer_id = %s", [id_customer])
        sourceCustomer = cur.fetchall()

        cur.execute("SELECT * FROM classifications")
        classification = cur.fetchall()

        for s in sourceCustomer:
            uri = s[3]
 
            # identifying yourself
            headers = {'user-agent' : 'Mozilla/5.0 (Macintosh; PPC Mac OS X 10_8_2) AppleWebKit/531.2 (KHTML, like Gecko) Chrome/26.0.869.0 Safari/531.2'}

            # make an empty array for your data
            rows = []

            # open the web site
            urls = [uri]

            def scrapeContent(url):
                # add headers
                try:
                    page = requests.get(url, headers=headers)
                    page_content = page.content

                    webnone = 0

                    if ".pdf" in url :
                        print("PDF")
                        webnone = 1

                    if "soundcloud" in url:
                        sqlinsert1 = """ INSERT INTO information (customer_id, data, count) VALUES (%s,%s,%s) """
                        cur.execute(sqlinsert1, (id_customer,'Hobi','Musik',1))
                        webnone = 1

                    if "youtube" in url:
                        sqlinsert1 = """ INSERT INTO information (customer_id, data, count) VALUES (%s,%s,%s) """
                        cur.execute(sqlinsert1, (id_customer,'Pekerjaan','Youtuber',1))
                        webnone = 1

                    if webnone == 0 :
                        # parse the page through the BeautifulSoup library
                        soup = BeautifulSoup(page_content, "html.parser", from_encoding="iso-8859-1")

                        interval = 0
                        for elem in soup(text=re.compile(c[1])):
                            interval=interval+1
                            
                        print(c[1])
                        print("Jumlah : "+str(interval))

                        if interval > 0 :
                            sqlinsert1 = """ INSERT INTO information (customer_id, data, count) VALUES (%s,%s,%s) """
                            cur.execute(sqlinsert1, (id_customer,c[1],interval))

                    sqlupdate2 = """ UPDATE customers SET status_mining = %s WHERE id = %s """
                    cur.execute(sqlupdate2, ('2', id_customer))
                except:
                    print("Connection refused by the server..")

            for url in urls:
                    scrapeContent(url)
                
   
    # close the communication with the PostgreSQL
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.commit()
        conn.close()
        print('Database connection closed.')
        input("Press enter to exit ...")
