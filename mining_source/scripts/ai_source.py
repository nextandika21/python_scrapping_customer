#Import our modules or packages that we will need to scrape a website
from bs4 import BeautifulSoup
import requests
import csv
import time
import re
import random
import logging

import psycopg2
from config import config
from connect import connect

from urllib.parse import unquote

from fake_useragent import UserAgent

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
    cur.execute(sqlupdate1, ('Running Process','Mining Source'))
    
    # execute a statement
    cur.execute("SELECT * FROM customers WHERE status_mining = '0'")

    # display the PostgreSQL database server version
    result = cur.fetchall()

    # Generate Proxies
    proxies = []
    def get_proxies():
        linkproxy = "https://www.freeproxylists.net/"
        # get the HTTP response and construct soup object
        soup = BeautifulSoup(requests.get(linkproxy).content, "html.parser")
        print(soup)
        for row in soup.find("tbody", attrs={"id": "proxylist_table"}).find_all("tr")[1:]:
            tds = row.find_all("td")
            try:
                host = tds[0].text.strip()
                https = tds[1].text.strip()
                if https == 'HTTPS':
                    proxies.append(host)
            except IndexError:
                continue
        return proxies

    def get_session(proxies, headers):
        # construct an HTTP session
        session = requests.session()
        # choose one random proxy
        proxy = random.choice(proxies)
        session.proxies = {"http": proxy, "https": proxy}
        session.headers = headers
        return session
    
    for r in result:
        id_customer = r[0]
        nama_customer = r[1]
        np = r[3]
        nomor_hp = np.replace("+62", "0")
        
        print("Nama : "+nama_customer)
        print("Nomor HP : "+nomor_hp)
        if nomor_hp != '0':
            # identifying yourself            
            get_proxies()
            ua = UserAgent().random
            
            headers = {'user-agent': ua,
                       'server': 'gws'}

            # make an empty array for your data
            rows = []

            # open the web site
            urls = ["https://www.google.com/search?q="+nomor_hp]

            def scrapeContent(url):
                # add headers
                s = get_session(proxies, headers)
                page = s.get(url)
                print(page.headers)
                page_content = page.content
                # parse the page through the BeautifulSoup library
                soup = BeautifulSoup(page_content, "html.parser")
                flagban = soup.body.findAll(text=re.compile('Our systems have detected unusual traffic from your computer network.'))
                if flagban:
                    print("GOT BANNED!!!")
                    cur.close()
                    conn.commit()
                    conn.close()
                    input("Press enter to exit ...")
                    exit()
                else:
                    all_groupings = soup.find_all('div', class_='egMi0 kCrYT')
                    all_groupings2 = soup.find_all('div', class_='kCrYT')
                    for grouping in all_groupings:
                        anchortag = grouping.find('a')
                        data = unquote(anchortag['href'].split("&")[4][4:])
                        source = grouping.find('div', class_='BNeawe UPmit AP7Wnd').get_text().split(" › ")[0]
                        if ((source != 'wrsa.ru') and (source != 'whocalled.cn') and (source != 'mujerbella.net') and (source != 'anruferauskunft.de') and (source != 'anrufer.info') and (source != 'www.chenumero.it') and (source != 'vizavi-s-mirom.ru') and (source != 'wessennummer.com') and (source != 'armonitv.info') and (source != 'empowereduser.com') and (source != 'www.who-calls-me.de') and (source != 'krealist.ru') and (source != 'pinnebergerleben.info') and (source != 'rapid-indonesia.com') and (source != 'www.telguarder.com') and (source != 'www.portais.pe.gov.br') and (source != 'telefon-kontakte.ch') and (source != 'trovanumero.it') and (source != 'chi-mi-chiama.it') and (source != 'chonso.mobifone.vn') and (source != 'www.aquiappartientcenumero.com') and (source != 'www.who-calls-me.fr') and (source != 'trendshopie.com') and (source != 'zoocrewpetsitting.com') and (source != 'www.qianwanhao.com') and (source != 'kotisaippua.info') and (source != 'wvsleep.com') and (source != 'sahibkimdir.com') and (source != 'meros.io') and (source != 'bobsbodyshoppasadena.com') and (source != 'mean.ly') and (source != 'kzworld.info') and (source != 'youcontrol.com.ua') and (source != 'anrufercheck.de') and (source != 'sweettbites.info') and (source != 'villatorosmaintenance.com') and (source != 'books.google.co.id') and (source != 'pknum.xyz') and (source != 'flowerslove.org') and (source != 'roadsidediesel.com') and (source != 'kubzub.com') and (source != 'chaxundianhua.cn') and (source != 'strisuksa.ac.th') and (source != 'awsd.publisherhunt.com') and (source != 'search-caller.online') and (source != 'tconversion.com') and (source != 'blokadapolaczen.pl') and (source != 'aprationcard.com') and (source != 'www.todalocura.net') and (source != 'www.trovanumeri.com') and (source != 'contact-who-called.com') and (source != 'wir-kaempfen.info') and (source != 'wtconverter.com') and (source != 'benva.xyz') and (source != 'first-auto-glas.de') and (source != 'id.unknownphone.com') and (source != 'jordan.nmbrpro.com') and (source != 'filtrite.biz') and (source != 'www.telefonforsaljare.nu') and (source != 'www.inelenco.com') and (source != 'kzpost.info') and (source != 'kzread.info') and (source != 'kzits.info') and (source != 'kzlife.info') and (source != 'www.annuaire-inverse.tm.fr') and (source != 'whocalled.com.tw') and (source != 'vibrantlivingrk.com') and (source != 'yellowdaisydesign.com') and (source != 'danvillechurchofgod.com') and (source != 'quem-chamou.com') and (source != 'turinsk66.ru') and (source != 'shortcutswigboutique.com') and (source != 'www.hreen.store') and (source != 'dimmichichiama.it') and (source != 'www.whitepages.cloud') and (source != 'yellow-page.us') and (source != 'telefono-numero.it') and (source != 'todalocura.net') and (source != 'ar.fifibook.com') and (source != 'sw.fifibook.com') and (source != 'www.smartphonelist.net') and (source != 'www.annu-phone.com') and (source != 'isdcode.org') and (source != 'clonkeengs.com') and (source != 'ammakalviyagam.info')):
                            if 'facebook' in data :
                                source = 'facebook'
                                data = 'https://'+data[data.find('facebook'):]
                                    
                            print("Source : "+source)
                            print("Data : "+data)
                            sqlinsert2 = """ INSERT INTO customer_sources (customer_id, source, data) VALUES (%s,%s,%s) """
                            cur.execute(sqlinsert2, (id_customer,source,data))

                    prioritas = 0
                    sqlupdate3 = """ UPDATE customers SET status_mining = %s, prioritas = %s WHERE id = %s """
                    cur.execute(sqlupdate3, ('1',prioritas, id_customer))

            for url in urls:
                    scrapeContent(url)
                    time.sleep(5)
        else:
            prioritas = 0
            sqlupdate4 = """ UPDATE customers SET status_mining = %s, prioritas = %s WHERE id = %s """
            cur.execute(sqlupdate41, ('1',prioritas, id_customer))
   
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
