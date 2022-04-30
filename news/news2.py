from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import csv
import time
from datetime import datetime
from datetime import timedelta
import os
import numpy as np


year_csv = ""
last_day = datetime(2021, 12, 31, 1, 1)

while True:
    
    date_forgoogle = last_day.strftime("%m/%d/%Y")
    date_for_me = last_day.strftime("%Y-%m-%d")
    data = date_for_me.split("-")
    if (year_csv == ""):
        year_csv = data[0]
    if data[0] != year_csv:
        print("new year")
        os.system('start-dfs.sh')
        os.system('hdfs dfs -put gnews_'+year_csv+'.csv /user/alumno/ibex35')
        year_csv = data[0]
        
    if year_csv == "2022":
        time.sleep(99999999)
    #print(date_for_me)
    delays = [66, 51, 71, 53, 70, 62]
    delay = np.random.choice(delays)
    time.sleep(delay)
    file = open('gnews_'+year_csv+'.csv', 'a')
    writer = csv.writer(file)
    try:
        q = "https://www.google.com/search?q=world+news&num=90&lr=lang_en&hl=en&tbs=lr:lang_1en,cdr:1,cd_min:{},cd_max:{}&tbm=nws&biw=1080&bih=1290&dpr=2"
        q = q.format(date_forgoogle,date_forgoogle)
 

        req = Request(q, headers={'User-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"})
        webpage = urlopen(req).read()
        soup = BeautifulSoup(webpage, 'html.parser')
        centinela = False
        for link in soup.find_all("div", class_="iRPxbe"):
            #print(link)
            news = link.find('div', class_='mCBkyc y355M JQe2Ld nDgy9d').string
            newspaper = link.span.string
            date = link.find('div', class_='OSrXXb ZE0LJd').span.string
            writer.writerow([date_for_me,news,newspaper,date])
            centinela = True
        file.close()
        last_day = last_day - timedelta(days=1)
        if centinela == False:
            print("Error desconocido")
        
        print(last_day)
        
    except Exception as e:   
        print("Exception " + str(e))


