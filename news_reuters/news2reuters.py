from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import csv
import time
from datetime import datetime
from datetime import timedelta
import os
import re
import numpy as np

def have_read(text):
    try:
        if text.find(" read more") != -1:
            text = text.split(" read more")[0]
    except:
        pass
    return text

def news():

    year_csv = ""
    last_day = datetime(2022, 3, 16, 1, 1)

    while True:
        
        date_forgoogle = last_day.strftime("%m/%d/%Y")
        date_for_me = last_day.strftime("%Y-%m-%d")
        data = date_for_me.split("-")
        if (year_csv == ""):
            year_csv = data[0]
        if data[0] != year_csv:
            print("new year")
            #os.system('start-dfs.sh')
            #os.system('hdfs dfs -put gnews_'+year_csv+'.csv /user/alumno/ibex35')
            year_csv = data[0]
            
        #print(date_for_me)
        delays = [2, 3, 2, 5, 1, 4]
        delay = np.random.choice(delays)
        time.sleep(delay)
        file = open('gnews_'+year_csv+'.csv', 'a')
        writer = csv.writer(file)
        try:
            centinela = False
            q = "https://www.google.com/search?q=reuters+world+news&num=40&lr=lang_en&hl=en&tbs=lr:lang_1en,cdr:1,cd_min:{},cd_max:{}&tbm=nws&biw=1080&bih=1290&dpr=2"
            q = q.format(date_forgoogle,date_forgoogle)

            req = Request(q, headers={'User-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"})
            webpage = urlopen(req).read()
            soup = BeautifulSoup(webpage, 'html.parser')
            #print(soup)
            #^C.*AA$
            regex = re.compile('^C.*AA$')

            for link in soup.find_all('div', attrs={'data-hveid' : regex}):

                fuente = link.span.string

                if fuente == "Reuters":
                    try:
                        date = link.find('div', class_='OSrXXb ZE0LJd').span.string
                        link_noticia = link.find('a', class_='WlydOe')['href']
                        req2 = Request(link_noticia, headers={'User-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"})
                        webpage2 = urlopen(req2).read()
                        soup2 = BeautifulSoup(webpage2, 'html.parser')

                        titular = soup2.find('h1', attrs={'data-testid' : 'Heading'}).getText()
                        
                        paragraph0 = ""
                        paragraph1 = ""
                        paragraph2 = ""
                        paragraph3 = ""
                        paragraph4 = ""
                        paragraph5 = ""
                        paragraph6 = ""
                        paragraph7 = ""
                        paragraph8 = ""
                        paragraph9 = ""
                        try:
                            paragraph0 = soup2.find('p', attrs={'data-testid' : 'paragraph-0'}).getText()
                            if paragraph0.find("(Reuters) - ") != -1:
                                paragraph0 = paragraph0.split(" - ")[1]
                        except:
                            pass
                        try:
                            paragraph1 = soup2.find('p', attrs={'data-testid' : 'paragraph-1'}).getText()
                            paragraph1 = have_read(paragraph1)
                        except:
                            pass
                        try:
                            paragraph2 = soup2.find('p', attrs={'data-testid' : 'paragraph-2'}).getText()
                            paragraph2 = have_read(paragraph2)
                        except:
                            pass
                        try:
                            paragraph3 = soup2.find('p', attrs={'data-testid' : 'paragraph-3'}).getText()
                            paragraph3 = have_read(paragraph3)
                        except:
                            pass
                        try:
                            paragraph4 = soup2.find('p', attrs={'data-testid' : 'paragraph-4'}).getText()
                            paragraph4 = have_read(paragraph4)
                        except:
                            pass
                        try:
                            paragraph5 = soup2.find('p', attrs={'data-testid' : 'paragraph-5'}).getText()
                            paragraph5 = have_read(paragraph5)
                        except:
                            pass
                        try:
                            paragraph6 = soup2.find('p', attrs={'data-testid' : 'paragraph-6'}).getText()
                            paragraph6 = have_read(paragraph6)
                        except:
                            pass
                        try:
                            paragraph7 = soup2.find('p', attrs={'data-testid' : 'paragraph-7'}).getText()
                            paragraph7 = have_read(paragraph7)
                        except:
                            pass
                        try:
                            paragraph8 = soup2.find('p', attrs={'data-testid' : 'paragraph-8'}).getText()
                            paragraph8 = have_read(paragraph8)
                        except:
                            pass
                        try:
                            paragraph9 = soup2.find('p', attrs={'data-testid' : 'paragraph-9'}).getText()
                            paragraph9 = have_read(paragraph9)
                        except:
                            pass
                        article = titular + " " + paragraph0 + " " + paragraph1 + " " + paragraph2
                        article = article + " " + paragraph3 + " " + paragraph4 + " " + paragraph5
                        article = article + " " + paragraph6 + " " + paragraph7 + " " + paragraph8 + " " + paragraph9
                        file2 = open('log.csv', 'a')
                        writer2 = csv.writer(file2)
                        writer2.writerow([titular])
                        file2.close()
                        centinela = True
                    except Exception as e:   
                        print("Exception tipo 2: " + str(e))
                        time.sleep(15)
                        file2 = open('log.csv', 'a')
                        writer2 = csv.writer(file2)
                        writer2.writerow([date_for_me,"Error tipo 2" ,article])
                        file2.close()


                    delays2 = [2, 2, 3, 4, 3]
                    delay2 = np.random.choice(delays2)
                    time.sleep(delay2)

                    writer.writerow([date_for_me,fuente,article,date,link_noticia])
                    fuente =""
                    article=""
                    date =""
                    link_noticia =""
                    
            
            
            file.close()
            last_day = last_day - timedelta(days=1)
            if centinela == False:
                print("Error desconocido")
            
            print(last_day)
        except Exception as e:   
            print("Exception tipo 1 " + str(e))
            time.sleep(120)
            file2 = open('log.csv', 'a')
            writer2 = csv.writer(file2)
            writer2.writerow([last_day,"Error tipo 1"])
            file2.close()


if __name__ == '__main__':
    news()