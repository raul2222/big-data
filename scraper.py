from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import time
from datetime import datetime
import os

start_url = "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"
lasthour = 77
fecha_csv = ""

while True:
    time.sleep(30)
    now = datetime.now()
    # YYYY-mm-dd
    mydate = now.strftime("%Y-%m-%d")
    if fecha_csv == "":
        fecha_csv = mydate
    myhour = now.strftime("%H:%M")
    day = now.weekday()

    # si cambia el dia subimos el fichero a HDFS
    if fecha_csv != mydate:
        print("nuevo dia")
        os.system('hdfs dfs -put '+fecha_csv+'.csv /user/alumno/ibex35')
        fecha_csv = mydate
    
    if day >= 0 and day <= 4:
        reloj = myhour.split(":")
        lahora = int(reloj[0])
        elminuto = int(reloj[1])
        if lahora >= 9 and lahora <= 18 and elminuto == 30 and lahora != lasthour:
            
            lasthour = lahora
            print("now =", now)

            with webdriver.Firefox() as driver:
                wait = WebDriverWait(driver, 15)
                driver.get(start_url)

                time.sleep(1)
                f = open(fecha_csv+'.csv',"a")
                #recuperamos listado de acciones
                commodities = driver.find_elements_by_xpath("/html/body/main/section/div/div/div/ul/li/div/section/div/article/section[2]/ul[2]/li[1]/div/section/table/tbody/tr")

                for stock in commodities:
                    #recuperamos la información de cada acción
                    values = stock.find_elements_by_xpath("td")       
                    list_of_values = [x.text for x in values]
                    row = ""
                    for value in range(len(list_of_values)-1):
                        if value != (len(list_of_values)-2):
                            #eliminamos el punto de millar y cambiamos la coma decimal
                            v = list_of_values[value].replace('.','')
                            v = v.replace(',','.')
                            row = row + v + ","
                        else:
                            row = row + str(list_of_values[value]) + "," + str(now)
                            print(row)
                            f.write(row+"\n")
                f.close()
                
            
            
    