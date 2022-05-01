from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import time
from datetime import datetime
import os

TRY_LIMIT = 15
SLEEP_TIMER = 60

start_url = "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"
Lasthour = 77
Fecha_csv = ""

while True:
    time.sleep(30)
    now = datetime.now()
    # YYYY-mm-dd
    mydate = now.strftime("%Y-%m-%d")
    if Fecha_csv == "":
        Fecha_csv = mydate
    myhour = now.strftime("%H:%M")
    day = now.weekday()

    # si cambia el dia subimos el fichero a HDFS
    if Fecha_csv != mydate:
        print("nuevo dia")
        os.system('start-dfs.sh')
        os.system('hdfs dfs -put '+Fecha_csv+'.csv /user/alumno/ibex35')
        Fecha_csv = mydate
    
    if day >= 0 and day <= 4:
        reloj = myhour.split(":")
        lahora = int(reloj[0])
        elminuto = int(reloj[1])
        if lahora >= 9 and lahora <= 19 and elminuto == 30 and lahora != Lasthour:
            
            Lasthour = lahora
            print("now =", now)
            try_number=1

            while try_number < TRY_LIMIT:
                try:
                    with webdriver.Firefox() as driver:
                        wait = WebDriverWait(driver, 50)
                        driver.get(start_url)

                        time.sleep(2)
                        f = open(Fecha_csv+'.csv',"a")
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
                        try_number = TRY_LIMIT
                except Exception as e:
                    print("Exception" + str(try_number) + " " + str(e))
                    try_number+1
                    time.sleep(SLEEP_TIMER)
            print("Ready")
                    

            
            
            
    