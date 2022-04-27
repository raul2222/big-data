from __future__ import division
from itertools import count
from random import paretovariate
from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import re
from datetime import datetime
from datetime import timedelta


4.
class MRFilter7(MRJob):

    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        accion = "iberdrola"
        now = datetime.now()
        inicio =  now.strftime("%d/%m/%Y %H:%M:%S")
        fecha_1h = now - timedelta(hours=1)
        fecha_1w = now - timedelta(weeks=1)
        fecha_1m = now - timedelta(days=30)

        field_line = line.split(",")
        
        #fecha_alt = fecha[2] + "-" + fecha[1]+"-" + fecha[0]
        fecha = datetime.strptime(field_line[0],"%Y-%m-%d")
        
        if fecha <= now and fecha >= fecha_1m:
            if fecha >= fecha_1h:
                yield accion, (field_line[2]+"-"+field_line[3]+"-1h")
            else:
                if fecha >= fecha_1w:
                    yield accion, (field_line[2]+"-"+field_line[3]+"-1w")
                
        yield accion, (field_line[2]+"-"+field_line[3]+"-1m")


    
    def reducer(self, key, values):

        t_minimo1h = 999999
        t_maximo1h = 0
        t_minimo1w = 999999
        t_maximo1w = 0
        t_minimo1m = 999999
        t_maximo1m = 0

        for data  in values:
            #maximo y minimo
            datos = data.split("-")
            maximo = datos[0]
            minimo = datos[1]
            tipo = datos[2]
            if tipo == "1h" and float(minimo) < float(t_minimo1h):
                t_minimo1h = minimo
            else:
                if tipo == "1w" and float(minimo) < float(t_minimo1w):
                    t_minimo1w = minimo 
                else:
                    t_minimo1m = minimo


            if tipo == "1h" and float(maximo) > float(t_maximo1h):
                t_maximo1h = maximo
            else:
                if tipo == "1w" and float(maximo) < float(t_maximo1w):
                    t_minimo1w = minimo 
                else:
                    t_minimo1m = minimo

        if t_minimo1h == 999999:
            t_minimo1h = 0
    
 
        yield key, (str(t_minimo1h) + " | " + str(t_maximo1h) + " | " + str(t_minimo1w) + " | " + str(t_maximo1w) + " | " + str(t_minimo1m) + " | " + str(t_maximo1m) )
            # accion, maximo, minimo, valor_inicial, incremento y decremento


if __name__ == '__main__':
    MRFilter7.run()