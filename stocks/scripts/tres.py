from __future__ import division
from itertools import count
from random import paretovariate
from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import re
from datetime import datetime


#3
class MRFilter7(MRJob):

    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        fecha_inicio = datetime.strptime("2020-01-01","%Y-%m-%d") 
        fecha_fin = datetime.strptime("2020-03-31","%Y-%m-%d")
        accion = "iberdrola"

        field_line = line.split(",")
        fecha = datetime.strptime(field_line[0],"%Y-%m-%d")
        if accion == field_line[7] and fecha >= fecha_inicio and fecha <= fecha_fin: #accion
            yield field_line[7], (field_line[0] + "*" + field_line[1] + "-" + field_line[4] + "-" +field_line[3] +"-" +field_line[2])

    def reducer(self, key, values):
        t_fecha =datetime.strptime("2050-01-01","%Y-%m-%d") 
        valor_inicial = 0
        t_minimo = 99999999
        t_maximo = 0
        decre = 0
        incre = 0
        for data  in values:
            #fecha, apertura, cierre, minimo, maximo
            datos = data.split("*")
            dat1 = datos[0]
            dat2 = datos[1].split("-")
            
            fecha = datetime.strptime(dat1,"%Y-%m-%d")
            apertura = dat2[0]
            minimo = dat2[2]
            maximo = dat2[3]
            if float(minimo) < float(t_minimo):
                t_minimo = minimo
            if float(maximo) > float(t_maximo):
                t_maximo = maximo
            if fecha < t_fecha:
                t_fecha = fecha
                valor_inicial = apertura
        incre = (float(t_maximo)-float(valor_inicial))/float(valor_inicial)
        decre = (float(t_minimo)-float(valor_inicial))/float(valor_inicial)
        yield key, (str(t_maximo) + " | " + str(t_minimo) + " | " + str(valor_inicial)+ " | " + str(incre)+ " | " + str(decre))
            # accion, maximo, minimo, valor_inicial, incremento y decremento

if __name__ == '__main__':
    MRFilter7.run()
