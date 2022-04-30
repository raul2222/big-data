from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


#4. 
#Fecha, Apertura, Alto, Bajo, Cierre, Ajuste_Cierre, Volumen, Nombre_Accion 
class MRFilter7(MRJob):

    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        fecha_1w = datetime.now() - timedelta(weeks=1)
        fecha_1m = datetime.now() - relativedelta(months=1)

        field_line = line.split(",")

        fecha = datetime.strptime(field_line[0],"%Y-%m-%d")
        
        if fecha >= fecha_1w and fecha <= datetime.now() :
            yield field_line[7], (str(fecha).split(" ")[0] + "|" +field_line[1]+"|1w")
        
        if fecha >= fecha_1m and fecha <= datetime.now() :
            yield field_line[7], (str(fecha).split(" ")[0] + "|" +field_line[1]+"|1m")

    
    def reducer(self, key, values):
        fecha_inicial_w =datetime.strptime("2111-01-01","%Y-%m-%d") 
        fecha_inicial_m =datetime.strptime("2111-01-01","%Y-%m-%d") 
        valor_inicial_w = 0
        t_minimo1w = 99999999
        t_maximo1w = 0
        valor_inicial_m = 0
        t_minimo1m = 99999999
        t_maximo1m = 0

        for data  in values:
            #maximo y minimo
            datos = data.split("|")
            valor = datos[1]
            tipo = datos[2]
            fecha = datetime.strptime(datos[0],"%Y-%m-%d")

            if tipo == "1w" and float(valor) < float(t_minimo1w):
                t_minimo1w = valor
            if tipo == "1w" and float(valor) > float(t_maximo1w):
                t_maximo1w = valor
            if tipo == "1w" and fecha <= fecha_inicial_w:
                fecha_inicial_w = fecha
                valor_inicial_w = valor


            if tipo == "1m" and float(valor) < float(t_minimo1m):
                t_minimo1m = valor
            if tipo == "1m" and float(valor) > float(t_maximo1m):
                t_maximo1m = valor
            if tipo == "1m" and fecha <= fecha_inicial_m:
                fecha_inicial_m = fecha
                valor_inicial_m = valor


        if t_minimo1w == 9999999: t_minimo1w = 0
        if t_minimo1m == 9999999: t_minimo1m = 0
        
        #incre_w = (float(t_maximo1w)-float(valor_inicial_w))/float(valor_inicial_w)
        #incre_m = (float(t_maximo1m)-float(valor_inicial_m))/float(valor_inicial_m)
        decre_w = (float(t_minimo1w)-float(valor_inicial_w))/float(valor_inicial_w)
        decre_m = (float(t_minimo1m)-float(valor_inicial_m))/float(valor_inicial_m)
 
        #yield key +"w", str(fecha_inicial_w).split(" ")[0] + " | " + valor_inicial_w + " | " + str(incre_w)
        #yield key +"m", str(fecha_inicial_m).split(" ")[0] + " | " + valor_inicial_m + " | " + str(incre_m)
        if decre_w < 0:
            yield "key", (str(decre_w),key+ " | Semana")
        if decre_m < 0:
            yield "key" , (str(decre_m), (key + " | Mes"))
            # accion, maximo, minimo, valor_inicial, incremento y decremento
  
    def reducer2(self, key, values):

        self.alist = []
        self.alistm = []
        for value in values:
            if str(value).find("Semana") > 0:
                #yield value
                self.alist.append(value)
            else:   
                self.alistm.append(value)
            
        
        self.blist = []
        self.blistm = []
        for i in range(5):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
            self.blistm.append(max(self.alistm))
            self.alistm.remove(max(self.alistm))
        
        
        for i in range(5):
            yield self.blist[i]

        for i in range(5):
            yield self.blistm[i]
    
   
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.reducer)
                ,
            MRStep(reducer = self.reducer2)
        ]
 
if __name__ == '__main__':
    MRFilter7.run()