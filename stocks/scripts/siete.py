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
    def configure_args(self):
        super(MRFilter7, self).configure_args()
        self.add_passthru_arg('--data', default='3|2019-01-01|2022-01-31', help="please enter percentage and dates")

    def mapper(self, _,line): 
        datos = self.options.data.split("|")
        fecha_inicio = datetime.strptime(datos[1],"%Y-%m-%d") 
        fecha_fin = datetime.strptime(datos[2],"%Y-%m-%d")
        porcentaje = datos[0]

        field_line = line.split(",")
        fecha = datetime.strptime(field_line[0],"%Y-%m-%d")
        if fecha >= fecha_inicio and fecha <= fecha_fin: 
            yield field_line[7], (field_line[0] +"|" + field_line[1]+"|"+porcentaje)

    
    def reducer(self, key, values):
        fecha_inicial_w =datetime.strptime("2111-01-01","%Y-%m-%d") 
        valor_inicial_w = 0
        t_minimo1w = 99999999
        t_maximo1w = 0
        porcentaje = 0

        for data  in values:
            #maximo y minimo
            datos = data.split("|")
            valor = datos[1]
            porcentaje = datos[2]
            fecha = datetime.strptime(datos[0],"%Y-%m-%d")

            if float(valor) < float(t_minimo1w):
                t_minimo1w = valor
            if float(valor) > float(t_maximo1w):
                t_maximo1w = valor
            if fecha <= fecha_inicial_w:
                fecha_inicial_w = fecha
                valor_inicial_w = valor

        if t_minimo1w == 9999999: t_minimo1w = 0
 
        
        incre = (float(t_maximo1w)-float(valor_inicial_w))/float(valor_inicial_w)

        if incre > float(porcentaje):
            yield "key", (str(incre),key)

            # accion, maximo, minimo, valor_inicial, incremento y decremento


    def reducer2(self, key, values):

        self.alist = []
 
        for value in values:
            self.alist.append(value)
  
        
        self.blist = []
        x = len (self.alist)
       
        for i in range(x):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
        
        for i in range(x):
            yield self.blist[i]
    
   
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.reducer)
                ,
            MRStep(reducer = self.reducer2)
        ]
 

if __name__ == '__main__':
    MRFilter7.run()