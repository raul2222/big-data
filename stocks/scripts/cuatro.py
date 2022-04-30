from __future__ import division
from mrjob.job import MRJob
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


#4. 
#Fecha, Apertura, Alto, Bajo, Cierre, Ajuste_Cierre, Volumen, Nombre_Accion 
class MRFilter7(MRJob):

    def configure_args(self):
        super(MRFilter7, self).configure_args()
        self.add_passthru_arg('--data', default='iberdrola', help="please enter the stock name")


    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        accion = self.options.data.upper()
        fecha_1h = datetime.now() - timedelta(hours=1)
        fecha_1w = datetime.now() - timedelta(weeks=1)
        fecha_1m = datetime.now() - relativedelta(months=1)

        field_line = line.split(",")

        fecha = datetime.strptime(field_line[0] + " 00:00:00","%Y-%m-%d %H:%M:%S")
        
        if fecha >= fecha_1h and fecha <= datetime.now() and accion == field_line[7]:
            yield accion, (str(fecha) + "|" +field_line[1]+"|1h")
        
        if fecha >= fecha_1w and fecha <= datetime.now() and accion == field_line[7]:
            yield accion, (str(fecha) + "|" +field_line[1]+"|1w")
        
        if fecha >= fecha_1m and fecha <= datetime.now() and accion == field_line[7]:
            yield accion, (str(fecha) + "|" +field_line[1]+"|1m")

    
    def reducer(self, key, values):

        t_minimo1h = 9999999
        t_maximo1h = 0
        t_minimo1w = 9999999
        t_maximo1w = 0
        t_minimo1m = 9999999
        t_maximo1m = 0

        for data  in values:
            #maximo y minimo
            datos = data.split("|")
            valor = datos[1]
            tipo = datos[2]

            if tipo == "1h" and float(valor) < float(t_minimo1h):
                t_minimo1h = valor
            if tipo == "1h" and float(valor) > float(t_maximo1h):
                t_maximo1h = valor
            if tipo == "1w" and float(valor) < float(t_minimo1w):
                t_minimo1w = valor
            if tipo == "1w" and float(valor) > float(t_maximo1w):
                t_maximo1w = valor
            if tipo == "1m" and float(valor) < float(t_minimo1m):
                t_minimo1m = valor
            if tipo == "1m" and float(valor) > float(t_maximo1m):
                t_maximo1m = valor


        if t_minimo1h == 9999999: t_minimo1h = 0
        if t_minimo1w == 9999999: t_minimo1w = 0
        if t_minimo1m == 9999999: t_minimo1m = 0
        

 
        yield key, (str(t_minimo1h) + " | " + str(t_maximo1h) + " | " + str(t_minimo1w) + " | " + str(t_maximo1w) + " | " + str(t_minimo1m) + " | " + str(t_maximo1m) )
            # accion, maximo, minimo, valor_inicial, incremento y decremento


if __name__ == '__main__':
    MRFilter7.run()