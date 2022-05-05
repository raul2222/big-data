from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
#3
# Entrada ->
# Fecha, Apertura, Alto, Bajo, Cierre, Ajuste_Cierre, Volumen, Nombre_Accion 



class MRFilter7(MRJob):

    def configure_args(self):
        super(MRFilter7, self).configure_args()
        self.add_passthru_arg('--data', default='IBEX35 2022-01-01 2022-04-21', help="please enter the name and dates")

    def mapper(self, _,line): 
        datos = self.options.data.split(" ")
        fecha_inicio = datetime.strptime(datos[1],"%Y-%m-%d") 
        fecha_fin = datetime.strptime(datos[2],"%Y-%m-%d")
        fecha_comodin = datetime.strptime(datos[1],"%Y-%m-%d") 
        print(fecha_comodin)

        

        while fecha_fin > fecha_inicio:
            field_line = line.split(",")
            fecha = datetime.strptime(field_line[0],"%Y-%m-%d")
            fecha_comodin = datetime.strptime(str(fecha_comodin).split(" ")[0],"%Y-%m-%d") - timedelta(weeks=1)
            print(fecha_comodin )
            print(str(fecha_inicio))
            if fecha >= fecha_comodin and fecha <= fecha_inicio: 
                #yield "IBEX35", (field_line[0] + "|" + field_line[1])
                print(str(fecha_comodin))
                print(str(fecha_inicio))
        fecha_fin = fecha_comodin

    def reducer(self, key, values):
        fecha_inicial =datetime.strptime("2111-01-01","%Y-%m-%d") 
        valor_inicial = 0
        valor_minimo = 99999999
        valor_maximo = 0
        decre = 0
        incre = 0
        for data  in values:
            #fecha, apertura
            datos = data.split("|")
            valor = datos[1]
            fecha = datetime.strptime(datos[0],"%Y-%m-%d")
            
            if float(valor) < float(valor_minimo):
                valor_minimo = valor
            if float(valor) > float(valor_maximo):
                valor_maximo = valor

            if fecha <= fecha_inicial:
                fecha_inicial = fecha
                valor_inicial = valor
        if valor_minimo == 99999999: valor_minimo = 0

        #menos_decimales = round(division_con_decimales, 2)

        incre = (float(valor_maximo)-float(valor_inicial))/float(valor_inicial)
        decre = (float(valor_minimo)-float(valor_inicial))/float(valor_inicial)
        yield key, (str(fecha_inicial).split(" ")[0] + " | " + str(valor_inicial) + " | " + str(valor_minimo) + " | " + str(valor_maximo)+ " | " + str(incre)+ " | " + str(decre))
            # accion, fecha valor inicial, inicial, minimo, maximo, incremento, decremento

if __name__ == '__main__':
    MRFilter7.run()

