from itertools import count
from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import re

#1. Generar un listado mensual
#donde se indique, para cada acci ́on, su valor inicial, final, m ́ınimo y m ́aximo.

# Entrada ->
# Fecha, Apertura, Alto, Bajo, Cierre, Ajuste_Cierre, Volumen, Nombre_Accion 
#
class MRFilter7(MRJob):

    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        year = 2022
        mes = 4
        field_line = line.split(",")
        fecha = field_line[0].split("-")
        if int(fecha[0]) == year and int(fecha[1]) == mes : #semana
            fecha_alt = fecha[2] + "-" + fecha[1]+"-" + fecha[0]
            yield (fecha_alt + " - " + field_line[7]+"              "), (field_line[1] + "        -" +field_line[4] + "        -" +field_line[3] +"        -" +field_line[2])


if __name__ == '__main__':
    MRFilter7.run()
