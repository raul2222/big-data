import string
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from datetime import timedelta

#1. Generar un listado semanal (de la semana actual)
#donde se indique, para cada acci ́on, su valor inicial, final, m ́ınimo y m ́aximo.

# Entrada ->
# Fecha, Apertura, Alto, Bajo, Cierre, Ajuste_Cierre, Volumen, Nombre_Accion 
#
class MRFilter7(MRJob):

    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        field_line = line.split(",")
        fecha_1w = datetime.now() - timedelta(weeks=1)
        fecha = datetime.strptime(field_line[0],"%Y-%m-%d")
        
        if fecha >= fecha_1w and fecha <= datetime.now(): #semana
            yield (str(fecha).split(" ")[0] + " - " + field_line[7]), (field_line[1] + " | " +field_line[4] + " | " +field_line[3] + " | " +field_line[2])


if __name__ == '__main__':
    MRFilter7.run()
