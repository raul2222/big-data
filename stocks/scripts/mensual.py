from mrjob.job import MRJob
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

#2. Generar un listado mensual
#donde se indique, para cada acci ́on, su valor inicial, final, m ́ınimo y m ́aximo.

# Entrada ->
# Fecha, Apertura, Alto, Bajo, Cierre, Ajuste_Cierre, Volumen, Nombre_Accion 
#
class MRFilter7(MRJob):

    #start-dfs.sh start-yarn.sh
    def mapper(self, _,line): 
        field_line = line.split(",")
        fecha_1m = datetime.now() - relativedelta(months=1)
        fecha = datetime.strptime(field_line[0],"%Y-%m-%d")

        if fecha >= fecha_1m and fecha <= datetime.now(): #semana
            yield (str(fecha).split(" ")[0] + " - " + field_line[7]), (field_line[1] + " | " +field_line[4] + " | " +field_line[3] +" | " +field_line[2])


if __name__ == '__main__':
    MRFilter7.run()
