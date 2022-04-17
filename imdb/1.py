from operator import truediv
from mrjob.job import MRJob
import re
import numpy as np

class MR101(MRJob):

    #start-dfs.sh start-yarn.sh
    # obtener el número de peliculas
    def mapper(self, _,line): 
        field_line = line.split(",")
        puntuacion = field_line[5]
        tipo = field_line[4]
        try:
            float(puntuacion)
        except:
            pass
        else:
            if tipo.find("Horror") > 0 and float(puntuacion) > 7.5:
                yield ('Horror', 1)
            if tipo.find("Thriller") > 0 and float(puntuacion) > 7.5:
                yield ('Thriller', 1)

    def reducer(self, key, values):
        yield (key, sum(values))


if __name__ == '__main__':
    MR101.run()
