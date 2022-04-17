from mrjob.job import MRJob
import re

class MR100(MRJob):

    #start-dfs.sh start-yarn.sh
    # python 2.py -r hadoop hdfs:///user/alumno/imdb/Netflix-Dataset-Rating.csv


    def mapper(self, _,line): 
        field_line = line.split(",")
        try:
            float(field_line[1])
        except:
            pass
        else:
            #revisor = field_line[0]
            #rating = float(field_line[1])
            yield field_line[0], float(field_line[1])

    def reducer(self, revisor, rating):
        count = 0
        total = 0
        for i in rating:
            count += 1
            total += i
        if count > 10:
            yield (revisor, total/count)

if __name__ == '__main__':
    MR100.run()
