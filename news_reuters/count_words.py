from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    def configure_args(self):
        super(MRMostUsedWord, self).configure_args()
        self.add_passthru_arg('--data', default='2018-11-12 2018-12-31', help="please enter dates")

    def mapper(self, _, line):
        # yield each word in the line
        datos = self.options.data.split(" ")
        fecha_inicio = datetime.strptime(datos[0],"%Y-%m-%d") 
        fecha_fin = datetime.strptime(datos[1],"%Y-%m-%d")
        fields = line.split(',') 
        fecha = fecha = datetime.strptime(fields[0],"%Y-%m-%d")
        if fecha >= fecha_inicio and fecha <= fecha_fin: 
            for word in WORD_RE.findall(fields[1]):
                if len(word) > 4:
                    yield word, 1

 
    def reducer(self, word, counts):
        yield "word", ( sum(counts),word)


    def reducer2(self, key, values):
        self.alist = []
        
        for value in values:
            self.alist.append(value)
        
        self.blist = []
        for i in range(20):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
        
        for i in range(20):
            yield self.blist[i]

    


    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.reducer2)
        ]


if __name__ == '__main__':
    MRMostUsedWord.run()