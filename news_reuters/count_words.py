from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    def mapper(self, _, line):
        # yield each word in the line
        fields = line.split(',') 
        fecha = fields[0]
        for word in WORD_RE.findall(fields[1]):
            yield (word.lower() +"-"+ fecha), 1

 

    def reducer(self, word, counts):
        yield "word", ( sum(counts),word)


    def reducer2(self, key, values):
        self.alist = []
        
        for value in values:
            self.alist.append(value)
        
        self.blist = []
        for i in range(100):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
        
        for i in range(100):
            yield self.blist[i]


    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.reducer2)
        ]


if __name__ == '__main__':
    MRMostUsedWord.run()