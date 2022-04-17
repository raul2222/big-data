from mrjob.job import MRJob
import os

    #start-dfs.sh start-yarn.sh
    # python 2.py -r hadoop hdfs:///user/alumno/imdb/Netflix-Dataset-Rating.csv
    #A partir del fichero imdb-processed.csv, obtener el tıtulo y el ano de las 
    #pelıculas que tambien aparezcan en Netflix-Dataset-Movie.csv, junto con la
    #puntuacion y la cantidad de votos. Si no aparecen, mostrar: tıtulo, ano, NA, NA.
    #Importante: los IDs de los dos ficheros no coinciden, ası
    #que tendr´as que comprobar si coincide el nombre y el ano.

class MRGold(MRJob):

    def mapper(self, _,line): 
        data = line.split(',')   
        file_name = os.environ['map_input_file'].split("/")[-1]
        if file_name == 'imdb-processed.csv' and data[2]:
            try:
                float(data[2])
            except:
                pass
            else:
                title = data[1]
                year = float(data[2])
                try:
                    rating = float(data[5])
                except:
                    rating = "NA"
                try:
                    votes = float(data[6])
                except:
                    votes = "NA"
                
                yield str(year) + " - " + str(title).replace("'",""), (str(rating),str(votes))  

        else:
            if file_name == 'Netflix-Dataset-Movie.csv' and data[1]:
                try:
                    float(data[1])
                except:
                    pass
                else:
                    year_movie = float(data[1])
                    title_movie = data[2]
                    yield str(year_movie) + " - " + str(title_movie).replace("'",""),("-999","-888")


    def reducer(self, title, values):
        
        for rating, votes in values:
            if rating != "-999" and votes != "-888":
                yield title, (rating,votes)
 
        

'''        
        suma = 0
        contry_name = ''
        person = ''
        for tipo, otro in medallas:
            if tipo == 'medal':
                suma += 1
                person = otro
            if tipo == 'c_n':
                contry_name = otro
        
        if suma > 2:
            yield((country,contry_name), person)      
'''

if __name__ == '__main__':
    MRGold.run()