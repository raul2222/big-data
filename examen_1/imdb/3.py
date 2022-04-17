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
                # replace("'","") for better coincidence
                yeartitle = str(year) + " - " + str(title).replace("'","")
                yield yeartitle, (yeartitle, str(rating)+"-"+str(votes)) 

        else:
            if file_name == 'Netflix-Dataset-Movie.csv' and data[1]:
                try:
                    float(data[1])
                except:
                    pass
                else:
                    year_movie = float(data[1])
                    title_movie = data[2]
                    yeartitle_netflix =str(year_movie) + " - " + str(title_movie).replace("'","")
                    yield yeartitle_netflix,(yeartitle_netflix,"netflix")


    def reducer(self, title, values):
        title_ = ""
        ratings_ = ""
        count = 0

        for name, rating in values:
            if rating != "netflix":
                title_ = name
                ratings_ = rating
            if rating == "netflix" and name == title_:
                count+=1
        if count > 0:
            yield title_,(ratings_, count)


if __name__ == '__main__':
    MRGold.run()