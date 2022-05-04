#!/bin/sh
python tres.py -r hadoop hdfs:///user/alumno/ibex35/ibexunido.csv
python count_words.py reuters_unido.csv


'''
list_of_arguments = sys.argv


accion = list_of_arguments[0]
inicio = list_of_arguments[1]
final = list_of_arguments[2]

stream = open("tres.py -r hadoop hdfs:///user/alumno/ibex35/ibexunido.csv")
read_file = stream.read()
exec(read_file)
'''