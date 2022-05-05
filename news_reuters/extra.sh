#!/bin/sh


input_start=2022-04-20
input_end=2019-01-01
#python count_words.py reuters_unido.csv

startdate=$(date -I -d "$input_start") || exit -1
enddate=$(date -I -d "$input_end")     || exit -1
p="IBEX.csv --data "
hasta=$startdate
desde=$startdate
while [ $hasta < $enddate ]; do 

    desde=$(date -I -d "$hasta - 1 weeks")
    
    param="$desde*$hasta"
    
    echo $p $param
    python tres.py $p $param

    hasta=$desde
    
    sleep 2
done
#last_week=$(date -d "$NOW -1 weeks" +"%Y-%m-%d")

#echo $last_week


#python tres.py -r hadoop hdfs:///user/alumno/ibex35/ibexunido.csv
#python count_words.py reuters_unido.csv



