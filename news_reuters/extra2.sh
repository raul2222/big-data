#!/bin/sh
date_now=`date -d "2019-01-01" +%Y-%m-%d`
echo $date_now

startdate=`date -d "2022-04-20" +%Y-%m-%d`  #|| exit -1
enddate=`date -d "2018-12-23" +%Y-%m-%d` #|| exit -1

p="reuters_unido.csv --data "
hasta=$startdate
desde=$startdate
while [ 1 ]; do 

    desde=$(date -I -d "$hasta - 1 weeks")
    param="$desde*$hasta"
    echo $p $param
    python count_words2.py $p $param >> words_despliegue.csv
  
    hasta=$desde
    #sleep 20
done
#last_week=$(date -d "$NOW -1 weeks" +"%Y-%m-%d")

#echo $last_week


#python tres.py -r hadoop hdfs:///user/alumno/ibex35/ibexunido.csv
#python count_words.py reuters_unido.csv



