import csv
import time

with open('informe_final.csv', 'a') as f:
    writer = csv.writer(f)

    with open("ibex_despliegue.csv", "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for lines in csv_reader:
            date = lines[1]
            value = lines[2]
            incre = lines[3]
            decre = lines[4]
            res = []
            res.append(date)
            res.append(value)
            res.append(incre)
            res.append(decre)
            with open("words_despliegue.csv", "r") as csv_file2:
                csv_reader2 = csv.reader(csv_file2, delimiter=',')
                
                for lines2 in csv_reader2:
                    if lines2[0] == date:

                        word = lines2[1] + "-" + lines2[2]
                        res.append(word)
                
                #print(res)
                writer.writerow(res)