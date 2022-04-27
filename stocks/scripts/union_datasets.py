
import pandas as pd
import glob
import os
import csv
dir = '/home/alumno/Documentos/bigdatapracticas/02-Proyecto_despliegue/stocks/historico/'
csvfiles = glob.glob(os.path.join(dir, '*.csv'))
print (csvfiles)


with open('ibexunido.csv', 'a') as f:
    writer = csv.writer(f)
    
    for fichero in csvfiles:
        
        file = fichero
        name = file.split('/')[-1].split('.')[0]
        #print (name)
        df = pd.read_csv(fichero, index_col=None, header=0)

        df.insert(loc=7, column=name, value=name)
        
        df.to_csv(fichero, header=False, index=False)

        #df2 = pd.read_csv(fichero, index_col=None, header=0)
        with open(fichero, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                print(row)
                writer.writerow(row)
                #dfs.append(row)
            
    