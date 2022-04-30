
import pandas as pd
import glob
import os
import csv
dir = '/home/alumno/Documentos/bigdatapracticas/02-Proyecto_despliegue/stocks/historico/'
csvfiles = glob.glob(os.path.join(dir, '*.csv'))
print (csvfiles)

dfs = []

#with os.scandir(dir) as ficheros:
with open('ibexunido.csv', 'a') as f:
    writer = csv.writer(f)
    

    for fichero in csvfiles:
        

        file = fichero
        #print(file)
        #data = file.split(".")
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
            
    
        

    

    # write the header
    #writer.writerow(header)

    # write the data
    
    #newdf = pd.concat(dfs, axis=0, ignore_index=True)
    #newdf.to_csv('ibexunido.csv')
    