import csv
import time
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import gensim
#from gensim.parsing.preprocessing import STOPWORDS


all_stopwords = stopwords.words('english')
all_stopwords.append('"') 
all_stopwords.append("'s")
all_stopwords.append(',')
all_stopwords.append('.')
all_stopwords.append("'s")
all_stopwords.append("''")
all_stopwords.append("``")
all_stopwords.append("%")
all_stopwords.append("(")
all_stopwords.append(")")
all_stopwords.append("The")
all_stopwords.append("the")

all_stopwords_gensim = gensim.parsing.preprocessing.STOPWORDS


import re
def untokenize(words):

    text = ' '.join(words)
    text = text.lower()
    step1 = text.replace("`` ", '').replace(" ''", '').replace('...',  '')
    step1 = step1.replace(".", '').replace(",", '').replace("'s",  '')
    step1 = step1.replace("%", '').replace("'re'", '').replace("'s",  '')
    step1 = step1.replace("“", '').replace("'", '').replace("(",  '').replace(")",  '')
    step1 = step1.replace("-", '').replace("'", '').replace("(",  '').replace(")",  '')
    step1 = step1.replace("A", '').replace("It", '').replace("I",  '').replace("In",  '')
    step1 = step1.replace(":", '').replace("”", '').replace("’",  '').replace("said",  '')
    step1 = step1.replace("said", '').replace("monday", '').replace("’",  '').replace("said",  '')
    step1 = step1.replace("friday", '').replace("sunday", '').replace("tuesday",  '').replace("wednesday",  '')
    step1 = step1.replace("year", '').replace("thursday", '').replace("saturday",  '').replace("said",  '')
    #step6 = step5.replace(" ` ", " '")
    return step1.strip()

with open('reuters_2021.csv', 'a') as f:
    writer = csv.writer(f)

    new_together = ""
    date_ant =""
    with open("reutersnews_2021.csv", "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for lines in csv_reader:
            date = lines[0]
            if date_ant == "": date_ant = date
 
            if date_ant == date: #acumula
                #print("true")
                new_together = new_together + lines[2]
                
            else:
                #print(date +","+ new_together)
                text = word_tokenize(new_together)
                text_ok = [word for word in text if not word in all_stopwords]
                untokenize(text_ok)
                text_ok2  = [word for word in text_ok if not word in all_stopwords_gensim]
                row = [date,untokenize(text_ok2)]
                writer.writerow(row)
                print(date)
                new_together = ""
                date_ant = ""