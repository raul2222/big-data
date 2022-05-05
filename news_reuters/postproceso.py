import csv
import time
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import gensim
#from gensim.parsing.preprocessing import STOPWORDS
all_stopwords_gensim = gensim.parsing.preprocessing.STOPWORDS

import re


def untokenize2(words):

    text = words
    step1 = text.lower()
    step1 = step1.replace(",", ' ').replace('"', '')
    step1 = step1.replace("united", 'unitedstates').replace(" would ",  ' ').replace(" states",  ' ')
    step1 = step1.replace("( ", '').replace(') ', '').replace("$ ",  '$').replace(" , ",  '')
    step1 = step1.replace(", ", ' ').replace('"', ' ').replace("'s ",  ' ').replace(". ",  ' ')
    #step1 = step1.replace(":", ' ').replace("”", ' ').replace("’",  ' ').replace("said",  ' ')
    #step1 = step1.replace("said ", '').replace("monday ", '').replace("’",  '').replace("said ",  '')
    #step1 = step1.replace("friday", '').replace("sunday ", '').replace("tuesday ",  '').replace("wednesday",  '')
    #step1 = step1.replace("year", '').replace("thursday ", '').replace("saturday ",  '').replace("said",  '')
    #step1 = step1.replace("it ", ' ').replace("in ", ' ').replace("told ",  ' ')
    #step1 = step1.replace(" s ", '').replace("including ", '')
    #step1 = step1.replace("united states ",  'united-states ')
    #step1 = step1.replace(" international ", '').replace(" group ", '').replace("time ",  '')
    #step1 = step1.replace(" global ", '').replace(" people ", '').replace("world ",  '')
    step1 = step1.replace("? ", '').replace(" said ", ' ').replace(" it ",  ' ')
    step1 = step1.replace(" in ", ' ').replace(" said ", ' ').replace(" it ",  ' ')
    step1 = step1.replace(" for ", ' ').replace(" a ", ' ').replace(" of ",  ' ')
    step1 = step1.replace(" and ", ' ').replace(" the ", ' ').replace(" on ",  ' ')
    step1 = step1.replace(" at ", ' ').replace(" to ", ' ').replace("by ",  ' ')
    step1 = step1.replace(" - ", '').replace('  ', ' ').replace("   ",  ' ')
    step1 = step1.replace(" have ", ' ').replace(" that ", ' ').replace(" will ",  ' ')
    step1 = step1.replace(" could ", ' ').replace(" there ", ' ').replace(" their ",  ' ')
    step1 = step1.replace(" which ", ' ').replace(" from ", ' ').replace(" will ",  ' ')
    step1 = step1.replace(" country ", ' ').replace(" after ", ' ').replace(" against ",  ' ')
    step1 = step1.replace(" since ", ' ').replace(" other ", ' ').replace(" world ",  ' ')
    step1 = step1.replace(" people ", ' ').replace(" about ", ' ').replace(" state ",  ' ')
    step1 = step1.replace(" while ", ' ').replace(" global ", ' ').replace(" march ",  ' ')
    step1 = step1.replace(" million ", ' ').replace(" billion ", ' ').replace(" month ",  ' ')
    step1 = step1.replace(" first ", ' ').replace(" billion ", ' ').replace(" month ",  ' ')
    step1 = step1.replace(" friday ", ' ').replace(" bucha ", ' ').replace(" international ",  ' ')
    step1 = step1.replace(" reuters ", ' ').replace(" prices ", ' ').replace(" minister ",  ' ')
    step1 = step1.replace(" thursday ", ' ').replace(" ministry ", ' ').replace(" monday ",  ' ')
    step1 = step1.replace(" tuesday ", ' ').replace(" sunday ", ' ').replace(" including ",  ' ')
    #.replace("'",  '')
    step1 = step1.replace(" wednesday ", ' ').replace(" talks ", ' ').replace(" countries ",  ' ')
    step1 = step1.replace(" president ", ' ').replace(" government ", ' ').replace(" statement ",  ' ')
    step1 = step1.replace(" three ", ' ').replace(" group ", ' ').replace(" years ",  ' ')
    step1 = step1.replace(" between ", ' ').replace(" according ", ' ').replace(" years ",  ' ')
    step1 = step1.replace(" company ", ' ').replace(" under ", ' ').replace(" around ",  ' ')
    step1 = step1.replace("  ", ' ').replace('  ', ' ').replace("     ",  ' ').replace("'",  '')
    step1 = step1.replace(" companies ", ' ').replace(" during ", ' ').replace(" before ",  ' ')
    step1 = step1.replace(" european ", ' ').replace(" russian ", ' ').replace(" ukrainian ",  ' ')
    step1 = step1.replace(" cases ", ' ').replace(" where ", ' ').replace(" because ",  ' ')
    step1 = step1.replace(" based ", ' ').replace(" those ", ' ').replace(" comment ",  ' ')
    step1 = step1.replace(" reported ", ' ').replace(" april ", ' ').replace(" public ",  ' ')
    step1 = step1.replace(" economic ", ' ').replace(" second ", ' ').replace(" still ",  ' ')

    step1 = step1.replace(",", ' ')
    #step6 = step5.replace(" ` ", " '")
    return step1.strip()

with open('reuters_unido.csv', 'a') as f:
    writer = csv.writer(f)

    new_together = ""
    date_ant =""
    with open("reutersnews_2022.csv", "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for lines in csv_reader:
            date = lines[0]
            if date_ant == "": date_ant = date
            if date_ant == date: #acumula
                new_together = new_together + lines[2]
                
            else:
                #text = word_tokenize(new_together)
                #text_ok = [word for word in text if not word in all_stopwords]
                #untokenize(text_ok)
                #text_ok2  = [word for word in text_ok if not word in all_stopwords_gensim]
                #row = [date,untokenize(text_ok2)]
                #print(new_together)
                text = untokenize2(new_together)
                text = word_tokenize(text)
                text =   [word for word in text if not word in all_stopwords_gensim]
                text = ' '.join(text)
                text = untokenize2(new_together)
                text = text.replace('"','')
                text = text.replace(',','')
                text = text.replace('\n','')
                print(date)
                #print(text)
                row = [date,text]
                writer.writerow(row)
                #time.sleep(80)
                new_together = ""
                date_ant = ""