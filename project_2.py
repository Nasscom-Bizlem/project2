import requests
import pandas as pd
import json
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import re
import html2text
import threading
import logging
import time
import traceback

#Create and configure logger
#scriptName = sys.argv[0].split("\\")[-1][0:-3]
scriptName = "MD_OFFiCE_ParseData" #the script without the .py extension
dateTimeStamp = time.strftime('%Y%m%d%H') #in the format YYYYMMDDHHMMSS
rootDir = './logs/'
logFile = rootDir + scriptName + "_" + dateTimeStamp + ".log"
logging.basicConfig(filename=logFile, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    filemode='a') 
  
#Creating an object 
logger = logging.getLogger() 
  
#Setting the threshold of logger to DEBUG 
logger.setLevel(logging.INFO) 
  
#Test messages 
logger.info("Harmless debug Message")


URL = 'http://35.188.227.39:8080/enhancer/chain/EssarApproval'
TITLE_STATUS = 'http://fise.iks-project.eu/ontology/entity-label'
TITLE = 'http://www.w3.org/2000/01/rdf-schema#label'

def is_int(s):
    try:
        num = int(s)
        return True
    except ValueError:
        return False

def is_str(s):
    return isinstance(s, str)

def strip(s):
    return ''.join(re.split('[^a-zA-Z]', s.lower()))

# crawl variation of labels
logger.info('Requesting server for labels...')

def request_label(label):
    r = requests.post(URL, data=label, headers={'Content-Type': 'application/pdf'})
    res = r.json()
    for v in res[0][TITLE]:
        labels[strip(v['@value'])] = label


default_labels = [
    'SO',
    'Item',
    'Remark',
]

labels = {}

threads = [ threading.Thread(
    target=request_label, 
    args=(label,)
) for label in default_labels ]

for thread in threads: thread.start()
for thread in threads: thread.join()

labels['status'] = 'Remark'
labels['Status'] = 'Remark'


status_list = ['hold', 'cleared', 'clear']
# extract text

def request_word(word):
    try:
        r = requests.post(URL, data=word, headers={
            'Content-Type': 'application/pdf',
        })

        res = r.json() 
        for v in res:
            if TITLE_STATUS in v:
                status = v[TITLE_STATUS][0]['@value']
                if strip(status) in status_list:
                    return status

        for status in status_list:
            if status in strip(word):
                return status 

        return None

    except Exception as e:
        traceback.print_exc()
        return None


def extract_text(path, verbose=True):
    with open(path, encoding='utf-8') as f:
        soup = BeautifulSoup(f, features='lxml')

    for t in soup.find_all('table'):
        t.extract()

    text = re.split('[^a-zA-Z0-9 ]', soup.text)
    text = [ t for t in text if len(t) > 0 ]

    if verbose: print('requesting in extract text')
    for t in text:
        status = request_word(t)
        if status is not None: 
            if verbose: print('finish requesting in extract text, status', status)
            return status

    if verbose: print('finish requesting in extract text, found no status')
    return None


def process_no_table(path, verbose=True):
    with open(path, encoding='utf-8') as f:
        text = html2text.html2text(f.read())
   
    text = [ t.strip() for t in re.split('[^a-zA-Z0-9 \:\-]', text) if len(t) > 0 ]
    results = []
    
    global_status = { 'value': None }

    def request_text(t, results, global_status):
        status = request_word(t)
        
        words = re.split('[^a-zA-Z0-9]', t)
        
        obj = { 'Remark': status }
        for word in words:
            if is_int(word):
                if len(word) == 10 or (word[0] == '-' and len(word) == 11):
                    obj['SO'] = word
                else: 
                    obj['Item'] = word

        if 'SO' in obj:
            results.append(obj)
        
        if status is not None and global_status['value'] is None:
            global_status['value'] = status


    threads = [ threading.Thread(
        target=request_text, 
        args=(t, results, global_status),
    ) for t in text ]

    if verbose: print('requesting when process no table')
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    if verbose: print('finish requesting when process no table')


    for obj in results:
        if obj['Remark'] is None:
            obj['Remark'] = global_status['value']

    return results

def process_table(table, global_status):
    table.dropna(axis=0, how='all', inplace=True)
    table.dropna(axis=1, how='all', inplace=True)


    height, width = table.shape

    label_so = None

    for col in table.iloc[0]:
        for k, v in labels.items():
            if v == 'SO' and is_str(col) and k == strip(col):
                label_so = col
                break

    if label_so is None:
        for col in table.columns:
            if is_int(table[col].iloc[0]):
                label_so = col
                break
    else:
        table.columns = table.iloc[0]

    results = []
    for i in range(height):
        if is_int(table[label_so].iloc[i]):
            obj = {}
            if is_int(label_so):
                obj['SO'] = int(table[label_so].iloc[i])
                if label_so < width - 1:
                    obj['Item'] = int(table[label_so + 1].iloc[i])
                    obj['Remark'] = global_status
            else:
                for col in table.columns:
                    if is_str(col) and strip(col) in labels:
                        value = table[col].iloc[i]
                        if is_int(value):
                            value = int(value)

                        obj[labels[strip(col)]] = value

                if 'Remark' not in obj:
                    obj['Remark'] = global_status

            results.append(obj)

    return results

# create json file
def p2_process(path, verbose=True):
    tables = []
    status = None
    results = []
    try:
        ext = path.split('.')[-1]

        if ext == 'html':
            tables = pd.read_html(path)
            tables = tables[:1]
            global_status = extract_text(path, verbose=verbose)

        elif ext == 'xls' or ext == 'xlsx':
            xls = pd.ExcelFile(path)
            sheets = xls.book.sheets()

            # read visibble sheet only
            tables = []
            for sheet in sheets:
                if sheet.visibility == 0:
                    table = pd.read_excel(xls, sheet.name, header=None)
                    th, tw = table.shape

                    if th > 0 and tw > 0:
                        tables.append(table)


    except ValueError:
        if verbose: print('No table found')
        tables = []

    results = process_no_table(path, verbose=verbose)
    temp_json = { 'SOITEMS': results }
    
    value_exist = True
    
    for item in temp_json['SOITEMS']:
        if 'Item' in item:
            value_exist = False
            break
    
    if (value_exist):
        results_table = [ process_table(table, global_status) for table in tables ]
        results = []
        for r in results_table:
            results += r

        logger.info('Found Record - %s' % (len(results)))
        if verbose: print('Found', len(results), 'record(s)')
        
        return { 'SOITEMS': results }

    logger.info('Value Check Flag - %s' % (value_exist))
    return temp_json

if __name__ == '__main__':
    r = p2_process('../data/p2materials/p28.html')
    print(json.dumps(r, indent=2))
