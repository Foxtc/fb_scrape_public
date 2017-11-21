import copy
import csv
import datetime
import json
import socket
import time
import urllib.request

import unicodedata
import json
import pandas as pd

# 60 seconds timeout
socket.setdefaulttimeout(60)

def load_data(data,enc='utf-8'):
    if type(data) is str:
        csv_data = []
        with open(data,'r',encoding = enc,errors = 'replace') as f:
            reader = csv.reader((line.replace('\0','') for line in f)) #remove NULL bytes
            for row in reader:
                if row != []:
                    csv_data.append(row)
        return csv_data
    else:
        return copy.deepcopy(data)

def save_records(filename,data,use_quotes=True,file_mode='w',enc='utf-8'): #this assumes a list of lists wherein the second-level list items contain no commas
    # print("not implemented yet")
    with open(filename,file_mode,encoding = enc) as out:
        for line in data:
            # row = json.load(line)
            row = json.dumps(line, ensure_ascii=False)
            row = row.replace("\n","")
            row += "\n"
            out.write(row)

def url_retry(url):
    succ = 0
    while succ == 0:
        try:
            json_out = json.loads(urllib.request.urlopen(url).read().decode(encoding="utf-8"))
            succ = 1
        except Exception as e:
            print(str(e))
            if 'HTTP Error 4' in str(e):
                return False
            else:
                time.sleep(1)
    return json_out

def optional_field(dict_item,dict_key):
    try:
        out = dict_item[dict_key]
        if dict_key == 'shares':
            out = dict_item[dict_key]['count']
        if dict_key == 'likes':
            out = dict_item[dict_key]['summary']['total_count']
    except KeyError:
        out = ''
    return out