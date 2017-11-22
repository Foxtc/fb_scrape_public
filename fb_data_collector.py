'''
This script can download posts and comments from public Facebook pages and groups. It requires Python 3.

INSTRUCTIONS

1.    This script is written for Python 3 and won't work with previous Python versions.
2.    The main function in this module is scrape_fb (see lines 107-109). It is the only function most users will need to run directly.
3.    You need to create your own Facebook app, which you can do here: https://developers.facebook.com/apps . Doesn't matter what you call your new app, you just need to pull its unique client ID (app ID) and app secret.
4.    Once you create your app, you can insert the client ID and secret AS STRINGS into the appropriate scrape_fb fields. 
5.    This function accepts text FB user IDs ('barackobama'), numerical user IDs, and post IDs. You can load them into the ids field using a comma-delimited string or by creating a plain text file in the same folder as the script containing one or more names of the Facebook pages you want to scrape, one ID per line (this file MUST have a .csv or .txt extension). For example, if you wanted to scrape Barack Obama's official FB page (http://facebook.com/barackobama/) using the text file method, your first line would simply be 'barackobama' without quotes. I suggest starting with only one ID to make sure it works. You'll only be able to collect data from public pages.
6.    The only required fields for the scrape_fb function are client_id, client_secret, and ids. I recommend not changing the other defaults (except for maybe outfile) unless you know what you're doing.
7.    If you did everything correctly, the command line should show you some informative status messages. Eventually it will save a CSV full of data to the same folder where this script was run. If something went wrong, you'll see an error.
'''

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

import utils

# 60 seconds timeout
socket.setdefaulttimeout(60)

class FacebookAuthenticator(object):
    
    def __init__(self,client_id,client_secret,version="2.10"):
        self.client_secret = client_secret
        self.client_id = client_id
        self.version = version

    def request_access_token(self):
        
        if type(self.client_id) is int:
            self.client_id = str(self.client_id)

        fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + self.client_id + '&client_secret=' + self.client_secret)
        
        fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
        return fb_token

class FacebookDataCollector(object):

    def __init__(self,access_token,version="2.10"):
        self.access_token = access_token


class FacebookPostsCollector(FacebookDataCollector):

    def __init__(self,fb_token,version="2.10",scrape_mode="feed", output_file=None):
        self.fb_token = fb_token
        self.scrape_mode = scrape_mode
        self.version = version
        self.output_file = output_file

    def collect(self,fb_object_id,end_date="",max_rows=0):
        
        time1 = time.time()

        self.fb_object_id = fb_object_id
        self.end_date = end_date

        if type(self.fb_object_id) is int:
            self.fb_object_id = str(self.fb_object_id)

        assert fb_object_id is not None
        assert type(fb_object_id) is str

        self.fb_object_id = self.fb_object_id.strip()
        
        try:
            self.end_dateobj = datetime.datetime.strptime(self.end_date, "%Y-%m-%d").date()
        except ValueError:
            self.end_dateobj = ''

        msg_user = ''
        msg_content = ''
        field_list = 'from,message,picture,link,name,description,type,created_time,shares,likes.summary(total_count).limit(0)'
            
        data_url = 'https://graph.facebook.com/v' + self.version + '/' + self.fb_object_id + '/' + self.scrape_mode + '?fields=' + field_list + '&limit=100&' + self.fb_token
        
        data_rxns = []
        new_rxns = ['LOVE','WOW','HAHA','SAD','ANGRY']
        for i in new_rxns:
            data_rxns.append('https://graph.facebook.com/v' + self.version + '/' + self.fb_object_id + '/' + self.scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + self.fb_token)
        
        
        # header = ['from','from_id','message','picture','link','name','description','type','created_time','shares','likes','loves','wows','hahas','sads','angrys','post_id']

        # if(self.output_file != None):
        #     with open(self.output_file, 'w') as f:
        #         f.write(','.join(header) + "\n")

        records = []

        next_item = utils.url_retry(data_url)
        

        if next_item != False:
            for n,i in enumerate(data_rxns):
                tmp_data = utils.url_retry(i)
                for z,j in enumerate(next_item['data']):
                    try:
                        j[new_rxns[n]] = tmp_data['data'][z]['reactions']['summary']['total_count']
                    except (KeyError,IndexError):
                        j[new_rxns[n]] = 0
            
            # records += [fb_json_page['data'] for fb_json_page in next_item]
            if(self.output_file != None):
                records = next_item['data']
                print("persisting data ... ",self.output_file)

                with open(self.output_file, 'a') as f:
                    for row in records:
                        f.write(json.dumps(row) + "\n")
            else:
                records += next_item['data']

            if(max_rows > 0):
                if(max_rows >= len(records)):
                    return records
                # print(line)

        else:
            print("Skipping ID " + self.fb_object_id + " ...")
            # continue
        n = 0
        
        while 'paging' in next_item and 'next' in next_item['paging']:
            next_item = utils.url_retry(next_item['paging']['next'])
            try:
                for i in new_rxns:
                    start = next_item['paging']['next'].find("from")
                    end = next_item['paging']['next'].find("&",start)
                    next_rxn_url = next_item['paging']['next'][:start] + 'reactions.type(' + i + ').summary(total_count).limit(0)' + next_item['paging']['next'][end:]
                    tmp_data = utils.url_retry(next_rxn_url)
                    for z,j in enumerate(next_item['data']):
                        try:
                            j[i] = tmp_data['data'][z]['reactions']['summary']['total_count']
                        except (KeyError,IndexError):
                            j[i] = 0
            except KeyError:
                continue
            
            # records += [fb_json_page['data'] for fb_json_page in next_item]
            

            if(self.output_file != None):
                records = next_item['data']
                
                with open(self.output_file, 'a') as f:
                    for row in records:
                        f.write(json.dumps(row) + "\n")
            else:
                records += next_item['data']
            
            print("number of records",len(records))

            if(max_rows > 0):
                if(max_rows >= len(records)):
                    return records

            try:
                print(n+1,"page(s) of data archived for ID",self.fb_object_id,"at",next_item['data'][-1]['created_time'],".",round(time.time()-time1,2),'seconds elapsed.')
            except IndexError:
                break
            n += 1
            time.sleep(1)
            if self.end_dateobj != '' and self.end_dateobj > datetime.datetime.strptime(next_item['data'][-1]['created_time'][:10],"%Y-%m-%d").date():
                break
            
        print(self.fb_object_id,'page archived.',round(time.time()-time1,2),'seconds elapsed.')

        print('Script completed in',time.time()-time1,'seconds.')
        return records

class FacebookCommentsCollector(FacebookDataCollector):

    scrape_mode = "comments"

    def __init__(self,fb_token,version="2.10"):
        self.fb_token = fb_token        
        self.version = version

    def collect(self,fb_object_id ,max_rows=0):
        time1 = time.time()

        self.fb_object_id = fb_object_id
        
        if type(self.fb_object_id) is int:
            self.fb_object_id = str(self.fb_object_id)

        assert fb_object_id is not None
        assert type(fb_object_id) is str

        self.fb_object_id = self.fb_object_id.strip()

        header = ['from','from_id','comment','created_time','likes','post_id','original_poster']

        msg_url = 'https://graph.facebook.com/v' + self.version + '/' + fb_object_id + '?fields=from,message&' + self.fb_token
        
        msg_json = utils.url_retry(msg_url)
        if msg_json == False:
            print("URL not available. Continuing...", fb_object_id)
        
        msg_user = msg_json['from']['name']
        msg_content = utils.optional_field(msg_json,'message')
        field_list = 'from,message,created_time,like_count'

        data_url = 'https://graph.facebook.com/v' + self.version + '/' + self.fb_object_id + '/' + self.scrape_mode + '?fields=' + field_list + '&limit=100&' + self.fb_token
        
        print(data_url)
        data_rxns = []
        new_rxns = ['LOVE','WOW','HAHA','SAD','ANGRY']
        for i in new_rxns:
            data_rxns.append('https://graph.facebook.com/v' + self.version + '/' + self.fb_object_id + '/' + self.scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + self.fb_token)
        
        
        records = []

        next_item = utils.url_retry(data_url)
        

        if next_item != False:
            for n,i in enumerate(data_rxns):
                tmp_data = utils.url_retry(i)
                for z,j in enumerate(next_item['data']):
                    try:
                        j[new_rxns[n]] = tmp_data['data'][z]['reactions']['summary']['total_count']
                    except (KeyError,IndexError):
                        j[new_rxns[n]] = 0
            
            # print(type(next_item.keys()))
            # records += [fb_json_page['data'] for fb_json_page in next_item]
            records += next_item['data']
            print("number of records",len(records))

            if(max_rows > 0):
                if(max_rows >= len(records)):
                    return records
                # print(line)


        else:
            print("Skipping ID " + fid + " ...")
            # continue
        n = 0
        
        
        while 'paging' in next_item and 'next' in next_item['paging']:
            next_item = utils.url_retry(next_item['paging']['next'])
            try:
                for i in new_rxns:
                    start = next_item['paging']['next'].find("from")
                    end = next_item['paging']['next'].find("&",start)
                    next_rxn_url = next_item['paging']['next'][:start] + 'reactions.type(' + i + ').summary(total_count).limit(0)' + next_item['paging']['next'][end:]
                    tmp_data = utils.url_retry(next_rxn_url)
                    for z,j in enumerate(next_item['data']):
                        try:
                            j[i] = tmp_data['data'][z]['reactions']['summary']['total_count']
                        except (KeyError,IndexError):
                            j[i] = 0
            except KeyError:
                continue
            
            # records += [fb_json_page['data'] for fb_json_page in next_item]
            records += next_item['data']
            
            print("number of records",len(records))

            if(max_rows > 0):
                if(max_rows >= len(records)):
                    return records

            try:
                print(n+1,"page(s) of data archived for ID",self.fb_object_id,"at",next_item['data'][-1]['created_time'],".",round(time.time()-time1,2),'seconds elapsed.')
            except IndexError:
                break
            n += 1
            time.sleep(1)
            # if self.end_dateobj != '' and self.end_dateobj > datetime.datetime.strptime(next_item['data'][-1]['created_time'][:10],"%Y-%m-%d").date():
            #     break
            
        print(x+1,'Facebook ID(s) archived.',round(time.time()-time1,2),'seconds elapsed.')

        print('Script completed in',time.time()-time1,'seconds.')
        print("done")
        return records


class FacebookReactionsCollector(FacebookDataCollector):
    def __init__(self,fb_token,version="2.10",scrape_mode=""):
        self.fb_token = fb_token
        self.scrape_mode = scrape_mode
        self.version = version

    def collect(self,fb_object_id,end_date="",max_rows=0):
        
        time1 = time.time()

        self.fb_object_id = fb_object_id
        self.end_date = end_date

        if type(self.fb_object_id) is int:
            self.fb_object_id = str(self.fb_object_id)

        assert fb_object_id is not None
        assert type(fb_object_id) is str

        self.fb_object_id = self.fb_object_id.strip()
        
        try:
            self.end_dateobj = datetime.datetime.strptime(self.end_date, "%Y-%m-%d").date()
        except ValueError:
            self.end_dateobj = ''

        msg_user = ''
        msg_content = ''
        field_list = 'reactions'
            
        data_url = 'https://graph.facebook.com/v' + self.version + '/' + self.fb_object_id + '/' + self.scrape_mode + '?fields=' + field_list + '&limit=100&' + self.fb_token
        
        # data_rxns = []
        # new_rxns = ['LOVE','WOW','HAHA','SAD','ANGRY']
        # for i in new_rxns:
        #     data_rxns.append('https://graph.facebook.com/v' + self.version + '/' + self.fb_object_id + '/' + self.scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + self.fb_token)
        
        
        records = []
        print(data_url)
        next_item = utils.url_retry(data_url)
        print(next_item)
        
        if next_item != False:
            # for n,i in enumerate(data_rxns):
            #     tmp_data = utils.url_retry(i)
            #     for z,j in enumerate(next_item['data']):
            #         try:
            #             j[new_rxns[n]] = tmp_data['data'][z]['reactions']['summary']['total_count']
            #         except (KeyError,IndexError):
            #             j[new_rxns[n]] = 0
            
            # print(type(next_item.keys()))
            # records += [fb_json_page['data'] for fb_json_page in next_item]
            records += next_item['data']
            print("number of records",len(records))

            if(max_rows > 0):
                if(max_rows >= len(records)):
                    return records
                # print(line)


        else:
            print("Skipping ID " + self.fb_object_id + " ...")
            # continue
        n = 0
        
        print(records)
        while 'paging' in next_item and 'next' in next_item['paging']:
            next_item = utils.url_retry(next_item['paging']['next'])
            # try:
                # for i in new_rxns:
                    # start = next_item['paging']['next'].find("from")
                    # end = next_item['paging']['next'].find("&",start)

                    # next_rxn_url = next_item['paging']['next'][:start] + 'reactions.type(' + i + ').summary(total_count).limit(0)' + next_item['paging']['next'][end:]
                    # tmp_data = utils.url_retry(next_rxn_url)
                    # for z,j in enumerate(next_item['data']):
                    #     try:
                    #         j[i] = tmp_data['data'][z]['reactions']['summary']['total_count']
                    #     except (KeyError,IndexError):
                    #         j[i] = 0
            # except KeyError:
                # continue
            
            # records += [fb_json_page['data'] for fb_json_page in next_item]
            records += next_item['data']
            
            print("number of records",len(records))

            if(max_rows > 0):
                if(max_rows >= len(records)):
                    return records

            try:
                print(n+1,"page(s) of data archived for ID",self.fb_object_id,"at",next_item['data'][-1]['created_time'],".",round(time.time()-time1,2),'seconds elapsed.')
            except IndexError:
                break
            n += 1
            time.sleep(1)

            # if self.end_dateobj != '' and self.end_dateobj > datetime.datetime.strptime(next_item['data'][-1]['created_time'][:10],"%Y-%m-%d").date():
            #     break
            
        print(x+1,'Facebook ID(s) archived.',round(time.time()-time1,2),'seconds elapsed.')

        print('Script completed in',time.time()-time1,'seconds.')
        print("done")
        return records
    
