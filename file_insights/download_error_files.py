"Script to download error files for In_Network folder and create file to npi_id mapping for each path"
"just a draft, use with caution"

import os
import json
import csv
from multiprocessing import Process
import requests
import pandas as pd


def get_files(f1, f2):
    with open(f1, 'r') as fp:
        json_files = fp.readlines()
    with open(f2, 'r') as fp:
        error_files = fp.readlines()
    return json_files, error_files

def parse_files():
    json_files, error_files = get_files('json_list.txt', 'Error_files.csv')
    json_list = ['index_json/' + f.strip() for f in json_files]
    error_list = [f.strip() for f in error_files]
    return json_list, error_list

def create_json_data(json_list):
    res_data = {}
    for rfile in json_list:
        with open(rfile, 'r') as fp:
            try:     
                data = pd.read_json(rfile)
            except IOError as e:
                print('not able to open file: %s'%(rfile))
                data = []
            for chunk in data:
                print(chunk)
            if len(data) !=  0:
                plan_data = data['reporting_structure'][0]['reporting_plans']
                file_data = data['reporting_structure'][0]['in_network_files']
                plan_id = [f['plan_id'] for f in plan_data]
                for data in file_data:
                    res_data.update({data['location'] : {'plan_id': plan_id, 'file': rfile}})
            else: 
                pass
        yield res_data

def get_file_path(json_list):
    res_data = {}
    for rfile in json_list:
        with open(rfile, 'r') as fp:
            try:
                data = json.loads(fp.read())
            except:
                data = []
            if len(data) !=  0:
                file_data = data['reporting_structure'][0]['in_network_files']
                for data in file_data:
                    res_data.update({data['location'].split('/')[4].split('=')[2] : data['location']})
            else:
                pass
        yield res_data

             
            

def create_file(json_list):
    count = 0
    for chunk in create_json_data(json_list):
        count += 1
        with open('converted/json_%s.csv'%(count), 'w') as fp:
            filewriter = csv.writer(fp, delimiter=',')
            for path, details in chunk.items():
                filewriter.writerow([path, details['plan_id'], details['file']])


def download_url(url, save_path, chunk_size=10):
        """
                function for downloading the file
        """
        with open(save_path, 'wb') as fd:
            fd.write(requests.get(url, stream=True).content)


if __name__ == '__main__':
    json_list, error_files = parse_files()
    path = os.getcwd()
    print(create_json_data(['index_json/2022-07-24_ZWIL-INC_index.json']))
    exit()
    
    for efile in error_files:
        try:
            download_url("https://prod-developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=%s"%(efile), '/error_files',10)
            print('======file downloaded: %s'%(efile))
        except:
            print('file %s not found'%(efile))
                
        


