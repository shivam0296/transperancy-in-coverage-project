import gc
import gzip
import os
import json
import sys

import numpy as np
import time
import pandas as pd
import psutil
from unzip_in_network_provider_reference import get_ids_list, form_provider_reference_to_csv, \
    parse_provider_reference_main, sub_process_bar

'''
The program will save splitted files into multiple small .npy files (in-network list) 
and a 'meta.npy' file which contains other information, such as provider_reference)

In default, the program will create a sub foldeer with name of the gz file, and save all splitted files in it.

'''


#################################################
def parsing_nego_arrange(save_path, subfile_name, sub_file=None):
    if sub_file is None:
        sub_file = np.load(save_path + subfile_name, allow_pickle=True).item()
    print(sub_file.keys())
    # dict_keys(['negotiation_arrangement', 'name', 'billing_code_type', 'billing_code_type_version', 'billing_code', 'description', 'negotiated_rates'])
    for k in list(sub_file.keys()):
        if (type(sub_file[k]) is list) or (type(sub_file[k]) is dict):
            print(k, len(sub_file[k]))
        else:
            # print(k, type(sub_file[k]))
            pass
        '''
    # negotiation_arrangement <class 'str'>
    # name <class 'str'>
    # billing_code_type <class 'str'>
    # billing_code_type_version <class 'str'>
    # billing_code <class 'str'>
    # description <class 'str'>
    # negotiated_rates 32760
    '''
    nr = sub_file['negotiated_rates']
    print('number of negotiate_rate items:', len(nr))
    # print(nr[0].keys())
    # dict_keys(['provider_references', 'negotiated_prices'])
    # after check, each of sub_file['negotiated_rates'][x]['negoriated_prices'] contains only one dicttionary (for x in 1,..., 32760)
    return sub_file


def convert_negotiated_arrangement_npy_to_csv(subfile_i, save_path):
    billing_code = subfile_i['billing_code']
    nr_table = pd.DataFrame(columns=['provider_reference', 'negotiated_prices', 'billing_class'])
    for j in range(len(subfile_i['negotiated_rates'])):
        for k in range(len(subfile_i['negotiated_rates'][j]['negotiated_prices'])):
            nr_table.loc[len(nr_table)] = [subfile_i['negotiated_rates'][j]['provider_references'][0],
                                           subfile_i['negotiated_rates'][j]['negotiated_prices'][k]['negotiated_rate'],
                                           subfile_i['negotiated_rates'][j]['negotiated_prices'][k]['billing_class']]
    nr_table['billing_code'] = billing_code
    nr_table.to_csv(save_path + '{}.csv'.format(billing_code), index=False)


########################################
# parsing meta information
def parse_meta_info(save_path):
    with open(save_path + 'meta.txt', 'r') as f:
        line = f.readlines()
    meta_text = ''
    for j in range(len(line)):
        if line[j].startswith('"provider_references'):
            provider_reference_i = j
            break
        else:
            meta_text += line[j]
    provider_reference_line = line[provider_reference_i]
    provider_reference_dict = json.loads(provider_reference_line[23:-2])  # first 23 chars: "provider_references":
    print(len(provider_reference_dict))
    # 66367
    # provider_reference_dict[0] = {'provider_groups': [{'npi': [1841361052], 'tin': {'type': 'ein', 'value': '582218877'}}], 'provider_group_id': 0}
    meta_dict = json.loads(meta_text[:-3] + '}')
    meta_dict['provider_reference'] = provider_reference_dict
    return meta_dict


def sub_provider_groups_table(pg):
    # pg_table = pd.DataFrame(columns=['npi', 'tin.type', 'tin.value'])
    pg_table = {'npi': [], 'ein.value': []}
    for pgi in pg:
        for npi_i in range(len(pgi['npi'])):
            pg_table['npi'].append(pgi['npi'][npi_i])
            pg_table['ein.value'].append(pgi['tin']['value'])
    return pg_table


def parse_provider_reference(save_path):
    meta_dict = parse_meta_info(save_path)
    pr_df = {'npi': [], 'ein.value': [], 'provider_group_id': []}
    for mi in meta_dict['provider_reference']:
        pg_table_i = sub_provider_groups_table(mi['provider_groups'])
        pg_table_i['provider_group_id'] = [mi['provider_group_id'] for j in range(len(pg_table_i['npi']))]
        pr_df['npi'] += pg_table_i['npi']
        pr_df['ein.value'] += pg_table_i['ein.value']
        pr_df['provider_group_id'] += pg_table_i['provider_group_id']
    pr_df = pd.DataFrame(pr_df)
    pr_df.to_csv(save_path + 'provider_reference_df.csv', index=None)


#############################################
def parse_data(d, insurer):
    '''
    from index.py, credit by Pratheek
    '''
    meta = {}
    meta["billing_code"] = d.get("billing_code")
    meta["billing_code_type"] = d.get("billing_code_type")
    meta["billing_code_type_version"] = d.get("billing_code_type_version")
    meta["name"] = d.get("name")

    n_rates = d.get("negotiated_rates")
    output = []
    if insurer == 'cigna':
        assert ('provider_groups' in n_rates[0]) and type(n_rates[0]['provider_groups'][0]) is dict
        # Cigna format
        for item in n_rates:
            for item2 in item['provider_groups']:
                tin_type = [k for k in item2 if k != 'tin'][0]
                item2['tin_type'] = tin_type
                item2['tin_value'] = '/'.join([str(s) for s in item2[tin_type]])
                del item2['npi']
                item2[item2['tin']['type']] = item2['tin']['value']
                del item2['tin']
                item.update(item2)
                del item['provider_groups']
                output.append(item)
    else:
        for item in n_rates:
            item.update(meta)
            item.update(item["negotiated_prices"][0])
            del item["negotiated_prices"]
            output.append(item)

    return output


#################################################
def split_large_json_gz_v3(data_path, file, save_path, billing_code_list=None, insurer=None):
    """
    :param data_path: where the .json.gz file locate
    :param file: the name of .json.gz file
    :param save_path: where the splitted files should be stored
    :param billing_code_list: ['0001', '0002M',....], None for all
    allow specific billing code
    """
    ts = time.time()
    if not save_path.endswith('/'):
        save_path += '/'
    try:
        os.makedirs(save_path)
    except:
        pass
    print('save_path:', save_path)
    if billing_code_list == None:
        flag = 0
    else:
        assert type(billing_code_list) is list
        flag = 1
        billing_code_list = [str(b) for b in billing_code_list]
    with gzip.open(data_path + file, 'rb') as f:
        i, j = 0, 0
        for line in f:
            row_str = line.decode("utf-8")
            if row_str.startswith('\t{\"negotiation_arrangement') or row_str.startswith('{\"negotiation_arrangement'):
                if row_str[0] != '{':
                    row_str = row_str[1:]
                while row_str[-1] != '}':
                    row_str = row_str[:-1]
                try:
                    rr = json.loads(row_str)
                except:
                    if row_str.endswith('}]}]}'):
                        rr = json.loads(row_str[:-2])
                if flag:
                    if rr['billing_code'] in billing_code_list:
                        with open(save_path + '{}.json'.format(rr['billing_code']), 'w') as json_file:
                            json.dump(row_str, json_file)
                        sub_df = parse_data(d=rr, insurer=insurer)
                        df = pd.DataFrame(sub_df)
                        df.to_csv(save_path + '{}.csv'.format(rr['billing_code']), index=False)
                else:
                    j += 1
                    with open(save_path + '{}.json'.format(rr['billing_code']),
                              'w') as json_file:
                        json.dump(row_str, json_file)
            else:
                pass
            i += 1
    print('time usage={} mins'.format((time.time() - ts) // 60))


#############################################
def map_files(file_path, provider_ref_csv):
    """
    Function for mapping billing_codes with respective npi values
    Args:
        file_path: folder path containing billing_code csv
        provider_ref_csv: CSV file containing group_ids and npi values
    """
    provider_df = pd.read_csv(provider_ref_csv)
    mappings = {}
    jj, total_steps = 0, len(provider_df)
    for idx, row in provider_df.iterrows():
        row = dict(row)
        group_id = row.get("provider_group_id")
        group_id = str(group_id).strip()
        del row["provider_group_id"]
        mappings[group_id] = row
        jj = sub_process_bar(j=jj, total_step=total_steps)
    print("==>", len(mappings))

    csv_files = [f for f in os.listdir(file_path) if f.endswith(".csv")]
    for f in csv_files:
        f = file_path + "/" + f
        print("Processing..", f)
        output = []
        df = pd.read_csv(f)
        for idx, row in df.iterrows():
            row = dict(row)
            _id = row.get("provider_references")
            _id = str(eval(_id)[0])
            data = mappings.get(_id, None)
            if not data:
                print(group_id)
                continue
            row.update(data)
            output.append(row)
        temp_df = pd.DataFrame(output)
        out_file = f.replace(".csv", "")
        out_file = out_file + "_mapped.csv"
        print("Generating File:", out_file)
        temp_df.to_csv(out_file, index=False)

    del mappings


#############################################
def calculate_memory():
    memory = (psutil.Process(os.getpid()).memory_full_info().uss / 1024) / 1024
    # calculate in MB
    return memory


#############################################
#############################################
if __name__ == '__main__':
    input_config = json.load(open('config_unzip_in_network_united.json', 'r'))
    data_path = input_config.get('data_path')
    file = input_config.get('file')
    if 'cigna-' in file:
        insurer = 'cigna'
    else:
        insurer = None
    billing_code_file = input_config.get('billing_code_file')
    mc1 = calculate_memory()
    time_initial = time.time()
    # For negotiate arrangements
    if input_config.get('parse_negotiate_arrangement'):
        billing_code_list1 = input_config.get('billing_code')
        split_large_json_gz_v3(data_path, file, save_path=billing_code_file, billing_code_list=billing_code_list1, insurer=insurer)
        mc2 = calculate_memory()
        print('Memory usage: {} MB'.format(round(mc2 - mc1, 3)))
    if insurer == 'cigna':
        sys.exit()

    # For provider reference
    if input_config.get('parse_provider_reference'):
        parse_meta = input_config.get('parse_meta')
        save_path2 = data_path + file.replace('.json.gz', '/provider_reference/')
        max_group_id_n = input_config.get('max_n_of_group_id')
        start_from_unzip = input_config.get('start_from_unzip')
        unique_ids = get_ids_list(billing_code_file)
        if start_from_unzip:
            print('Start to unzip gz file....')
            parse_provider_reference_main(data_path, file, save_path2, unique_ids, max_group_id_n)
        else:
            print('Start to collect json files and combine to csv files....')
            form_provider_reference_to_csv(save_path2)
        mc2 = calculate_memory()
        print('Memory usage: {} MB'.format(round(mc2 - mc1, 3)))

    # For mapping group_ids
    if input_config.get("map_group_ids"):
        print("Mapping billing_codes with provider reference groups..")
        provider_reference_file = data_path + file.replace('.json.gz',
                                                           '/provider_reference/') + 'provider_reference.csv'
        map_files(billing_code_file, provider_reference_file)
        mc2 = calculate_memory()
        print('Memory usage: {} MB'.format(round(mc2 - mc1, 3)))
    print('Total time usage = {} mins'.format(round((time.time() - time_initial) / 60, 1)))
