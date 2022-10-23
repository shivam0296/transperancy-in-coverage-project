import gc
import gzip
import os
import json
import numpy as np
import time
import sys
import pandas as pd


########################################
def sub_process_bar(j, total_step):
    str_ = '>' * (50 * j // total_step) + ' ' * (50 - 50 * j // total_step)
    sys.stdout.write('\r[' + str_ + '][%s%%]' % (round(100 * j / total_step, 2)))
    sys.stdout.flush()
    j += 1
    return j


# parsing meta information
def form_provider_reference_to_csv(save_path):
    meta = {"provider_group_id": [], 'tin_type':[], 'tin_value':[]}
    json_f_list = [f1 for f1 in os.listdir(save_path) if f1.endswith('.json')]
    total_steps = len(json_f_list)
    jj = 0
    # df = pd.DataFrame({})
    df_list = []
    for file in json_f_list:
        with open(save_path + file) as f:
            data = json.load(f)
        data = json.loads(data)

        provider_groups = data.get("provider_groups")
        group_id = data.get("provider_group_id")
        tin_type_group = {}
        for sub in provider_groups:
            if sub['tin']['type'] in tin_type_group:
                tin_type_group[sub['tin']['type']] = tin_type_group[sub['tin']['type']] +  '/' + sub['tin']['value']
            else:
                tin_type_group[sub['tin']['type']] = sub['tin']['value']

        for tin_type in tin_type_group:
            meta["provider_group_id"].append(group_id)
            meta['tin_type'].append(tin_type)
            meta['tin_value'].append(tin_type_group[tin_type])

        del data
        df_list.append(pd.DataFrame(meta))
        # df = pd.concat([df, pd.DataFrame(meta)], axis=0)  # Can we reduce RAM?
        gc.collect()
        jj = sub_process_bar(j=jj, total_step=total_steps)
    df = pd.concat(df_list, axis=0)
    df.to_csv(save_path+'provider_reference.csv', index=None)
    print('provider_reference.csv file saved.')
    print('Start to remove json files...')
    for file in json_f_list:
        os.remove(save_path + file)


########################################
def get_ids_list(file_path):
    ids_set = set()
    csv_f_list = [f1 for f1 in os.listdir(file_path) if f1.endswith('.csv')]
    for f in csv_f_list:
        df = pd.read_csv(file_path + "/" + f)
        df["group_id"] = df["provider_references"].apply(lambda x: str(eval(x)[0]))
        unique_ids = set(df["group_id"].unique())
        print("Unique ids in {} are {}".format(f, str(len(unique_ids))))
        ids_set.update(unique_ids)
    return ids_set


########################################
def parse_provider_reference_main(data_path, file, save_path,  unique_ids, max_group_id_n=10000):
    """
    :param data_path: where the .json.gz file locate
    :param file: the name of .json.gz file
    :param save_path: where the splitted files should be stored
    :param billing_code_list: ['0001', '0002M',....], None for all
    allow specific billing code
    """
    assert max_group_id_n >= len(unique_ids)
    ts = time.time()
    try:
        os.makedirs(save_path)
    except:
        pass

    with gzip.open(data_path + file, 'rb') as f:
        i, n = 0, 0
        flag = 0
        for line in f:
            row_str = line.decode("utf-8")
            if row_str.startswith('\t{\"negotiation_arrangement') or row_str.startswith('{\"negotiation_arrangement') or row_str.startswith('"in_network"'):
                print('i=', i)
                print('start negotiation arrangement data, break')
                break
            elif row_str.startswith('"provider_references":[') or row_str.startswith('"provider_references": ['):
                print('start provider_references...')
                flag = 1
                if n % 5000 == 0:
                    print('Currently number of group:{}'.format(n))
                if 'provider_group_id' in row_str:
                    row_str2 = row_str[22:].replace(',\n', '')
                    rr_all = json.loads(row_str2)
                    for rr in rr_all:
                        id = str(rr['provider_group_id'])
                        if id not in unique_ids:
                            # Checking if id is present in the required ids list
                            continue
                        id = id.replace('.', '_')
                        with open(save_path + '{}.json'.format(id), 'w') as json_file:
                            json.dump(str(rr).replace('\'', '\"'), json_file)
                        n += 1
            else:
                if flag:
                    row_str2 = row_str.replace(',\n', '')
                    try:
                        rr = json.loads(row_str2)
                    except:
                        print('Fail: rr = json.loads(row_str2)', row_str2[:30], row_str2[-30:])
                        break
                    id = str(rr['provider_group_id'])
                    if n % 5000 == 0:
                        print('Currently number of group:{}'.format(n))
                    if id not in unique_ids:
                        # Checking if id is present in the required ids list
                        continue
                    id = id.replace('.','_')
                    with open(save_path + '{}.json'.format(id), 'w') as json_file:
                        json.dump(row_str2, json_file)
                    n += 1
                else:
                    pass
            i += 1
            if n > max_group_id_n:
                print('Already parsing {} provider group id......'.format(max_group_id_n))
                break
    print('time usage={} mins'.format((time.time() - ts) // 60))
    form_provider_reference_to_csv(save_path)


########################################
if __name__ == '__main__':
    input_config = json.load(open('config_unzip_in_network.json', 'r'))
    file = input_config.get('file')
    data_path = input_config.get('data_path')
    save_path = data_path + file.replace('.json.gz', '/provider_reference/')
    max_group_id_n = input_config.get('max_n_of_group_id')
    start_from_unzip = input_config.get('start_from_unzip')
    billing_code_file = input_config.get('billing_code_file')
    unique_ids = get_ids_list(billing_code_file)
    if start_from_unzip:
        print('Start to unzip gz file....')
        parse_provider_reference_main(data_path, file, save_path, unique_ids, max_group_id_n)
    else:
        print('Start to collect json files and combine to csv files....')
        form_provider_reference_to_csv(save_path)