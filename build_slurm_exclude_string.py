# https://stackoverflow.com/questions/37480603/slurm-how-to-run-30-jobs-on-particular-nodes-only

import requests
from bs4 import BeautifulSoup
import json
import re
import subprocess
import fire
import os

def fetch_compute_res_webpage(no_cache):
    if no_cache or not os.path.exists('uva_computing_resources.html'):
        print("Fetching compute resources web page")
        r = requests.get('https://www.cs.virginia.edu/wiki/doku.php?id=compute_resources')
        # print(r.text)
        with open('uva_computing_resources.html', 'w') as outfile:
            outfile.write(r.text)
    else:
        print("Using cached web page")

def read_compute_res_webpage():
    print("Parsing webpage")
    with open('uva_computing_resources.html') as infile:
        computing_resources_html = infile.read()
    return BeautifulSoup(computing_resources_html, features="lxml")

def extract_compute_res_table(soup, print_data):
    def extract_header(soup_table):
        ths = soup_table.find('thead').find('tr').find_all('th')
        return [th.text.strip() for th in ths]

    def extract_body(soup_table, header):
        # print(soup_table)
        data = []
        trs = soup_table.find_all('tr')
        multirow = 0
        for tr in trs:
            # print(tr)
            row = []
            # first_td = tr.find('td')
            # if first_td:
            #     if 'rowspan' in first_td.attrs:
            #         multirow = rowspan - 1

            tds = list(tr.find_all('td'))
            if len(tds) == len(header):
                row = []
                for col, td in zip(header, tds):
                    text = td.text.strip()
                    if col in ['GPUs', 'GPU Type', 'GPU RAM (GB)']:
                        text = [text]
                    row.append(text)
                data.append(row)
            elif len(tds) == 3: # TODO Don't hardcode this part
                for i, td in zip(range(7, 10), tds): # col 7,8,9
                    data[-1][i].append(td.text.strip())
            elif len(tds) == 0: # Header row (thead)
                pass
            else:
                print('TDS', tds)
                raise Exception("Wrong number of columns")
        return data

    table = soup.find(id="nodes_controlled_by_the_slurm_job_scheduler").findNext('table')
    header = extract_header(table)
    if print_data:
        print("HEADER", header)
    body = extract_body(table, header)
    # print(body)

    data = []
    for row in body:
        if print_data:
            print(row)
        assert len(row) == len(header)
        data.append(dict(zip(header, row)))
    return data

def write_compute_res_data_json(data):
    with open('uva_computing_resources.json', 'w') as outfile:
        json.dump(data, outfile)

def read_compute_res_data_json():
    with open('uva_computing_resources.json') as infile:
        return json.load(infile)

#(?:whatever) is a non-capture group
def parse_mem_string(s):
    # print(s)
    m = re.match(r'(\d+)\s*(?:\((\d+)\))?', s)
    if m:
        # print(m.group(1))
        # print(m.group(2))
        return int(m.group(1))
    return 0

def filter_gpu_mem(data, amount):
    names = []
    for row in data:
        min_gpu_mem = min([parse_mem_string(s) for s in row['GPU RAM (GB)']])
        if  min_gpu_mem >= amount and row['GPUs'] and row['GPUs'] != ['0']:
            names.append(row['Hostname'])
    return names

def build_exclude_str(unwanted_machines):
    print("Building exclude string")
    sub_str = ','.join(unwanted_machines)
    return f'#SBATCH --exclude={sub_str}'

def get_gpu_names_sinfo():
    print("Using sinfo to get slurm machine information")
    sinfo_str = subprocess.check_output(['sinfo', '-o', "%N %P"]).decode('utf-8')
    for partition_str in sinfo_str.strip().split('\n'):
        names, part_name = partition_str.split(' ')
        if part_name == 'gpu':
            gpu_names_str = names
           
    return re.findall(r'[a-z]+(?:\d\d|\[(?:(?:\d\d-\d\d|\d\d),)*(?:\d\d-\d\d|\d\d)\])?', gpu_names_str)

# lynx[01-07,10-12]
def explode_name(name):
    m = re.match(r'(.*)\[(.+)\]', name)
    if not m:
        return [name]
    prefix = m.group(1)
    index_str = m.group(2)

    names = []
    for sub_str in index_str.split(','):
        sub_str = sub_str.strip()
        if '-' in sub_str: # Then its a range
            start, end = sub_str.split('-')
            # https://stackoverflow.com/questions/134934/display-number-with-leading-zeros
            "{:02d}".format(20)
            names.extend([prefix+"{:02d}".format(x) for x in range(int(start), int(end)+1)])
        else:
            names.append(prefix+sub_str)
    return names

# print(explode_name("lynx[01-07,10-12]"))
# print(explode_name("pegasusboots"))
# print(explode_name("optane01"))
# print(explode_name("jaguar[01-04]"))
# print(explode_name("ristretto[01,04]"))
def explode_names(names):
    new_names = []
    for name in names:
        new_names.extend(explode_name(name))
    return new_names

def build_exclude_set(orig_wanted_machines, sinfo_list):
    return list(set(sinfo_list) - set(orig_wanted_machines))

# sinfo -o "%N %P"
def main(gpu_mem, no_cache=False, print_data=False):
    fetch_compute_res_webpage(no_cache)
    soup = read_compute_res_webpage()
    data = extract_compute_res_table(soup, print_data)
    write_compute_res_data_json(data)

    data = read_compute_res_data_json()
    wanted_machines = explode_names(filter_gpu_mem(data, gpu_mem))
    gpu_names_sinfo = explode_names(get_gpu_names_sinfo())
    

    #compare_sinfo_webpage_data(data, gpu_names_sinfo)
    print("Acceptable machines:", ",".join(wanted_machines))
    unwanted_machines = build_exclude_set(wanted_machines, gpu_names_sinfo)
    print(build_exclude_str(unwanted_machines))

if __name__ == '__main__':
    fire.Fire(main)



# def fetch_table():
# nodes_controlled_by_the_slurm_job_scheduler