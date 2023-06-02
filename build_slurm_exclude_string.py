# https://stackoverflow.com/questions/37480603/slurm-how-to-run-30-jobs-on-particular-nodes-only

import requests
from bs4 import BeautifulSoup
import json
import re
import subprocess

def fetch_compute_res_webpage():
    r = requests.get('https://www.cs.virginia.edu/wiki/doku.php?id=compute_resources')
    # print(r.text)
    with open('uva_computing_resources.html', 'w') as outfile:
        outfile.write(r.text)

def read_compute_res_webpage():
    with open('uva_computing_resources.html') as infile:
        computing_resources_html = infile.read()
    return BeautifulSoup(computing_resources_html)

def extract_compute_res_table(soup):
    def extract_header(soup_table):
        ths = soup_table.find('thead').find('tr').find_all('th')
        return [th.text.strip() for th in ths]

    def extract_body(soup_table):
        # print(soup_table)
        data = []
        trs = soup_table.find_all('tr')
        for tr in trs:
            row = []
            for td in tr.find_all('td'):
                row.append(td.text.strip())
            data.append(row)
        return data[1:] # Skips first row because that was for the thead

    table = soup.find(id="nodes_controlled_by_the_slurm_job_scheduler").findNext('table')
    header = extract_header(table)
    body = extract_body(table)

    data = []
    for row in body:
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
        # NOTE: LESS THAN BECAUSE WE ARE DOING INVERSE
        if parse_mem_string(row['GPU RAM (GB)']) < amount and row['GPUs'] and int(row['GPUs']) > 0:
            names.append(row['Hostname'])
    return names

def build_exclude_str(unwanted_machines):
    sub_str = ','.join(unwanted_machines)
    return f'#SBATCH --exclude={sub_str}'

def get_gpu_names_sinfo():
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

def compare_sinfo_webpage_data(webpage_data, gpu_names_sinfo):
    webpage_names = []
    for row in webpage_data:
        if int(row['GPUs']) > 0:
            webpage_names.append(row['Hostname'])
    print('Webpage Names:', webpage_names)

    exploded_webpage_names = []
    for name in webpage_names:
        exploded_webpage_names.extend(explode_name(name))

    print('SINFO GPU Names:', gpu_names_sinfo)
    exploded_sinfo_names = []
    for name in gpu_names_sinfo:
        exploded_sinfo_names.extend(explode_name(name))

    print('Machine on webpage, missing from SINFO:', 
        set(exploded_webpage_names)-set(exploded_sinfo_names))
    print('Machine on SINFO, missing from WEBPAGE:',
        set(exploded_sinfo_names)-set(exploded_webpage_names))

# sinfo -o "%N %P"
def main():
    # fetch_compute_res_webpage()
    # soup = read_compute_res_webpage()
    # data = extract_compute_res_table(soup)
    # write_compute_res_data_json(data)

    data = read_compute_res_data_json()
    unwanted_machines = filter_gpu_mem(data, 30)
    gpu_names_sinfo = get_gpu_names_sinfo()
    compare_sinfo_webpage_data(data, gpu_names_sinfo)
    print()
    print(build_exclude_str(unwanted_machines))

if __name__ == '__main__':
    main()



# def fetch_table():
# nodes_controlled_by_the_slurm_job_scheduler