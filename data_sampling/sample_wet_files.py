import random
import os
import csv
import pandas as pd
import gzip
import datetime as dt


def extract_wet_paths(wet_filename):
    with gzip.GzipFile(wet_filename,mode='r') as f:
        paths = [x.decode('utf8').strip() for x in f.readlines()]
    return paths

def files_in_dir():
    return [i for i in os.listdir() if 'wet.path' in i]

def gen_rand_seq(paths,n):
    rand_sequence = [random.randint(0,len(paths)-1) for i in range(n)]
    if len(set(rand_sequence))==n:
        return rand_sequence
    else:
        return gen_rand_seq(paths,n)
    
def process_files(files,n):
    temp = []
    dir_stats = []
    for wet_filename in files:
        paths = extract_wet_paths(wet_filename)
        
        _set = paths[0].split('/segments')[0]
        _len = len(paths)
        
        rand_sequence = gen_rand_seq(paths,n)
        paths_rand = [paths[i] for i in rand_sequence]
        temp.append(paths_rand)
        dir_stats.append([wet_filename,_set,_len])
    pathlist =  [y for x in temp for y in x]
    df = pd.DataFrame(pathlist)
    
    df_stats = pd.DataFrame(dir_stats)
    df_stats.columns = ['wet_filename','set','length']
    
    return df, df_stats

def main():
    files = files_in_dir()

    n = 10
    _today = str(dt.datetime.today()).split(' ')[0]
    filename = _today+'_wet_paths_10series.csv'
    df, df_stats = process_files(files,n)
    df.to_csv(filename,index=False,header=False)
    df_stats.to_csv(filename.replace('.csv','_stats.csv'),index=False)
    print(filename,' -- saved')
    return

if __name__ == '__main__':
    main()