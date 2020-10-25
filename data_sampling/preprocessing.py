import random
import os
import csv
import pandas as pd
import gzip
import datetime as dt
import boto3
import logging

log_level = 'INFO'
_name = 'spark-cc-analysis'
LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
logging.basicConfig(level=log_level, format=LOGGING_FORMAT)


def save_config(today, new_dir, filename_inputcsv, bucket,
                filename_outputparquet):
    lines = [
        "'''\n", "config file for anything that shouldn't be on github\n",
        "'''\n", "input_file = '" + today + "_" + filename_inputcsv + "'\n",
        "output_file = 's3://" + bucket + "/results/" + new_dir + "/" +
        filename_outputparquet + "'\n"
    ]
    with open('new_config.py', 'w') as f:
        for line in lines:
            f.write(line)


def save_projectfiles_tos3(new_dir, cwd, bucket):
    '''
    filename: local file or filepath
    bucket: name for boto3 regex search
    _key: new directory path with filename appended'''
    try:
        os.chdir(new_dir)
        files = os.listdir()
        for file in files:
            _key = 'results/' + new_dir + '/' + file
            logging.info('upload to S3: ' + _key)
            s3 = boto3.resource('s3')
            s3.meta.client.upload_file(Filename=file, Bucket=bucket, Key=_key)
    finally:
        os.chdir(cwd)


def extract_wet_paths(wet_filename):
    with gzip.GzipFile(wet_filename, mode='r') as f:
        paths = [x.decode('utf8').strip() for x in f.readlines()]
    return paths


def files_in_dir():
    return [i for i in os.listdir() if 'wet.path' in i]


def gen_rand_seq(paths, n):
    rand_sequence = [random.randint(0, len(paths) - 1) for i in range(n)]
    if len(set(rand_sequence)) == n:
        return rand_sequence
    else:
        return gen_rand_seq(paths, n)


def process_files(files, n):
    temp = []
    dir_stats = []
    for wet_filename in files:
        paths = extract_wet_paths(wet_filename)

        _set = paths[0].split('/segments')[0]
        _len = len(paths)

        rand_sequence = gen_rand_seq(paths, n)
        paths_rand = [paths[i] for i in rand_sequence]
        temp.append(paths_rand)
        dir_stats.append([wet_filename, _set, _len])
    pathlist = [y for x in temp for y in x]
    df = pd.DataFrame(pathlist)

    df_stats = pd.DataFrame(dir_stats)
    df_stats.columns = ['wet_filename', 'set', 'length']
    return df, df_stats


def mknewdir(new_dir):
    if new_dir in os.listdir():
        return mknewdir(new_dir + '_dup')
    else:
        return new_dir


def main():
    today = str(dt.datetime.today()).split(' ')[0]
    cwd = os.getcwd()
    n = 10

    # create list of wet files
    try:
        os.chdir('wet_path_files')
        files = files_in_dir()
        filename = today + '_wet_paths_10series.csv'
        df, df_stats = process_files(files, n)
    finally:
        os.chdir(cwd)

    # save csvs and config to project sub directory
    bucket = 'will-cc-bucket'
    new_dir = today + '_cc_process_subdir'
    new_dir = mknewdir(new_dir)

    try:
        os.mkdir(new_dir)
        os.chdir(new_dir)
        logging.info('dataframe length: ' + str(len(df)))

        df.to_csv(filename, index=False, header=False)
        df_stats.to_csv(filename.replace('.csv', '_stats.csv'), index=False)

        filename_inputcsv = "wet_paths_10series.csv"
        filename_outputparquet = "results_newprocess_690paths.parquet"
        save_config(today, new_dir, filename_inputcsv, bucket,
                    filename_outputparquet)
    finally:
        logging.info('files in ' + new_dir + ': ' + str(os.listdir()))
        os.chdir(cwd)

    save_projectfiles_tos3(new_dir, cwd, bucket)
    logging.info('files saved to sub directory and uploaded to S3')
    return


if __name__ == '__main__':
    main()
