import requests
import csv
import datetime
from datetime import timedelta
import pandas as pd
import json
import matplotlib.path as mplPath
from tqdm import tqdm

from subprocess import PIPE, Popen


def is_in_poly(geo_point, poly):
    try:
        poly1 = json.loads(poly)['coordinates']
        poly1 = [[i[1],i[0]] for i in poly1[0]]
        poly_path = mplPath.Path(poly1)
        tmp =  geo_point.split(', ')
        x1 =(float(tmp[0]), float(tmp[1]))
        return poly_path.contains_point(x1)
    except:
        return False

def main():
    print("Chargement du premier dataframe")
    current_date = datetime.datetime.now().replace(microsecond=0).isoformat()
    current_date += 'Z'
    next_day = datetime.datetime.now() - timedelta(days=1)
    next_day = str(next_day.replace(microsecond=0)) + 'Z'
    next_day = next_day.replace(" ", "T")
    query = 't_1h: [' + current_date + ' TO ' + next_day + ']'

    payload = {'limit': 10000,
               'q': query,
               'sort': '-t_1h',
               'facet': 't_1h'
               }

    res1 = requests.get('https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv/',
                        params=payload)

    decoded_content = res1.content.decode('utf-8')

    cr = csv.reader(decoded_content.splitlines(), delimiter=';')
    my_list = list(cr)
    df = pd.DataFrame(my_list)
    df = df.rename(columns=df.iloc[0]).drop(0)
    df = df.reset_index(drop=True)


    print("Chargement du second dataframe")
    payload2 = {'facet': 'l_qu'}

    res2 = requests.get('https://opendata.paris.fr/api/v2/catalog/datasets/quartier_paris/exports/csv/',
                        params=payload2)

    decoded_content2 = res2.content.decode('utf-8')

    cr2 = csv.reader(decoded_content2.splitlines(), delimiter=';')
    my_list2 = list(cr2)
    df2 = pd.DataFrame(my_list2)
    df2 = df2.rename(columns=df2.iloc[0]).drop(0)
    df2 = df2.reset_index(drop=True)
    print(df2.shape)
    df2.head(5)
    for i in range(len(df2)):
        df2.loc[i, 'geom_custom'] = str(json.loads(df2.loc[i, 'geom'])['coordinates'])
        # notre df avec les geopoints pour calculer le quartier
    df_iu_geo = df[['iu_ac', 'geo_point_2d']].drop_duplicates()
    print("Jointure")
    for i in tqdm(range(len(df_iu_geo))):
        for j in range(len(df2)):
            if is_in_poly(df_iu_geo.loc[i].geo_point_2d, df2.loc[j].geom):
                df_iu_geo.loc[i, 'c_qu'] = df2.loc[j, 'c_qu']
                break
    df_iu_geo.to_csv("merging_df.csv", sep=";")
    hdfs_file = "hdfs:///data/g6/clean/merging_df.csv"
    put = Popen(["hadoop", "fs", "-copyFromLocal", "-f", "merging_df.csv", hdfs_file], stdin=PIPE, bufsize=-1)
    print("Writing data to hdfs")
    # subprocess.call(["hadoop fs -copyFromLocal   hdfs:///data/g6/raw/daily_data2.csv"], shell=True) # pas sur du chemin HDFS
    put.communicate()

    df2.to_csv("merging_df2.csv", sep=";")
    hdfs_file = "hdfs:///data/g6/clean/merging_df2.csv"
    put = Popen(["hadoop", "fs", "-copyFromLocal", "-f", "merging_df2.csv", hdfs_file], stdin=PIPE, bufsize=-1)
    print("Writing data to hdfs")
    # subprocess.call(["hadoop fs -copyFromLocal   hdfs:///data/g6/raw/daily_data2.csv"], shell=True) # pas sur du chemin HDFS
    put.communicate()


if __name__ == '__main__':
    main()