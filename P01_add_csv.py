#%%
"""
P01_add_csv.py

CSVファイル(5日分)から2年半分の情報にかさ増ししてSQLite-DBを作成する
作成　2022-01-15　村井智哉

入力ファイル　"20210901-20210905_テストUTF-8.csv"
　　　　　　　CSVから読み込む情報は2021年9月1日〜5日の5日分
編集　　　　　項目"日時"を日付として認識するため"/"を"-"に変更する
編集　　　　　日付を5日
出力DB 　　　"ListFinder.sqlite3"
"""
#%%
import pandas as pd
import sqlite3
#%%
filepath = "20210901-20210905_テストUTF-8.csv"
df = pd.read_csv(filepath)
df['日時'] = pd.to_datetime(df['日時'])
df['日時'].head()
#%%
df['日時'].tail()
#%%
from datetime import datetime, timedelta

file_sqlite3 = "./ListFinder.sqlite3"
table_file = "accesslog"

df1 = df.copy()
#%%
for d in range(-920, 30, 5):
    df1['日時'] = df['日時'] + timedelta(days=d)

    with sqlite3.connect(file_sqlite3) as conn:
        df1.to_sql(table_file, con=conn, if_exists='append')


#%%
df1['日時'].head()
