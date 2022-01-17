"""
P02_draw_graph.py

SQLite-DBを読み込み、グラフを描く
作成    2022-01-15  村井智哉
更新    2022-01-17  村井智哉

入力DB      'ListFinder.sqlite3'
            アクセスログ テーブル 'accesslog'
            項目 "対象"(初期値=1)
前処理1     項目 "URL"の内容から以下のように対応する。
            (a)URLが東光高岳である行を対象外(対象=0)とする
            (b)URLがNANである行は対象(対象=1)とする。
                (個人による参照と判断)
前処理2     以下を目的として、項目 "流入ページURL"の内容が
            'https://www.tktk.co.jp/…'を含まない行を
            対象外(対象=0)とする。
            (a)社内のテストページへの参照
                Amazon Web Serviceのページ
                (例 IPが13.112.114.77など)
            (b)不明なページを経由した流入
                (例 www.translatoruser-int.com/…)
集計1       テーブル　"accesslog" にて1ヶ月毎に対象=1の行数を
            DataFrame "df"に求める
            (a)全体_参照数
                条件なし
            (b)QC_J参照数   
                流入ページURL 
                = 'www.tktk.co.jp/product/ev/quickcharger/%'
            (c)QC_E1参照数  
                流入ページURL 
                = 'www.tktk.co.jp/en/product/ev/quickcharger/%'
            (d)QC_E2参照数  
                流入ページURL 
                = 'www.tktk.co.jp/en/product/ev-charging/quickcharger/%'
            (e)V2H_J参照数
                流入ページURL 
                = 'www.tktk.co.jp/product/ev/conditioner-ev/%'
            (f)V2H_E1参照数
                流入ページURL 
                = 'www.tktk.co.jp/en/product/ev/conditioner-ev/%'
            (g)V2H_E2参照数
                流入ページURL 
                = 'www.tktk.co.jp/en/product/ev-charging/conditioner-ev/%'
            
"""
# %%
from time import strftime
import pandas as pd
from datetime import datetime
import sqlite3
import matplotlib.pyplot as plt

dbpath = "ListFinder.sqlite3"

# %%
# 前処理0 全ての行を対象とする

with sqlite3.connect(dbpath) as conn:
    conn.execute("UPDATE accesslog "\
        + "SET 対象=? ",(1,))

# %%
# 前処理1(a)URLが東光高岳である行を対象外とする

with sqlite3.connect(dbpath) as conn:
    conn.execute("UPDATE accesslog "\
        + "SET 対象=? "\
        + "WHERE URL='https://www.tktk.co.jp/'",\
        (0,))

# %%
# 前処理1(b)URLがNANである行は対象とする

with sqlite3.connect(dbpath) as conn:
    conn.execute("UPDATE accesslog "\
        + "SET 対象=? "\
        + "WHERE URL IS NULL",\
        (1,))

# %%
# 前処理2     以下を目的として、項目・流入ページURLの内容が
#             'www.tktk.co.jp/…'を含まない行を対象外とする。

with sqlite3.connect(dbpath) as conn:
    conn.execute("UPDATE accesslog "\
        + "SET 対象=? "\
        + "WHERE NOT(流入ページURL LIKE 'www.tktk.co.jp/%')",\
        (0,))

# %%
# 集計1 テーブル　"accesslog" にて1ヶ月毎に対象=1の行数を
# DataFrame "df"に求める
# 
# cols = ['全体_参照数', 'QC_J参照数', 'QC_E1参照数', 'QC_E2参照数',
#         'V2H_J参照数', 'V2H_E1参照数', 'V2H_E2参照数']
# df = pd.DataFrame(index=[], columns=cols)

# %%
# (a)全体_参照数
#   条件なし

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS '全体_参照数'
        FROM accesslog
        WHERE 対象 = 1
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_ALL = pd.read_sql(SQL, con=conn)

df_ALL

# %%
# (b)QC_J参照数   
#   流入ページURL = 'www.tktk.co.jp/product/ev/quickcharger/%'

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS 'QC_J参照数'
        FROM accesslog
        WHERE 対象 = 1 AND
            流入ページURL LIKE
            'www.tktk.co.jp/product/ev/quickcharger/%'
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_QCJ = pd.read_sql(SQL, con=conn)
df_QCJ

# %%
# (c)QC_E1参照数  
#   流入ページURL = 'www.tktk.co.jp/en/product/ev/quickcharger/%'

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS 'QC_E1参照数'
        FROM accesslog
        WHERE 対象 = 1 AND
            流入ページURL LIKE 
            'www.tktk.co.jp/en/product/ev/quickcharger/%'
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_QCE1 = pd.read_sql(SQL, con=conn)
df_QCE1


# %%
# (d)QC_E2参照数  
#   流入ページURL = 'www.tktk.co.jp/en/product/ev-charging/quickcharger/%'

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS 'QC_E2参照数'
        FROM accesslog
        WHERE 対象 = 1 AND
            流入ページURL LIKE
            'www.tktk.co.jp/en/product/ev-charging/quickcharger/%'
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_QCE2 = pd.read_sql(SQL, con=conn)
df_QCE2

# %%
# (e)V2H_J参照数
#   流入ページURL = 'www.tktk.co.jp/product/ev/conditioner-ev/%'

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS 'V2H_J参照数'
        FROM accesslog
        WHERE 対象 = 1 AND
            流入ページURL LIKE
            'www.tktk.co.jp/product/ev/conditioner-ev/%'
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_V2HJ = pd.read_sql(SQL, con=conn)
df_V2HJ

# %%
# (f)V2H_E1参照数
#   流入ページURL = 'www.tktk.co.jp/en/product/ev/conditioner-ev/%'

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS 'V2H_E1参照数'
        FROM accesslog
        WHERE 対象 = 1 AND
            流入ページURL LIKE
            'www.tktk.co.jp/en/product/ev/conditioner-ev/%'
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_V2HE1 = pd.read_sql(SQL, con=conn)
df_V2HE1

# %%
# (g)V2H_E2参照数
#   流入ページURL = 'www.tktk.co.jp/en/product/ev-charging/conditioner-ev/%'

with sqlite3.connect(dbpath) as conn:
    SQL = """
        SELECT
            strftime('%Y-%m', 日時) AS '年月',
            count(日時) AS 'V2H_E2参照数'
        FROM accesslog
        WHERE 対象 = 1 AND
            流入ページURL LIKE
            'www.tktk.co.jp/en/product/ev-charging/conditioner-ev/%'
        GROUP BY strftime('%Y-%m', 日時);
    """
    df_V2HE2 = pd.read_sql(SQL, con=conn)
df_V2HE2

# %%
# 参照数を連結する

columns = {"月末"}
df = pd.DataFrame(
    pd.date_range(
        '2019-02-01', '2021-09-30', freq='M'),
    index=pd.date_range(
        '2019-02-01', '2021-09-30', freq='M'),
    columns=columns
    )
df

#%%
df['年月'] = df['月末'].dt.strftime('%Y-%m')

df = pd.merge(df, df_ALL, on='年月', how='left')
df = pd.merge(df, df_QCJ, on='年月', how='left')
df = pd.merge(df, df_QCE1, on='年月', how='left')
df = pd.merge(df, df_QCE2, on='年月', how='left')
df = pd.merge(df, df_V2HJ, on='年月', how='left')
df = pd.merge(df, df_V2HE1, on='年月', how='left')
df = pd.merge(df, df_V2HE2, on='年月', how='left')

df['QC参照数'] \
    = df['QC_J参照数'].fillna(0) \
    + df['QC_E1参照数'].fillna(0) \
    + df['QC_E2参照数'].fillna(0)

df['V2H参照数'] \
    = df['V2H_J参照数'].fillna(0) \
    + df['V2H_E1参照数'].fillna(0) \
    + df['V2H_E2参照数'].fillna(0)

df
# %%
plt.rcParams['font.family'] = 'IPAexGothic'
plt.title('TKTK-WebPage月別参照数')
plt.xlabel('年月')
plt.ylabel('参照数')
plt.plot(df['年月'],df['全体_参照数'], label='全体')
plt.plot(df['年月'],df['QC参照数'], label='QC')
plt.plot(df['年月'],df['V2H参照数'], label='V2H')
plt.xticks([1, 4, 7, 10, 13, 16, 
    19, 22, 25, 28], rotation=90)
plt.legend()
plt.style.use('bmh')

# %%
# 全体、QC合計、V2H合計の参照数を比較するグラフを描く
