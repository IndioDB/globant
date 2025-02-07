import pandas as pd
import os
import psycopg2
import re
from dotenv import load_dotenv

load_dotenv()
dbname = os.getenv("DB_NAME")
user = os.getenv("USER_DB")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")

def correct_brands(brand):
    correct_dict = {
        "AERIE": "AERIE",
        "b-temptd": "b-tempt'd",
        "b.tempt'd": "b-tempt'd",
        "b.tempt'd by Wacoal": "b.tempt'd by Wacoal",
        "B.TEMPT'D BY WACOAL": "b.tempt'd by Wacoal",
        "Calvin-Klein": "Calvin Klein",
        "Calvin Klein": "Calvin Klein",
        "CALVIN KLEIN": "Calvin Klein",
        "Hanky-Panky": "Hanky Panky",
        "Hanky Panky": "Hanky Panky",
        "HANKY PANKY": "Hanky Panky",
        "HankyPanky": "Hanky Panky",
        "NORDSTROM LINGERIE": "NORDSTROM LINGERIE",
        "ref=w_bl_sl_l_ap_ap_web_2586685011?ie=UTF8&node=2586685011&field-lbr_brands_browse-bin=Calvin+Klein": "Calvin Klein",
        "ref=w_bl_sl_l_b_ap_web_2586451011?ie=UTF8&node=2586451011&field-lbr_brands_browse-bin=b.tempt%27d": "b-tempt'd",
        "ref=w_bl_sl_l_b_ap_web_2603426011?ie=UTF8&node=2603426011&field-lbr_brands_browse-bin=Wacoal": "Wacoal",
        "s": "s",
        "US TOPSHOP": "US TOPSHOP",
        "Vanity Fair": "Vanity Fair",
        "Victoria's Secret": "Victoria's Secret",
        "Victoria's Secret Pink": "Victoria's Secret Pink",
        "Victorias-Secret": "Victoria's Secret",
        "Wacoal": "Wacoal",
        "WACOAL": "Wacoal",
    }
    if brand in correct_dict.keys():
        return correct_dict[brand]


def update_table():
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE globant1 RESTART IDENTITY;")
    with open('data_compilated.csv', 'r', encoding='utf-8') as f:
        cur.copy_expert("COPY globant1 FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    cur.close()
    conn.close()


def is_bra(sizes):
    sizes = sizes.split(',')
    for size in sizes:
        if re.match(r'^\d{2,3}[A-X]*', size):
            return True
        if re.match(r'^\dX$', size):
            return True
    return False


def build_csv():
    # read all files in this directory and create a list of the files that ends with '.csv'
    dir = os.listdir('data')
    files = [f for f in dir if f.endswith('.csv')]
    # create an empty dataframe
    df = pd.DataFrame()
    # iter over every '.csv' file in the selected directory
    for csv in files:
        # reads the '.csv' file
        temp_df = pd.read_csv(f'data/{csv}')
        # create a new column called 'store' that has the value of the '.csv' it was red from
        temp_df['store'] = csv.replace('.csv', '')
        # available sizes where not matching the total_sizes, so I had to 'upper' the array
        temp_df['available_size'] = temp_df['available_size'].str.upper()
        # I'll attach this function below (1). But it uses patterns (using regex) to see if the item is a bra or not
        temp_df['is_bra'] = temp_df['total_sizes'].apply(is_bra)
        # I'll also attach this function below (2). Some brands names was not correct, so I put up a dictionary to correct this
        temp_df['brand_name'] = temp_df['brand_name'].apply(correct_brands)
        # In the column mrp some values had '$' prefix, so it was not converting to float at Power BI, so I had the preffix removed
        temp_df['mrp'] = temp_df['mrp'].apply(lambda x: float(str(x).replace('$', '')))
        # Usually vizualization tools have problems reading null values, so I saw that the only columns that had null was numeric, so I filled with zeros
        temp_df = temp_df.fillna(0)
        # This part concats the '.csv' that is being readed with all it's transformation to that blank dataframe that we created
        df = pd.concat([df, temp_df], ignore_index=True)
    # Now it creates a new '.csv' for me to consume further
    df.to_csv('data_compilated.csv', index=False)


def create_availability_table():
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cur = conn.cursor()
    sql = open('sql_views/availability_per_store.sql').read()
    cur.execute(sql)
    cur.execute("DROP TABLE IF EXISTS tb_availability;")
    cur.execute("CREATE TABLE tb_availability AS SELECT * FROM vw_availability;")
    conn.commit()
    conn.close()
    cur.close()


if __name__ == '__main__':
    build_csv()
    update_table()
    create_availability_table()
