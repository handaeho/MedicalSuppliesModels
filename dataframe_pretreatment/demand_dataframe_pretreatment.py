import pandas as pd
import pymysql

from login.info import info

login = info


def select_table(niin, _type, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        params = {'niin': niin, '_type': _type, 'year': year}

        query = "select type as types, niin," \
                " x_minus_11, x_minus_10, x_minus_9, x_minus_8, x_minus_7, x_minus_6, x_minus_5, x_minus_4, x_minus_3," \
                " x_minus_2, x_minus_1, x_year" \
                " from demand" \
                " where niin = %(niin)s and type = %(_type)s and year(x_year) = %(year)s"

        cur.execute(query, params)

        conn.commit()

        row = []
        for r in cur:
            row.append(r)

        df = pd.DataFrame(row, columns=['type', 'niin', 'x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                        'x_5', 'x_4', 'x_3', 'x_2', 'x_1', 'year'])

    finally:
        cur.close()
        conn.close()

    return df


def drug_demand_pret(niin, year):
    drug_df = select_table(niin, 'drug', year)
    drug_dataset = drug_df[drug_df['niin'] == niin]
    drug_dataset = drug_dataset.astype({'niin': 'str',
                                        'x_11': 'float', 'x_10': 'float', 'x_9': 'float',
                                        'x_8': 'float', 'x_7': 'float', 'x_6': 'float',
                                        'x_5': 'float', 'x_4': 'float', 'x_3': 'float',
                                        'x_2': 'float', 'x_1': 'float'})

    return drug_dataset


def goods_demand_pret(niin, year):
    goods_df = select_table(niin, 'goods', year)
    goods_dataset = goods_df[goods_df['niin'] == niin]
    goods_dataset = goods_dataset.astype({'niin': 'str',
                                          'x_11': 'float', 'x_10': 'float', 'x_9': 'float',
                                          'x_8': 'float', 'x_7': 'float', 'x_6': 'float',
                                          'x_5': 'float', 'x_4': 'float', 'x_3': 'float',
                                          'x_2': 'float', 'x_1': 'float'})

    return goods_dataset


def train_target_split(data):
    # Split demand data into train and target
    train = data[:-1]
    target = data[-1:]

    return train, target
