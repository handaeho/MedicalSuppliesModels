import pymysql
import pandas as pd
from login.info import info

login = info


def select_hosp(drugcode, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        params = {'drug_code': drugcode, 'year': year}

        query = "select drug_code, hosp_div," \
                " sum(X_Minus_7_Q) as x_7_q, sum(X_Minus_6_Q) as x_6_q, sum(X_Minus_5_Q) as x_5_q, " \
                " sum(X_Minus_4_Q) as x_4_q, sum(X_Minus_3_Q) as x_3_q, sum(X_Minus_2_Q) as x_2_q, " \
                " sum(X_Minus_1_Q) as x_1_q " \
                " from prescription_sumqc ps " \
                " where drug_code = %(drug_code)s and year(year) = %(year)s group by hosp_div"

        cur.execute(query, params)

        conn.commit()

        row = []
        for r in cur:
            row.append(r)

        df = pd.DataFrame(row, columns=['drug_code', 'hosp_div',
                                        'x_7_q', 'x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q'])

        r_df = df.copy()

        re_df = r_df[r_df['hosp_div'] == '병원']

    finally:
        cur.close()
        conn.close()

    return re_df


def select_div(drugcode, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        params = {'drug_code': drugcode, 'year': year}

        if year == '2020':
            query_20 = "select drug_code, hosp_div," \
                       " sum(X_Minus_3_Q) as x_3_q, sum(X_Minus_2_Q) as x_2_q, sum(X_Minus_1_Q) as x_1_q " \
                       " from prescription_sumqc ps " \
                       " where drug_code = %(drug_code)s and year(year) = %(year)s group by hosp_div"

            cur.execute(query_20, params)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['drug_code', 'hosp_div', 'x_3_q', 'x_2_q', 'x_1_q'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2021':
            query_21 = "select drug_code, hosp_div," \
                       " sum(X_Minus_4_Q) as x_4_q, sum(X_Minus_3_Q) as x_3_q, sum(X_Minus_2_Q) as x_2_q, " \
                       " sum(X_Minus_1_Q) as x_1_q " \
                       " from prescription_sumqc ps " \
                       " where drug_code = %(drug_code)s and year(year) = %(year)s group by hosp_div"

            cur.execute(query_21, params)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['drug_code', 'hosp_div', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2022':
            query_22 = "select drug_code, hosp_div," \
                       " sum(X_Minus_5_Q) as x_5_q, sum(X_Minus_4_Q) as x_4_q, sum(X_Minus_3_Q) as x_3_q, " \
                       " sum(X_Minus_2_Q) as x_2_q, sum(X_Minus_1_Q) as x_1_q " \
                       " from prescription_sumqc ps " \
                       " where drug_code = %(drug_code)s and year(year) = %(year)s group by hosp_div"

            cur.execute(query_22, params)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['drug_code', 'hosp_div', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2023':
            query_23 = "select drug_code, hosp_div," \
                       " sum(X_Minus_6_Q) as x_6_q, sum(X_Minus_5_Q) as x_5_q, sum(X_Minus_4_Q) as x_4_q, " \
                       " sum(X_Minus_3_Q) as x_3_q, sum(X_Minus_2_Q) as x_2_q, sum(X_Minus_1_Q) as x_1_q " \
                       " from prescription_sumqc ps " \
                       " where drug_code = %(drug_code)s and year(year) = %(year)s group by hosp_div"

            cur.execute(query_23, params)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['drug_code', 'hosp_div', 'x_6_q', 'x_5_q',
                                            'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        else:
            query_after_24 = "select drug_code, hosp_div," \
                             " sum(X_Minus_7_Q) as x_7_q, sum(X_Minus_6_Q) as x_6_q, sum(X_Minus_5_Q) as x_5_q, " \
                             " sum(X_Minus_4_Q) as x_4_q, sum(X_Minus_3_Q) as x_3_q, sum(X_Minus_2_Q) as x_2_q, " \
                             " sum(X_Minus_1_Q) as x_1_q " \
                             " from prescription_sumqc ps " \
                             " where drug_code = %(drug_code)s and year(year) = %(year)s group by hosp_div"

            cur.execute(query_after_24, params)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['drug_code', 'hosp_div', 'x_7_q', 'x_6_q', 'x_5_q',
                                            'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

    finally:
        cur.close()
        conn.close()

    return re_df


def train_target_split(data):
    # Split demand data into train and target
    train = data[:-1]
    target = data[-1:]

    return train, target
