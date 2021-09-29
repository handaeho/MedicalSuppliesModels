import pandas as pd
import pymysql
from login.info import info

login = info


def select_hosp(diag_code, in_out, location, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        query = "select diag_code, hosp_div," \
                " sum(X_Minus_7_C) as x_7_c, sum(X_Minus_6_C) as x_6_c, sum(X_Minus_5_C) as x_5_c, " \
                " sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, " \
                " sum(X_Minus_1_C) as x_1_c " \
                " from prescription_diag_sumqc2 pds " \
                " where diag_code = '{}' and in_out in {} and loc = '{}' and year(year) = '{}' group by hosp_div"

        query_string = query.format(diag_code, in_out, location, year)

        cur.execute(query_string)

        conn.commit()

        row = []
        for r in cur:
            row.append(r)

        df = pd.DataFrame(row, columns=['diag_code', 'hosp_div',
                                        'x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c',  'x_2_c', 'x_1_c'])

        r_df = df.copy()

        re_df = r_df[r_df['hosp_div'] == '병원']

    finally:
        cur.close()
        conn.close()

    return re_df


def select_div(diag_code, in_out, location, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        if year == '2020':
            query_20 = "select diag_code, hosp_div," \
                       " sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and loc = '{}' and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_20 = query_20.format(diag_code, in_out, location, year)

            cur.execute(query_string_20)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2021':
            query_21 = "select diag_code, hosp_div," \
                       " sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, " \
                       " sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and loc = '{}' and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_21 = query_21.format(diag_code, in_out, location, year)

            cur.execute(query_string_21)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2022':
            query_22 = "select diag_code, hosp_div," \
                       " sum(X_Minus_5_C) as x_5_c, sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, " \
                       " sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and loc = '{}' and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_22 = query_22.format(diag_code, in_out, location, year)

            cur.execute(query_string_22)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2023':
            query_23 = "select diag_code, hosp_div," \
                       " sum(X_Minus_6_C) as x_6_c, sum(X_Minus_5_C) as x_5_c, sum(X_Minus_4_C) as x_4_c, " \
                       " sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and loc = '{}' and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_23 = query_23.format(diag_code, in_out, location, year)

            cur.execute(query_string_23)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_6_c', 'x_5_c', 'x_4_c',
                                            'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        else:
            query_after_24 = "select diag_code, hosp_div," \
                             " sum(X_Minus_7_C) as x_7_c, sum(X_Minus_6_C) as x_6_c, sum(X_Minus_5_C) as x_5_c, " \
                             " sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, " \
                             " sum(X_Minus_1_C) as x_1_c " \
                             " from prescription_diag_sumqc2 pds " \
                             " where diag_code = '{}' and in_out in {} and loc = '{}' and year(year) = '{}' " \
                             " group by hosp_div"

            query_string_after_24 = query_after_24.format(diag_code, in_out, location, year)

            cur.execute(query_string_after_24)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                            'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

    finally:
        cur.close()
        conn.close()

    return re_df


def select_all_loc_hosp(diag_code, in_out, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        query = "select diag_code, hosp_div," \
                " sum(X_Minus_7_C) as x_7_c, sum(X_Minus_6_C) as x_6_c, sum(X_Minus_5_C) as x_5_c, " \
                " sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, " \
                " sum(X_Minus_1_C) as x_1_c " \
                " from prescription_diag_sumqc2 pds " \
                " where diag_code = '{}' and in_out in {} and year(year) = '{}' group by hosp_div"

        query_string = query.format(diag_code, in_out, year)

        cur.execute(query_string)

        conn.commit()

        row = []
        for r in cur:
            row.append(r)

        df = pd.DataFrame(row, columns=['diag_code', 'hosp_div',
                                        'x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c'])

        r_df = df.copy()

        re_df = r_df[r_df['hosp_div'] == '병원']

    finally:
        cur.close()
        conn.close()

    return re_df


def select_all_loc_div(diag_code, in_out, year):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        if year == '2020':
            query_20 = "select diag_code, hosp_div," \
                       " sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_20 = query_20.format(diag_code, in_out, year)

            cur.execute(query_string_20)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2021':
            query_21 = "select diag_code, hosp_div," \
                       " sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, " \
                       " sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_21 = query_21.format(diag_code, in_out, year)

            cur.execute(query_string_21)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2022':
            query_22 = "select diag_code, hosp_div," \
                       " sum(X_Minus_5_C) as x_5_c, sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, " \
                       " sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_22 = query_22.format(diag_code, in_out, year)

            cur.execute(query_string_22)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        elif year == '2023':
            query_23 = "select diag_code, hosp_div," \
                       " sum(X_Minus_6_C) as x_6_c, sum(X_Minus_5_C) as x_5_c, sum(X_Minus_4_C) as x_4_c, " \
                       " sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, sum(X_Minus_1_C) as x_1_c " \
                       " from prescription_diag_sumqc2 pds " \
                       " where diag_code = '{}' and in_out in {} and year(year) = '{}' " \
                       " group by hosp_div"

            query_string_23 = query_23.format(diag_code, in_out, year)

            cur.execute(query_string_23)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_6_c', 'x_5_c', 'x_4_c',
                                            'x_3_c', 'x_2_c', 'x_1_c'])

            r_df = df.copy()

            re_df = r_df[r_df['hosp_div'] == '사단']

        else:
            query_after_24 = "select diag_code, hosp_div," \
                             " sum(X_Minus_7_C) as x_7_c, sum(X_Minus_6_C) as x_6_c, sum(X_Minus_5_C) as x_5_c, " \
                             " sum(X_Minus_4_C) as x_4_c, sum(X_Minus_3_C) as x_3_c, sum(X_Minus_2_C) as x_2_c, " \
                             " sum(X_Minus_1_C) as x_1_c " \
                             " from prescription_diag_sumqc2 pds " \
                             " where diag_code = '{}' and in_out in {} and year(year) = '{}' " \
                             " group by hosp_div"

            query_string_after_24 = query_after_24.format(diag_code, in_out, year)

            cur.execute(query_string_after_24)

            conn.commit()

            row = []
            for r in cur:
                row.append(r)

            df = pd.DataFrame(row, columns=['diag_code', 'hosp_div', 'x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                            'x_3_c', 'x_2_c', 'x_1_c'])

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


if __name__ == '__main__':
    r = select_div("J00", ('외래', '입원'), "강원도", 2020)
    rr = select_all_loc_div("J00", ('외래', '입원'), 2020)
    print(r)
    print(rr)
