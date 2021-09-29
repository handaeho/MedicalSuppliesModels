import pandas as pd
import pymysql
from login.info import info

login = info


def select_table(hosp_div, in_out, location, diag_code):
    conn = pymysql.connect(host=login['host'], port=login['port'],
                           user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])
    cur = conn.cursor()

    try:
        query = "select hosp_div, dates, in_out, diag_code, org, sum(counts) as counts " \
                " from prescription_dataset pd " \
                " where hosp_div in {} and in_out in {} and org in (select org_code from org_group og " \
                " where location = '{}') and diag_code = '{}' " \
                " group by dates"

        query_string = query.format(hosp_div, in_out, location, diag_code)

        cur.execute(query_string)

        conn.commit()

        row = []
        for r in cur:
            row.append(r)

        df = pd.DataFrame(row, columns=['hosp_div', 'dates', 'in_out', 'diag_code', 'org', 'counts'])

        df['years'] = df['dates'].apply(lambda x: int(str(x)[:4]))

    finally:
        cur.close()
        conn.close()

    return df
