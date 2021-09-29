import pandas as pd
import datetime
import pymysql
import numpy as np

from flask import Flask, request
from flask_cors import CORS
from login.info import info

# Flask
app = Flask(__name__)
CORS(app, resources={r'*': {'origins': '*'}})
app.config['CORS_HEADERS'] = 'Content-Type'

login = info

# connection
connection = pymysql.connect(host=login['host'], port=login['port'],
                             user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])

# cursor
cur = connection.cursor()

lpad_niin = " update {0} set {1} = lpad({1} , 9 , '0') "


@app.route('/ddrug', methods=['POST'])
def ddrug():
    print('Enter the Ddrug')

    test = request.get_json("testJson")

    if request.method == 'POST':
        nowYears = list(test.values())[0]
        fileLocation = list(test.values())[1]

    print('@@@@@@@@@@@@@')
    print(nowYears)
    print('@@@@@@@@@@@@@')
    print(fileLocation)
    print(' Step 1 Done ')

    # TODO : 전달받은 데이터에 값에 comma가 찍혀있으므로, 이후 데이터도 같은 방법으로 들어와야함
    locationString = r'C:\UploadData\{}'.format(fileLocation)

    print(locationString)

    print(' Step 2 Done ')

    ddrug = pd.read_excel(locationString, thousands=',', dtype=str)

    ddrug.columns = ['fsc', 'niin', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                     'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11']

    # ddrug_fsc = ddrug['fsc']

    # ddrug.drop('fsc', inplace=True, axis=1)

    ddrug = ddrug.groupby(['fsc', 'niin', 'niin_name', 'price']).agg(
        lambda x: pd.to_numeric(x, errors='coerce').sum()).reset_index()

    ddrug['type'] = 'drug'
    ddrug['x_year'] = datetime.date(int(nowYears), 12, 1)

    print('===================================')
    print(ddrug.head(5))

    # TODO: 웹에서 기준년도 가져온 값을 nowYear에 assign

    ddrug = ddrug[['fsc', 'niin', 'type', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                   'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11', 'x_year']]

    ddrug.columns = ['fsc', 'niin', 'type', 'niin_name', 'price', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4',
                     'x_minus_5',
                     'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']

    #### update PTYPE ####
    update_ptype_drug = ddrug[['fsc', 'niin', 'type', 'niin_name', 'price', 'x_year']]
    update_ptype_drug.rename(columns={'type': 'niin_type'}, inplace=True)
    update_ptype_drug = update_ptype_drug.replace({np.nan: None})

    print('===================================')
    print(update_ptype_drug)

    for i, row in update_ptype_drug.iterrows():
        sql = "update `ptype` set fsc = %s , niin_type = %s , niin_name = %s , price = %s where niin = %s and year = %s"
        print(sql, tuple(row))
        cur.execute(sql, (tuple(row)[0], tuple(row)[2], tuple(row)[3], tuple(row)[4], tuple(row)[1], tuple(row)[5]))
        connection.commit()

    cur.execute(lpad_niin.format('test_ptype', 'niin'))
    connection.commit()
    #########################
    print('done')

    print(ddrug.head(5))

    ddrug = ddrug[['type', 'fsc', 'niin', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4', 'x_minus_5',
                   'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']]

    # ddrug.columns = ['fsc', 'niin', 'x_minus_1', 'x_minus_2', 'x_minus_3',
    #                  'x_minus_4', 'x_minus_5',
    #                  'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']

    # 년도가 같고, type이 같고 , niin이 같으면 update(replace) , 다른게 있으면 insert =>upsert , replace

    cols = "`,`".join([str(i) for i in ddrug.columns.tolist()])

    ddrug = ddrug.replace({np.nan: None})
    print(ddrug)

    # ddrug.to_csv('demand_drug.csv' , encoding ='CP949' , index = False)

    for i, row in ddrug.iterrows():
        sql = "INSERT INTO `demand` (`" + cols + "`) VALUES (" + "%s," * (
                len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
              + "x_minus_1 = " + str(tuple(row)[3]) + " , " \
              + "x_minus_2 = " + str(tuple(row)[4]) + " , " \
              + "x_minus_3 = " + str(tuple(row)[5]) + " , " \
              + "x_minus_4 = " + str(tuple(row)[6]) + " , " \
              + "x_minus_5 = " + str(tuple(row)[7]) + " , " \
              + "x_minus_6 = " + str(tuple(row)[8]) + " , " \
              + "x_minus_7 = " + str(tuple(row)[9]) + " , " \
              + "x_minus_8 = " + str(tuple(row)[10]) + " , " \
              + "x_minus_9 = " + str(tuple(row)[11]) + " , " \
              + "x_minus_10 = " + str(tuple(row)[12]) + " , " \
              + "x_minus_11 = " + str(tuple(row)[13])

        print(sql)
        print(tuple(row))
        cur.execute(sql, tuple(row))
        connection.commit()

    cur.execute(lpad_niin.format('demand', 'niin'))
    connection.commit()

    cur.execute('update mngDate set ddrug = now()')
    connection.commit()

    print('Done')
    return 'DONE'

    # sys.exit(0)


@app.route('/dgoods', methods=['POST'])
def dgoods():
    print('Enter the Dgoods')

    test = request.get_json("testJson")

    if request.method == 'POST':
        nowYears = list(test.values())[0]
        fileLocation = list(test.values())[1]

    print('@@@@@@@@@@@@@')
    print(nowYears)
    print('@@@@@@@@@@@@@')
    print(fileLocation)
    print(' Step 1 Done ')

    # TODO : 전달받은 데이터에 값에 comma가 찍혀있으므로, 이후 데이터도 같은 방법으로 들어와야함
    locationString = r'C:\UploadData\{}'.format(fileLocation)

    print(locationString)

    print(' Step 2 Done ')

    ddrug = pd.read_excel(locationString, thousands=',', dtype=str)

    ddrug.columns = ['fsc', 'niin', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                     'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11']

    # ddrug_fsc = ddrug['fsc']

    # ddrug.drop('fsc', inplace=True, axis=1)

    ddrug = ddrug.groupby(['fsc', 'niin', 'niin_name', 'price']).agg(
        lambda x: pd.to_numeric(x, errors='coerce').sum()).reset_index()

    ddrug['type'] = 'goods'
    ddrug['x_year'] = datetime.date(int(nowYears), 12, 1)

    print('===================================')
    print(ddrug.head(5))

    # TODO: 웹에서 기준년도 가져온 값을 nowYear에 assign

    ddrug = ddrug[['fsc', 'niin', 'type', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                   'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11', 'x_year']]

    ddrug.columns = ['fsc', 'niin', 'type', 'niin_name', 'price', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4',
                     'x_minus_5',
                     'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']

    #### update PTYPE ####
    update_ptype_drug = ddrug[['fsc', 'niin', 'type', 'niin_name', 'price', 'x_year']]
    update_ptype_drug.rename(columns={'type': 'niin_type'}, inplace=True)
    update_ptype_drug = update_ptype_drug.replace({np.nan: None})

    print('===================================')
    print(update_ptype_drug)

    for i, row in update_ptype_drug.iterrows():
        sql = "update `ptype` set fsc = %s , niin_type = %s , niin_name = %s , price = %s where niin = %s and year = %s"
        print(sql, tuple(row))
        cur.execute(sql, (tuple(row)[0], tuple(row)[2], tuple(row)[3], tuple(row)[4], tuple(row)[1], tuple(row)[5]))
        connection.commit()

    cur.execute(lpad_niin.format('test_ptype', 'niin'))
    connection.commit()
    #########################
    print('done')

    print(ddrug.head(5))

    ddrug = ddrug[['type', 'fsc', 'niin', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4', 'x_minus_5',
                   'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']]

    # ddrug.columns = ['fsc', 'niin', 'x_minus_1', 'x_minus_2', 'x_minus_3',
    #                  'x_minus_4', 'x_minus_5',
    #                  'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']

    # 년도가 같고, type이 같고 , niin이 같으면 update(replace) , 다른게 있으면 insert =>upsert , replace

    cols = "`,`".join([str(i) for i in ddrug.columns.tolist()])

    ddrug = ddrug.replace({np.nan: None})
    print(ddrug)

    # ddrug.to_csv('demand_drug.csv' , encoding ='CP949' , index = False)

    for i, row in ddrug.iterrows():
        sql = "INSERT INTO `demand` (`" + cols + "`) VALUES (" + "%s," * (
                len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
              + "x_minus_1 = " + str(tuple(row)[3]) + " , " \
              + "x_minus_2 = " + str(tuple(row)[4]) + " , " \
              + "x_minus_3 = " + str(tuple(row)[5]) + " , " \
              + "x_minus_4 = " + str(tuple(row)[6]) + " , " \
              + "x_minus_5 = " + str(tuple(row)[7]) + " , " \
              + "x_minus_6 = " + str(tuple(row)[8]) + " , " \
              + "x_minus_7 = " + str(tuple(row)[9]) + " , " \
              + "x_minus_8 = " + str(tuple(row)[10]) + " , " \
              + "x_minus_9 = " + str(tuple(row)[11]) + " , " \
              + "x_minus_10 = " + str(tuple(row)[12]) + " , " \
              + "x_minus_11 = " + str(tuple(row)[13])

        print(sql)
        print(tuple(row))
        cur.execute(sql, tuple(row))
        connection.commit()

    cur.execute(lpad_niin.format('demand', 'niin'))
    connection.commit()

    cur.execute('update mngDate set dgoods = now()')
    connection.commit()

    print('Done')
    return 'DONE'
