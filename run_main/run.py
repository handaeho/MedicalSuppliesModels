import pandas as pd
import datetime
import pymysql
import numpy as np
import os

from datetime import timedelta
from flask import Flask, request, session
from flask_core import CORS

from demand_main.demand_main_lstm import DemandRunLSTM
from demand_main.demand_main_arima import DemandRunARIMA
from demand_main.demand_main_arma import DemandRunARMA
from demand_main.demand_main_average import DemandRunAverage

from consumption_main.consumption_main_lstm import ConsumptionRunLSTM
from consumption_main.consumption_main_arima import ConsumptionRunARIMA
from consumption_main.consumption_main_arma import ConsumptionRunARMA
from consumption_main.consumption_main_avg import ConsumptionRunAvg

from patient_trend_main.trend_main_arima import PatientTrendRunARIMA
from patient_trend_main.trend_main_arma import PatientTrendRunARMA
from patient_trend_main.trend_main_lstm import PatientTrendRunLSTM
from patient_trend_main.trend_main_avg import PatientTrendRunAvg

from eval.correlation import Correlation

from encrypt.encrypt_decrypt import AesEncrypt

from login.info import info

# Flask
app = Flask(__name__)
CORS(app, resources={r'*': {'origins': '*'}})
app.config['CORS_HEADERS'] = 'Content-Type'

login = info

lpad_niin = " update {0} set {1} = lpad({1}, 9 ,'0') "

# connection
connection = pymysql.connect(host=login['host'], port=login['port'],
                             user=login['user'], passwd=login['passwd'], db=login['db'], charset=login['charset'])

# cursor
cur = connection.cursor()


@app.before_request
def make_session_permanent():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=100)


@app.route('/shutdown', methods=['GET'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return "SERVER Shutdown..."


@app.route('/demand/demand_run_lstm_drug', methods=['GET', 'POST'])
def main_run_lstm_drug():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_lstm = DemandRunLSTM()
        encrypt = AesEncrypt()

        result = run_lstm.run_lstm_drug(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_lstm_goods', methods=['GET', 'POST'])
def main_run_lstm_goods():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_lstm = DemandRunLSTM()
        encrypt = AesEncrypt()

        result = run_lstm.run_lstm_goods(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_arima_drug', methods=['GET', 'POST'])
def main_run_arima_drug():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_arima = DemandRunARIMA()
        encrypt = AesEncrypt()

        result = run_arima.run_arima_drug(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_arima_goods', methods=['GET', 'POST'])
def main_run_arima_goods():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_arima = DemandRunARIMA()
        encrypt = AesEncrypt()

        result = run_arima.run_arima_goods(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_arma_drug', methods=['GET', 'POST'])
def main_run_arma_drug():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_arma = DemandRunARMA()
        encrypt = AesEncrypt()

        result = run_arma.run_arma_drug(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_arma_goods', methods=['GET', 'POST'])
def main_run_arma_goods():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_arma = DemandRunARMA()
        encrypt = AesEncrypt()

        result = run_arma.run_arma_goods(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_avg_drug', methods=['GET', 'POST'])
def main_run_avg_drug():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_avg = DemandRunAverage()
        encrypt = AesEncrypt()

        result = run_avg.run_calculate_avg_drug(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_avg_goods', methods=['GET', 'POST'])
def main_run_avg_goods():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_avg = DemandRunAverage()
        encrypt = AesEncrypt()

        result = run_avg.run_calculate_avg_goods(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_ma_drug', methods=['GET', 'POST'])
def main_run_ma_drug():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_avg = DemandRunAverage()
        encrypt = AesEncrypt()

        result = run_avg.run_calculate_ma_drug(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_ma_goods', methods=['GET', 'POST'])
def main_run_ma_goods():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_avg = DemandRunAverage()
        encrypt = AesEncrypt()

        result = run_avg.run_calculate_ma_goods(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_ols_drug', methods=['GET', 'POST'])
def main_run_ols_drug():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_avg = DemandRunAverage()
        encrypt = AesEncrypt()

        result = run_avg.run_calculate_ols_drug(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/demand/demand_run_ols_goods', methods=['GET', 'POST'])
def main_run_ols_goods():
    niin_list = request.get_json('niinCodeList')

    if request.method == 'POST':
        niin = list(niin_list.values())[0]
        year = list(niin_list.values())[1]
        password = list(niin_list.values())[2]

        run_avg = DemandRunAverage()
        encrypt = AesEncrypt()

        result = run_avg.run_calculate_ols_goods(niin_list=niin, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_lstm_hosp', methods=['GET', 'POST'])
def main_run_lstm_hosp():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_lstm = ConsumptionRunLSTM()
        encrypt = AesEncrypt()

        result = run_lstm.hosp_run_lstm(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_lstm_div', methods=['GET', 'POST'])
def main_run_lstm_div():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_lstm = ConsumptionRunLSTM()
        encrypt = AesEncrypt()

        result = run_lstm.div_run_lstm(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_arima_hosp', methods=['GET', 'POST'])
def main_run_arima_hosp():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_arima = ConsumptionRunARIMA()
        encrypt = AesEncrypt()

        result = run_arima.hosp_run_arima(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_arima_div', methods=['GET', 'POST'])
def main_run_arima_div():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_arima = ConsumptionRunARIMA()
        encrypt = AesEncrypt()

        result = run_arima.div_run_arima(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_arma_hosp', methods=['GET', 'POST'])
def main_run_arma_hosp():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_arma = ConsumptionRunARMA()
        encrypt = AesEncrypt()

        result = run_arma.hosp_run_arma(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_arma_div', methods=['GET', 'POST'])
def main_run_arma_div():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_arma = ConsumptionRunARMA()
        encrypt = AesEncrypt()

        result = run_arma.div_run_arma(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_avg_hosp', methods=['GET', 'POST'])
def main_run_avg_hosp():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_avg = ConsumptionRunAvg()
        encrypt = AesEncrypt()

        result = run_avg.hosp_run_avg(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_avg_div', methods=['GET', 'POST'])
def main_run_avg_div():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_avg = ConsumptionRunAvg()
        encrypt = AesEncrypt()

        result = run_avg.div_run_avg(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_ma_hosp', methods=['GET', 'POST'])
def main_run_ma_hosp():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_ma = ConsumptionRunAvg()
        encrypt = AesEncrypt()

        result = run_ma.hosp_run_ma(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_ma_div', methods=['GET', 'POST'])
def main_run_ma_div():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_ma = ConsumptionRunAvg()
        encrypt = AesEncrypt()

        result = run_ma.div_run_ma(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_ols_hosp', methods=['GET', 'POST'])
def main_run_ols_hosp():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_ols = ConsumptionRunAvg()
        encrypt = AesEncrypt()

        result = run_ols.hosp_run_ols(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/consumption/consumption_run_ols_div', methods=['GET', 'POST'])
def main_run_ols_div():
    drugcode_list = request.get_json('drugCodeList')

    if request.method == 'POST':
        drugcode = list(drugcode_list.values())[0]
        year = list(drugcode_list.values())[1]
        password = list(drugcode_list.values())[2]

        run_ols = ConsumptionRunAvg()
        encrypt = AesEncrypt()

        result = run_ols.div_run_ols(drugcode_list=drugcode, year=year)

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arima_all_loc_hosp', methods=['GET', 'POST'])
def main_run_arima_trend_all_loc_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        airma = PatientTrendRunARIMA()
        encrypt = AesEncrypt()

        result = airma.run_arima_all_loc_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arima_all_loc_div', methods=['GET', 'POST'])
def main_run_arima_trend_all_loc_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        airma = PatientTrendRunARIMA()
        encrypt = AesEncrypt()

        result = airma.run_arima_all_loc_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arima_hosp', methods=['GET', 'POST'])
def main_run_arima_trend_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        airma = PatientTrendRunARIMA()
        encrypt = AesEncrypt()

        result = airma.run_arima_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arima_div', methods=['GET', 'POST'])
def main_run_arima_trend_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        airma = PatientTrendRunARIMA()
        encrypt = AesEncrypt()

        result = airma.run_arima_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arma_all_loc_hosp', methods=['GET', 'POST'])
def main_run_arma_trend_all_loc_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        arma = PatientTrendRunARMA()
        encrypt = AesEncrypt()

        result = arma.run_arma_all_loc_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arma_all_loc_div', methods=['GET', 'POST'])
def main_run_arma_trend_all_loc_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        arma = PatientTrendRunARMA()
        encrypt = AesEncrypt()

        result = arma.run_arma_all_loc_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arma_hosp', methods=['GET', 'POST'])
def main_run_arma_trend_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        arma = PatientTrendRunARMA()
        encrypt = AesEncrypt()

        result = arma.run_arma_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_arma_div', methods=['GET', 'POST'])
def main_run_arma_trend_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        arma = PatientTrendRunARMA()
        encrypt = AesEncrypt()

        result = arma.run_arma_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_lstm_all_loc_hosp', methods=['GET', 'POST'])
def main_run_lstm_trend_all_loc_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        lstm = PatientTrendRunLSTM()
        encrypt = AesEncrypt()

        result = lstm.run_lstm_all_loc_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_lstm_all_loc_div', methods=['GET', 'POST'])
def main_run_lstm_trend_all_loc_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        lstm = PatientTrendRunLSTM()
        encrypt = AesEncrypt()

        result = lstm.run_lstm_all_loc_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_lstm_hosp', methods=['GET', 'POST'])
def main_run_lstm_trend_hosp():

    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        lstm = PatientTrendRunLSTM()
        encrypt = AesEncrypt()

        result = lstm.run_lstm_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_lstm_div', methods=['GET', 'POST'])
def main_run_lstm_trend_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        lstm = PatientTrendRunLSTM()
        encrypt = AesEncrypt()

        result = lstm.run_lstm_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_avg_all_loc_hosp', methods=['GET', 'POST'])
def main_run_avg_trend_all_loc_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_avg_all_loc_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_avg_all_loc_div', methods=['GET', 'POST'])
def main_run_avg_trend_all_loc_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_avg_all_loc_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_avg_hosp', methods=['GET', 'POST'])
def main_run_avg_trend_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_avg_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_avg_div', methods=['GET', 'POST'])
def main_run_avg_trend_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_avg_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ma_all_loc_hosp', methods=['GET', 'POST'])
def main_run_ma_trend_all_loc_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ma_all_loc_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ma_all_loc_div', methods=['GET', 'POST'])
def main_run_ma_trend_all_loc_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ma_all_loc_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ma_hosp', methods=['GET', 'POST'])
def main_run_ma_trend_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ma_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ma_div', methods=['GET', 'POST'])
def main_run_ma_trend_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ma_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ols_all_loc_hosp', methods=['GET', 'POST'])
def main_run_ols_trend_all_loc_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ols_all_loc_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ols_all_loc_div', methods=['GET', 'POST'])
def main_run_ols_trend_all_loc_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        year = list(select_list.values())[2]
        password = list(select_list.values())[3]

        userselect.append([diag, inout, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ols_all_loc_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ols_hosp', methods=['GET', 'POST'])
def main_run_ols_trend_hosp():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ols_hosp(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/trend/trend_run_ols_div', methods=['GET', 'POST'])
def main_run_ols_trend_div():
    select_list = request.get_json("userSelect")

    userselect = []

    if request.method == 'POST':
        diag = list(select_list.values())[0]
        inout = list(select_list.values())[1]
        loc = list(select_list.values())[2]
        year = list(select_list.values())[3]
        password = list(select_list.values())[4]

        userselect.append([diag, inout, loc, year])

        avg = PatientTrendRunAvg()
        encrypt = AesEncrypt()

        result = avg.run_ols_div(userselect=userselect[0])

        en_result = encrypt.encrypt(result)
        de_result = encrypt.decrypt(en_result, pw=password)

        return de_result

    else:
        return 'Plz change request method to "POST" and try again'


@app.route('/correlation/correlation_run', methods=['GET', 'POST'])
def main_run_correlation():
    select_list = request.get_json("weatherQuantityList")

    if request.method == 'POST':
        celsius_list = list(select_list.values())[0]
        rain_list = list(select_list.values())[1]
        dust_list = list(select_list.values())[2]
        quantity_list = list(select_list.values())[3]

        corr = Correlation()

        result = corr.corr(celsius_list=celsius_list, rain_list=rain_list,
                           dust_list=dust_list, quantity_list=quantity_list)

        return result

    else:
        return 'Plz change request method to "POST" and try again'


# TODO 데이터 관리 파트 시작

# Init Engine
from sqlalchemy import create_engine


@app.route('/prescriptionhosp', methods=['POST'])
def prescription_dataset_hosp():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        db_data = 'mysql+pymysql://' + 'powername411' + ':' + 'lightofSTAR95!' + '@' + 'localhost' + ':3306/' \
                  + 'army' + '?charset=utf8mb4'

        engine = create_engine(db_data)
        # Preprocessing dataset ( prescription_hosp )

        print('Enter the prescription_hosp')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            fileLocation = list(InputJson.values())[1]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {}, file : {} ".format(nowYears, fileLocation))

        # sys.exit(0)

        hosp = pd.read_csv(locationString, encoding='cp949')

        # print(hosp)

        # 원본데이터에서만 사용 !
        # Add col; '년' , '월'
        # hosp[['년', '월']] = hosp.년월.str.split("-", expand=True)

        # regex '월'
        # 05월 => 5월 for datetime Format
        # hosp['월'] = hosp.월.str.replace('^0*', '')

        hosp = hosp[

            ['년', '월', '입원외래구분', '진료과', '진단코드', '약품코드', '약품재고단위코드명',
             '조제불출건수', '조제불출수량', '청구불출건수', '청구불출량', 'DC반납건수', 'DC반납수량',
             '잉여반납건수', '잉여반납수량', '비식별_병원']

        ]

        hosp['실사용량'] = hosp.조제불출수량 + hosp.청구불출량 - hosp.DC반납수량 - hosp.잉여반납수량
        hosp['실건수'] = hosp.조제불출건수 + hosp.청구불출건수 - hosp.DC반납건수 - hosp.잉여반납건수
        hosp = hosp[['년', '월', '입원외래구분', '진단코드', '약품코드', '비식별_병원', '실사용량', '실건수']]

        hosp['년'] = hosp.년.astype(str)
        hosp['월'] = hosp.월.astype(str)
        hosp["날짜"] = pd.to_datetime(hosp["년"] + hosp["월"], format='%Y%m')

        hosp = hosp[['날짜', '년', '월', '입원외래구분', '진단코드', '약품코드', '비식별_병원', '실사용량', '실건수']]
        hosp.columns = ['dates', 'year', 'month', 'in_out', 'diag_code', 'drug_code', 'org', 'quantity', 'counts']

        hosp = hosp[hosp['year'] != 2012]

        # Load Origin DB; 이전 데이터 가져와서 max(Dates) 구함
        sql = "select max(dates) as dates from `prescription_dataset_hosp` "

        orginDB = pd.read_sql(sql, connection)
        orginDB['dates'] = pd.to_datetime(orginDB['dates'], format="%Y-%m-%d")
        maxDate = orginDB.iloc[0, 0]

        print(hosp)
        print(maxDate)

        # print(orginDB.dtypes)
        # print(orginDB.iloc[0,0])

        # TODO: 모든 datetime에 대해 TEST
        if maxDate is pd.NaT:
            print('DB is empty, Insert new data now ...')
            hosp.to_sql('prescription_dataset_hosp', engine, if_exists='append', index=False)
            print('######### Insert This #############')
            print(hosp)
            print('####################################')

        else:
            # Filter by maxDate
            # TODO : check gt ? gte ?
            hosp = hosp[hosp['dates'] > maxDate]

            # Execute the to_sql for writting DF into SqL
            hosp.to_sql('prescription_dataset_hosp', engine, if_exists='append', index=False)
            print('######### Insert This #############')
            print(hosp)
            print('####################################')

        strr = "Updated {} Rows".format(len(hosp))
        print(strr)

        def makeSumqc():

            ##############sumqc###################

            prescription_hosp = hosp[hosp['year'] != 2012]

            # fixed 1223
            prescription_hosp['year'] = prescription_hosp.year.astype('int')
            prescription_hosp['month'] = prescription_hosp.month.astype('int')

            # TEST
            # prescription_hosp = prescription_hosp.drop(prescription_hosp[(prescription_hosp['year'] == 2019) & (prescription_hosp['month'] == 12)].index)
            # prescription_hosp = prescription_hosp.drop(prescription_hosp[(prescription_hosp['year'] == 2019) & (prescription_hosp['month'] == 11)].index)

            # print(prescription_hosp)
            # print('Delete 12m DATA : {}'.format(len(prescription_hosp)))

            prescription_hosp = prescription_hosp[prescription_hosp['year'] < nowYears]

            maxYear = prescription_hosp['year'].max()

            numofMonth = len(prescription_hosp[prescription_hosp['year'] == int(maxYear)].month.value_counts().keys().tolist())

            addMonth = 12 - numofMonth
            if addMonth >= 1:
                addhosp = prescription_hosp[prescription_hosp['year'] == (int(maxYear) - 1)]
                mon = sorted(list(addhosp.month.value_counts().keys()), reverse=True)[:addMonth]
                addhosp = addhosp[addhosp['month'].isin(mon)]

                print('#################################################################')
                print(addhosp)
                print('#################################################################')
                addhosp['year'] = addhosp['year'].apply(lambda x: x + 1)
                prescription_hosp = pd.concat([prescription_hosp, addhosp], axis=0).reset_index()

            # print("maxYear : {}, numofMonth : {}, addMonth  : {} ".format(maxYear ,numofMonth , addMonth))

            prescription_hosp['year'] = prescription_hosp['year'].apply(lambda x: 'X_Minus_' + str((int(nowYears) - int(x))))

            prescription_hosp_c = prescription_hosp[['drug_code', 'org', 'counts', 'year']]
            prescription_hosp_q = prescription_hosp[['drug_code', 'org', 'quantity', 'year']]
            prescription_hosp_c_dg = prescription_hosp[['in_out', 'diag_code', 'drug_code', 'org', 'counts', 'year']]
            prescription_hosp_q_dg = prescription_hosp[['in_out', 'diag_code', 'drug_code', 'org', 'quantity', 'year']]

            #  사용건수 #
            prescription_hosp_c_grouped = prescription_hosp_c.groupby(['drug_code', 'org', 'year'])
            pre_hosp_csum = prescription_hosp_c_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            hosp_c_cols = pre_hosp_csum.columns.get_level_values(1).tolist()
            hosp_c_cols[0] = 'drug_code'
            hosp_c_cols[1] = 'org'
            pre_hosp_csum.columns = hosp_c_cols

            # diag sumqc 사용건수
            prescription_hosp_c_dg_grouped = prescription_hosp_c_dg.groupby(['in_out', 'diag_code', 'drug_code', 'org', 'year'])
            pre_hosp_csum_dg = prescription_hosp_c_dg_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            hosp_c_dg_cols = pre_hosp_csum_dg.columns.get_level_values(1).tolist()
            hosp_c_dg_cols[0] = 'in_out'
            hosp_c_dg_cols[1] = 'diag_code'
            hosp_c_dg_cols[2] = 'drug_code'
            hosp_c_dg_cols[3] = 'org'
            pre_hosp_csum_dg.columns = hosp_c_dg_cols

            # 사용량 #
            prescription_hosp_q_grouped = prescription_hosp_q.groupby(['drug_code', 'org', 'year'])
            pre_hosp_qsum = prescription_hosp_q_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            hosp_cols = pre_hosp_qsum.columns.get_level_values(1).tolist()
            hosp_cols[0] = 'drug_code'
            hosp_cols[1] = 'org'
            pre_hosp_qsum.columns = hosp_cols

            # diag sumqc 사용량
            prescription_hosp_q_dg_grouped = prescription_hosp_q_dg.groupby(['in_out', 'diag_code', 'drug_code', 'org', 'year'])
            pre_hosp_qsum_dg = prescription_hosp_q_dg_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            hosp_dg_cols = pre_hosp_qsum_dg.columns.get_level_values(1).tolist()
            hosp_dg_cols[0] = 'in_out'
            hosp_dg_cols[1] = 'diag_code'
            hosp_dg_cols[2] = 'drug_code'
            hosp_dg_cols[3] = 'org'
            pre_hosp_qsum_dg.columns = hosp_dg_cols

            pre_hosp_qcsum = pd.merge(left=pre_hosp_qsum, right=pre_hosp_csum, how='inner', on=['drug_code', 'org'],
                                      suffixes=['_Q', '_C'])

            pre_hosp_qcsum_dg = pd.merge(left=pre_hosp_qsum_dg, right=pre_hosp_csum_dg, how='inner', on=['in_out', 'diag_code', 'drug_code', 'org'],
                                         suffixes=['_Q', '_C'])

            # diag_sumqc 위해서 org_group과 join 필요
            sql = "select org_code, location from `org_group` "

            org_group = pd.read_sql(sql, connection)

            pre_hosp_qcsum_dg_loc = pd.merge(left=pre_hosp_qcsum_dg, right=org_group, left_on='org', right_on='org_code', how='left')
            print(pre_hosp_qcsum_dg_loc)
            pre_hosp_qcsum_dg_loc.drop('org_code', axis=1, inplace=True)
            print(pre_hosp_qcsum_dg_loc)
            pre_hosp_qcsum_dg_loc.rename(columns={'location': 'loc'}, inplace=True)
            pre_hosp_qcsum_dg_loc.rename(columns={'org': 'org_code'}, inplace=True)

            # pre_hosp_qcsum_dcode
            pre_hosp_qcsum_dcode = pre_hosp_qcsum.groupby('drug_code').sum().reset_index()

            # TODO: 웹에서 기준년도 가져온 값을 nowYear에 assign
            pre_hosp_qcsum['year'] = datetime.datetime(nowYears, 12, 1)
            pre_hosp_qcsum_dcode['year'] = datetime.datetime(nowYears, 12, 1)
            pre_hosp_qcsum_dg_loc['year'] = datetime.datetime(nowYears, 12, 1)

            pre_hosp_qcsum['hosp_div'] = '병원'
            pre_hosp_qcsum_dcode['hosp_div'] = '병원'
            pre_hosp_qcsum_dg_loc['hosp_div'] = '병원'

            db_cols = ['hosp_div', 'drug_code', 'org', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q', 'X_Minus_5_Q', 'X_Minus_6_Q',
                       'X_Minus_7_Q',
                       'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C', 'X_Minus_7_C', 'year']

            db_cols_dg = ['in_out', 'hosp_div', 'diag_code', 'drug_code', 'loc', 'org_code', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q',
                          'X_Minus_4_Q',
                          'X_Minus_5_Q', 'X_Minus_6_Q', 'X_Minus_7_Q',
                          'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C',
                          'X_Minus_7_C', 'year']

            addcols = [item for item in db_cols if item not in pre_hosp_qcsum.columns.tolist()]
            addcols_dcode = [item for item in db_cols if item not in pre_hosp_qcsum_dcode.columns.tolist()]
            addcols_dg = [item for item in db_cols_dg if item not in pre_hosp_qcsum_dg_loc.columns.tolist()]

            # 빈 컬럼 생성
            for i in range(0, len(addcols)):
                pre_hosp_qcsum[addcols[i]] = 0.0

            for i in range(0, len(addcols_dcode)):
                pre_hosp_qcsum_dcode[addcols_dcode[i]] = 0.0

            for i in range(0, len(addcols_dg)):
                pre_hosp_qcsum_dg_loc[addcols_dg[i]] = 0.0

            #
            # print(pre_hosp_qcsum.columns.tolist())
            # print(pre_hosp_qcsum.head(5))

            # print(pre_hosp_qcsum_dcode.head(5))
            pre_hosp_qcsum = pre_hosp_qcsum[
                ['hosp_div', 'drug_code', 'org', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q', 'X_Minus_5_Q', 'X_Minus_6_Q',
                 'X_Minus_7_Q',
                 'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C', 'X_Minus_7_C', 'year']]

            pre_hosp_qcsum_dcode = pre_hosp_qcsum_dcode[
                ['hosp_div', 'drug_code', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q', 'X_Minus_5_Q',
                 'X_Minus_6_Q', 'X_Minus_7_Q',
                 'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C', 'X_Minus_7_C',
                 'year']]

            pre_hosp_qcsum_dg_loc = pre_hosp_qcsum_dg_loc[
                ['in_out', 'hosp_div', 'diag_code', 'drug_code', 'loc', 'org_code', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q',
                 'X_Minus_5_Q', 'X_Minus_6_Q', 'X_Minus_7_Q',
                 'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C',
                 'X_Minus_7_C', 'year']]

            cols = "`,`".join([str(i) for i in pre_hosp_qcsum.columns.tolist()])
            cols_dcode = "`,`".join([str(i) for i in pre_hosp_qcsum_dcode.columns.tolist()])
            cols_dg = "`,`".join([str(i) for i in pre_hosp_qcsum_dg_loc.columns.tolist()])

            # 연산을 위한 year toString
            pre_hosp_qcsum["year"] = pre_hosp_qcsum.year.astype(str)
            pre_hosp_qcsum_dcode["year"] = pre_hosp_qcsum_dcode.year.astype(str)
            pre_hosp_qcsum_dg_loc["year"] = pre_hosp_qcsum_dg_loc.year.astype(str)

            pre_hosp_qcsum_dg_loc['loc'] = pre_hosp_qcsum_dg_loc['loc'].fillna('unknown')
            # Upsert

            # to qcsum
            for i, row in pre_hosp_qcsum.iterrows():
                sql = "INSERT INTO `prescription_sumqc` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1_Q = X_Minus_1_Q + " + str(tuple(row)[3]) \
                      + ", X_Minus_2_Q = X_Minus_2_Q + " + str(tuple(row)[4]) \
                      + ", X_Minus_3_Q = X_Minus_3_Q + " + str(tuple(row)[5]) \
                      + ", X_Minus_4_Q = X_Minus_4_Q + " + str(tuple(row)[6]) \
                      + ", X_Minus_5_Q = X_Minus_5_Q + " + str(tuple(row)[7]) \
                      + ", X_Minus_6_Q = X_Minus_6_Q + " + str(tuple(row)[8]) \
                      + ", X_Minus_7_Q = X_Minus_7_Q + " + str(tuple(row)[9]) \
                      + ", X_Minus_1_C = X_Minus_1_C + " + str(tuple(row)[10]) \
                      + ", X_Minus_2_C = X_Minus_2_C + " + str(tuple(row)[11]) \
                      + ", X_Minus_3_C = X_Minus_3_C + " + str(tuple(row)[12]) \
                      + ", X_Minus_4_C = X_Minus_4_C + " + str(tuple(row)[13]) \
                      + ", X_Minus_5_C = X_Minus_5_C + " + str(tuple(row)[14]) \
                      + ", X_Minus_6_C = X_Minus_6_C + " + str(tuple(row)[15]) \
                      + ", X_Minus_7_C = X_Minus_7_C + " + str(tuple(row)[16])

                print(sql)
                print(tuple(row))
                cur.execute(sql, tuple(row))
                connection.commit()

            # to dcode_qcsum
            for i, row in pre_hosp_qcsum_dcode.iterrows():
                sql = "INSERT INTO `prescription_dcode_sumqc` (`" + cols_dcode + "`) VALUES (" + "%s," * (
                            len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1_Q = X_Minus_1_Q + " + str(tuple(row)[2]) \
                      + ", X_Minus_2_Q = X_Minus_2_Q + " + str(tuple(row)[3]) \
                      + ", X_Minus_3_Q = X_Minus_3_Q + " + str(tuple(row)[4]) \
                      + ", X_Minus_4_Q = X_Minus_4_Q + " + str(tuple(row)[5]) \
                      + ", X_Minus_5_Q = X_Minus_5_Q + " + str(tuple(row)[6]) \
                      + ", X_Minus_6_Q = X_Minus_6_Q + " + str(tuple(row)[7]) \
                      + ", X_Minus_7_Q = X_Minus_7_Q + " + str(tuple(row)[8]) \
                      + ", X_Minus_1_C = X_Minus_1_C + " + str(tuple(row)[9]) \
                      + ", X_Minus_2_C = X_Minus_2_C + " + str(tuple(row)[10]) \
                      + ", X_Minus_3_C = X_Minus_3_C + " + str(tuple(row)[11]) \
                      + ", X_Minus_4_C = X_Minus_4_C + " + str(tuple(row)[12]) \
                      + ", X_Minus_5_C = X_Minus_5_C + " + str(tuple(row)[13]) \
                      + ", X_Minus_6_C = X_Minus_6_C + " + str(tuple(row)[14]) \
                      + ", X_Minus_7_C = X_Minus_7_C + " + str(tuple(row)[15])

                print(sql)
                print(tuple(row))
                cur.execute(sql, tuple(row))
                connection.commit()

            print(pre_hosp_qcsum_dg_loc)
            # to dg_qcsum
            for i, row in pre_hosp_qcsum_dg_loc.iterrows():
                sql = "INSERT INTO `prescription_diag_sumqc2` (`" + cols_dg + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" \
                      + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1_Q = X_Minus_1_Q + " + str(tuple(row)[6]) \
                      + ", X_Minus_2_Q = X_Minus_2_Q + " + str(tuple(row)[7]) \
                      + ", X_Minus_3_Q = X_Minus_3_Q + " + str(tuple(row)[8]) \
                      + ", X_Minus_4_Q = X_Minus_4_Q + " + str(tuple(row)[9]) \
                      + ", X_Minus_5_Q = X_Minus_5_Q + " + str(tuple(row)[10]) \
                      + ", X_Minus_6_Q = X_Minus_6_Q + " + str(tuple(row)[11]) \
                      + ", X_Minus_7_Q = X_Minus_7_Q + " + str(tuple(row)[12]) \
                      + ", X_Minus_1_C = X_Minus_1_C + " + str(tuple(row)[13]) \
                      + ", X_Minus_2_C = X_Minus_2_C + " + str(tuple(row)[14]) \
                      + ", X_Minus_3_C = X_Minus_3_C + " + str(tuple(row)[15]) \
                      + ", X_Minus_4_C = X_Minus_4_C + " + str(tuple(row)[16]) \
                      + ", X_Minus_5_C = X_Minus_5_C + " + str(tuple(row)[17]) \
                      + ", X_Minus_6_C = X_Minus_6_C + " + str(tuple(row)[18]) \
                      + ", X_Minus_7_C = X_Minus_7_C + " + str(tuple(row)[19])

                print(sql)
                print(tuple(row))
                cur.execute(sql, tuple(row))
                connection.commit()

        def makecountO():

            div_sql = "select  drug_code, year , month,  org  from prescription_dataset_div"
            div = pd.read_sql(div_sql, connection)

            hosp_sql = "select drug_code, year , month, org  from prescription_dataset_hosp"
            hosp = pd.read_sql(hosp_sql, connection)

            df = pd.concat([div, hosp])

            df = df[df['year'] != 2012]

            # fixed 1223
            df['year'] = df.year.astype('int')
            df['month'] = df.month.astype('int')

            df = df[df['year'] < nowYears]

            maxYear = df['year'].max()

            numofMonth = len(df[df['year'] == int(maxYear)]['month'].value_counts().keys().tolist())

            addMonth = 12 - numofMonth
            if addMonth >= 1:
                addhosp = df[df['year'] == (int(maxYear) - 1)]
                mon = sorted(list(addhosp.month.value_counts().keys()), reverse=True)[:addMonth]
                addhosp = addhosp[addhosp['month'].isin(mon)]

                print('#################################################################')
                print(addhosp)
                print('#################################################################')
                addhosp['year'] = addhosp['year'].apply(lambda x: x + 1)
                df = pd.concat([df, addhosp], axis=0).reset_index()

            print("maxYear : {}, numofMonth : {}, addMonth  : {} ".format(maxYear, numofMonth, addMonth))

            #

            df['year'] = df['year'].apply(lambda x: 'X_Minus_' + str((nowYears - x)))

            df_grouped = df.groupby(['drug_code', 'year'])['org'].nunique()

            df_grouped_corg = df_grouped.unstack(level=-1).fillna(0).reset_index()

            db_cols = ['seq', 'drug_code', 'X_Minus_1', 'X_Minus_2', 'X_Minus_3', 'X_Minus_4', 'X_Minus_5',
                       'X_Minus_6', 'X_Minus_7']

            addcols = [item for item in db_cols if item not in df_grouped_corg.columns.tolist()]

            for i in range(0, len(addcols)):
                df_grouped_corg[addcols[i]] = 0.0

            df_grouped_corg.drop('seq', axis=1, inplace=True)

            df_grouped_corg = df_grouped_corg.replace({np.nan: None})

            df_grouped_corg['year'] = datetime.date(nowYears, 12, 1)

            df_grouped_corg = df_grouped_corg[
                ['drug_code', 'X_Minus_1', 'X_Minus_2', 'X_Minus_3', 'X_Minus_4', 'X_Minus_5',
                 'X_Minus_6', 'X_Minus_7', 'year']]

            df_grouped_corg['hosp_div'] = '병원'

            cols = "`,`".join([str(i) for i in df_grouped_corg.columns.tolist()])

            print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            print(df_grouped_corg)

            for i, row in df_grouped_corg.iterrows():
                sql = "INSERT INTO `prescription_counto` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1 = " + str(tuple(row)[1]) + " , " \
                      + "X_Minus_2 = " + str(tuple(row)[2]) + " , " \
                      + "X_Minus_3 = " + str(tuple(row)[3]) + " , " \
                      + "X_Minus_4 = " + str(tuple(row)[4]) + " , " \
                      + "X_Minus_5 = " + str(tuple(row)[5]) + " , " \
                      + "X_Minus_6 = " + str(tuple(row)[6]) + " , " \
                      + "X_Minus_7 = " + str(tuple(row)[7])

                # print(sql)
                # print(tuple(row))
                cur.execute(sql, tuple(row))
                connection.commit()

        makeSumqc()
        makecountO()

        cur.execute('update mngDate set hosp = now()')

        connection.commit()

    except Exception as e:
        connection.rollback()
        print(e)

    finally:
        engine.dispose()
        connection.close()
        cur.close()
        return "DONE"
    ##############End of sumqc###############

    # # end of session


@app.route('/prescriptiondiv', methods=['POST'])
def prescription_dataset_div():
    try:

        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        db_data = 'mysql+pymysql://' + 'powername411' + ':' + 'lightofSTAR95!' + '@' + 'localhost' + ':3306/' \
                  + 'army' + '?charset=utf8mb4'

        engine = create_engine(db_data)

        print('Enter the prescription_div')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            fileLocation = list(InputJson.values())[1]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {},file : {} ".format(nowYears, fileLocation))

        # sys.exit(0)

        division = pd.read_csv(locationString, encoding='cp949')

        print('############')
        print(division)

        ## 원본 데이터에서만 사용 !
        # division['년'] = division['원처방년월명'].apply(lambda x: str(x)[:4])
        # division['월'] = division['원처방년월명'].apply(lambda x: str(x)[4:])
        #
        # division['월'] = division.월.str.replace('^0*', '')

        division = division[~division['약품코드번호'].str.contains("\\*")]

        division.rename(columns={
            '진료과명': '진료과',
            '내원구분명': '입원외래구분',
            '약품코드번호': '약품코드',
            '함량약품단위코드명': '약품재고단위코드명',
            '처방불출건수': '조제불출건수',
            '처방불출수량': '조제불출수량',
            '처치불출건수': '청구불출건수',
            '처치불출량': '청구불출량'

        }, inplace=True)

        division = division[
            ['년', '월', '입원외래구분', '진료과', '진단코드', '약품코드', '약품재고단위코드명',
             '조제불출건수', '조제불출수량', '청구불출건수', '청구불출량', 'DC반납건수', 'DC반납수량',
             '잉여반납건수', '잉여반납수량', '비식별_병원']
        ]

        print('############')
        print(division)

        division['실사용량'] = division.조제불출수량 + division.청구불출량 - division.DC반납수량 - division.잉여반납수량
        division['실건수'] = division.조제불출건수 + division.청구불출건수 - division.DC반납건수 - division.잉여반납건수

        division = division[['년', '월', '입원외래구분', '진단코드', '약품코드', '비식별_병원', '실사용량', '실건수']]

        division['년'] = division.년.astype(str)
        division['월'] = division.월.astype(str)

        division["날짜"] = pd.to_datetime(division["년"] + division["월"], format='%Y%m')

        division = division[['날짜', '년', '월', '입원외래구분', '진단코드', '약품코드', '비식별_병원', '실사용량', '실건수']]
        division.columns = ['dates', 'year', 'month', 'in_out', 'diag_code', 'drug_code', 'org', 'quantity', 'counts']

        # Load Origin DB
        sql = "select max(dates) as dates from `prescription_dataset_div` "

        orginDB = pd.read_sql(sql, connection)
        orginDB['dates'] = pd.to_datetime(orginDB['dates'], format="%Y-%m-%d")

        maxDate = orginDB.iloc[0, 0]

        # print(orginDB.dtypes)
        # print(orginDB.iloc[0,0])

        if maxDate is pd.NaT:
            print('DB is empty, Insert new data now ...')
            division.to_sql('prescription_dataset_div', engine, if_exists='append', index=False)
            print('######### Insert This #############')
            print(division)
            print('####################################')


        else:
            division = division[division['dates'] > maxDate]
            division.to_sql('prescription_dataset_div', engine, if_exists='append', index=False)
            print('######### Insert This #############')
            print(division)
            print('####################################')

        strr = "Updated {} Rows".format(len(division))

        # print(strr)
        def makeSumqc2():

            ##############sumqc###################

            prescription_division = division[division['year'] != 2012]

            # fixed 1223
            prescription_division['year'] = prescription_division.year.astype('int')
            prescription_division['month'] = prescription_division.month.astype('int')

            # TEST
            # prescription_hosp = prescription_hosp.drop(prescription_hosp[(prescription_hosp['year'] == 2019) & (prescription_hosp['month'] == 12)].index)
            # prescription_hosp = prescription_hosp.drop(prescription_hosp[(prescription_hosp['year'] == 2019) & (prescription_hosp['month'] == 11)].index)

            # print(prescription_hosp)
            # print('Delete 12m DATA : {}'.format(len(prescription_hosp)))

            prescription_division = prescription_division[prescription_division['year'] < nowYears]

            maxYear = prescription_division['year'].max()

            numofMonth = len(prescription_division[prescription_division['year'] == int(maxYear)]['month'].value_counts().keys().tolist())

            addMonth = 12 - numofMonth
            if addMonth >= 1:
                addhosp = prescription_division[prescription_division['year'] == (int(maxYear) - 1)]
                mon = sorted(list(addhosp.month.value_counts().keys()), reverse=True)[:addMonth]
                addhosp = addhosp[addhosp['month'].isin(mon)]

                print('#################################################################')
                print(addhosp)
                print('#################################################################')
                addhosp['year'] = addhosp['year'].apply(lambda x: x + 1)
                prescription_division = pd.concat([prescription_division, addhosp], axis=0).reset_index()

            # print("maxYear : {}, numofMonth : {}, addMonth  : {} ".format(maxYear ,numofMonth , addMonth))

            ##

            prescription_division['year'] = prescription_division['year'].apply(lambda x: 'X_Minus_' + str((int(nowYears) - int(x))))

            prescription_division_c = prescription_division[['drug_code', 'org', 'counts', 'year']]
            prescription_division_q = prescription_division[['drug_code', 'org', 'quantity', 'year']]
            prescription_division_c_dg = prescription_division[['in_out', 'diag_code', 'drug_code', 'org', 'counts', 'year']]
            prescription_division_q_dg = prescription_division[['in_out', 'diag_code', 'drug_code', 'org', 'quantity', 'year']]

            #  사용건수 #
            prescription_division_c_grouped = prescription_division_c.groupby(['drug_code', 'org', 'year'])
            pre_division_csum = prescription_division_c_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            division_c_cols = pre_division_csum.columns.get_level_values(1).tolist()
            division_c_cols[0] = 'drug_code'
            division_c_cols[1] = 'org'
            pre_division_csum.columns = division_c_cols

            # diag sumqc 사용건수
            prescription_division_c_dg_grouped = prescription_division_c_dg.groupby(['in_out', 'diag_code', 'drug_code', 'org', 'year'])
            pre_divison_csum_dg = prescription_division_c_dg_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            division_c_dg_cols = pre_divison_csum_dg.columns.get_level_values(1).tolist()
            division_c_dg_cols[0] = 'in_out'
            division_c_dg_cols[1] = 'diag_code'
            division_c_dg_cols[2] = 'drug_code'
            division_c_dg_cols[3] = 'org'
            pre_divison_csum_dg.columns = division_c_dg_cols

            # 사용량 #
            prescription_division_q_grouped = prescription_division_q.groupby(['drug_code', 'org', 'year'])
            pre_division_qsum = prescription_division_q_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            division_cols = pre_division_qsum.columns.get_level_values(1).tolist()
            division_cols[0] = 'drug_code'
            division_cols[1] = 'org'
            pre_division_qsum.columns = division_cols

            # diag sumqc 사용량
            prescription_division_q_dg_grouped = prescription_division_q_dg.groupby(['in_out', 'diag_code', 'drug_code', 'org', 'year'])
            pre_division_qsum_dg = prescription_division_q_dg_grouped.sum().unstack(level=-1).fillna(0).reset_index()
            division_dg_cols = pre_division_qsum_dg.columns.get_level_values(1).tolist()
            division_dg_cols[0] = 'in_out'
            division_dg_cols[1] = 'diag_code'
            division_dg_cols[2] = 'drug_code'
            division_dg_cols[3] = 'org'
            pre_division_qsum_dg.columns = division_dg_cols

            pre_division_qcsum = pd.merge(left=pre_division_qsum, right=pre_division_csum, how='inner', on=['drug_code', 'org'],
                                          suffixes=['_Q', '_C'])

            pre_division_qcsum_dg = pd.merge(left=pre_division_qsum_dg, right=pre_divison_csum_dg, how='inner',
                                             on=['in_out', 'diag_code', 'drug_code', 'org'],
                                             suffixes=['_Q', '_C'])

            # diag_sumqc 위해서 org_group과 join 필요
            sql = "select org_code, location from `org_group` "

            org_group = pd.read_sql(sql, connection)

            pre_division_qcsum_dg_loc = pd.merge(left=pre_division_qcsum_dg, right=org_group, left_on='org', right_on='org_code', how='left')

            pre_division_qcsum_dg_loc.drop('org_code', axis=1, inplace=True)

            pre_division_qcsum_dg_loc.rename(columns={'location': 'loc'}, inplace=True)
            pre_division_qcsum_dg_loc.rename(columns={'org': 'org_code'}, inplace=True)

            # pre_hosp_qcsum_dcode
            pre_division_qcsum_dcode = pre_division_qcsum.groupby('drug_code').sum().reset_index()

            # TODO: 웹에서 기준년도 가져온 값을 nowYear에 assign
            pre_division_qcsum['year'] = datetime.datetime(nowYears, 12, 1)
            pre_division_qcsum_dcode['year'] = datetime.datetime(nowYears, 12, 1)
            pre_division_qcsum_dg_loc['year'] = datetime.datetime(nowYears, 12, 1)

            pre_division_qcsum['hosp_div'] = '사단'
            pre_division_qcsum_dcode['hosp_div'] = '사단'
            pre_division_qcsum_dg_loc['hosp_div'] = '사단'

            db_cols = ['hosp_div', 'drug_code', 'org', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q', 'X_Minus_5_Q', 'X_Minus_6_Q',
                       'X_Minus_7_Q',
                       'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C', 'X_Minus_7_C', 'year']

            db_cols_dg = ['in_out', 'hosp_div', 'diag_code', 'drug_code', 'loc', 'org_code', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q',
                          'X_Minus_4_Q',
                          'X_Minus_5_Q', 'X_Minus_6_Q', 'X_Minus_7_Q',
                          'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C',
                          'X_Minus_7_C', 'year']

            addcols = [item for item in db_cols if item not in pre_division_qcsum.columns.tolist()]
            addcols_dcode = [item for item in db_cols if item not in pre_division_qcsum_dcode.columns.tolist()]
            addcols_dg = [item for item in db_cols_dg if item not in pre_division_qcsum_dg_loc.columns.tolist()]

            # 빈 컬럼 생성
            for i in range(0, len(addcols)):
                pre_division_qcsum[addcols[i]] = 0.0

            for i in range(0, len(addcols_dcode)):
                pre_division_qcsum_dcode[addcols_dcode[i]] = 0.0

            for i in range(0, len(addcols_dg)):
                pre_division_qcsum_dg_loc[addcols_dg[i]] = 0.0

            #
            # print(pre_hosp_qcsum.columns.tolist())
            # print(pre_hosp_qcsum.head(5))

            # print(pre_hosp_qcsum_dcode.head(5))
            pre_division_qcsum = pre_division_qcsum[
                ['hosp_div', 'drug_code', 'org', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q', 'X_Minus_5_Q', 'X_Minus_6_Q',
                 'X_Minus_7_Q',
                 'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C', 'X_Minus_7_C', 'year']]

            pre_division_qcsum_dcode = pre_division_qcsum_dcode[
                ['hosp_div', 'drug_code', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q', 'X_Minus_5_Q',
                 'X_Minus_6_Q', 'X_Minus_7_Q',
                 'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C', 'X_Minus_7_C',
                 'year']]

            pre_division_qcsum_dg_loc = pre_division_qcsum_dg_loc[
                ['in_out', 'hosp_div', 'diag_code', 'drug_code', 'loc', 'org_code', 'X_Minus_1_Q', 'X_Minus_2_Q', 'X_Minus_3_Q', 'X_Minus_4_Q',
                 'X_Minus_5_Q', 'X_Minus_6_Q', 'X_Minus_7_Q',
                 'X_Minus_1_C', 'X_Minus_2_C', 'X_Minus_3_C', 'X_Minus_4_C', 'X_Minus_5_C', 'X_Minus_6_C',
                 'X_Minus_7_C', 'year']]

            cols = "`,`".join([str(i) for i in pre_division_qcsum.columns.tolist()])
            cols_dcode = "`,`".join([str(i) for i in pre_division_qcsum_dcode.columns.tolist()])
            cols_dg = "`,`".join([str(i) for i in pre_division_qcsum_dg_loc.columns.tolist()])

            # 연산을 위한 year toString
            pre_division_qcsum["year"] = pre_division_qcsum.year.astype(str)
            pre_division_qcsum_dcode["year"] = pre_division_qcsum_dcode.year.astype(str)
            pre_division_qcsum_dg_loc["year"] = pre_division_qcsum_dg_loc.year.astype(str)

            pre_division_qcsum_dg_loc['loc'] = pre_division_qcsum_dg_loc['loc'].fillna('unknown')

            # Upsert

            # to qcsum
            for i, row in pre_division_qcsum.iterrows():
                sql = "INSERT INTO `prescription_sumqc` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1_Q = X_Minus_1_Q + " + str(tuple(row)[3]) \
                      + ", X_Minus_2_Q = X_Minus_2_Q + " + str(tuple(row)[4]) \
                      + ", X_Minus_3_Q = X_Minus_3_Q + " + str(tuple(row)[5]) \
                      + ", X_Minus_4_Q = X_Minus_4_Q + " + str(tuple(row)[6]) \
                      + ", X_Minus_5_Q = X_Minus_5_Q + " + str(tuple(row)[7]) \
                      + ", X_Minus_6_Q = X_Minus_6_Q + " + str(tuple(row)[8]) \
                      + ", X_Minus_7_Q = X_Minus_7_Q + " + str(tuple(row)[9]) \
                      + ", X_Minus_1_C = X_Minus_1_C + " + str(tuple(row)[10]) \
                      + ", X_Minus_2_C = X_Minus_2_C + " + str(tuple(row)[11]) \
                      + ", X_Minus_3_C = X_Minus_3_C + " + str(tuple(row)[12]) \
                      + ", X_Minus_4_C = X_Minus_4_C + " + str(tuple(row)[13]) \
                      + ", X_Minus_5_C = X_Minus_5_C + " + str(tuple(row)[14]) \
                      + ", X_Minus_6_C = X_Minus_6_C + " + str(tuple(row)[15]) \
                      + ", X_Minus_7_C = X_Minus_7_C + " + str(tuple(row)[16])
                # print(sql)
                # print(tuple(row))
                cur.execute(sql, tuple(row))
                connection.commit()

            # to dcode_qcsum
            for i, row in pre_division_qcsum_dcode.iterrows():
                sql = "INSERT INTO `prescription_dcode_sumqc` (`" + cols_dcode + "`) VALUES (" + "%s," * (
                            len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1_Q = X_Minus_1_Q + " + str(tuple(row)[2]) \
                      + ", X_Minus_2_Q = X_Minus_2_Q + " + str(tuple(row)[3]) \
                      + ", X_Minus_3_Q = X_Minus_3_Q + " + str(tuple(row)[4]) \
                      + ", X_Minus_4_Q = X_Minus_4_Q + " + str(tuple(row)[5]) \
                      + ", X_Minus_5_Q = X_Minus_5_Q + " + str(tuple(row)[6]) \
                      + ", X_Minus_6_Q = X_Minus_6_Q + " + str(tuple(row)[7]) \
                      + ", X_Minus_7_Q = X_Minus_7_Q + " + str(tuple(row)[8]) \
                      + ", X_Minus_1_C = X_Minus_1_C + " + str(tuple(row)[9]) \
                      + ", X_Minus_2_C = X_Minus_2_C + " + str(tuple(row)[10]) \
                      + ", X_Minus_3_C = X_Minus_3_C + " + str(tuple(row)[11]) \
                      + ", X_Minus_4_C = X_Minus_4_C + " + str(tuple(row)[12]) \
                      + ", X_Minus_5_C = X_Minus_5_C + " + str(tuple(row)[13]) \
                      + ", X_Minus_6_C = X_Minus_6_C + " + str(tuple(row)[14]) \
                      + ", X_Minus_7_C = X_Minus_7_C + " + str(tuple(row)[15])

                # print(sql)
                cur.execute(sql, tuple(row))
                connection.commit()

            # to dg_qcsum
            for i, row in pre_division_qcsum_dg_loc.iterrows():
                sql = "INSERT INTO `prescription_diag_sumqc2` (`" + cols_dg + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" \
                      + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1_Q = X_Minus_1_Q + " + str(tuple(row)[6]) \
                      + ", X_Minus_2_Q = X_Minus_2_Q + " + str(tuple(row)[7]) \
                      + ", X_Minus_3_Q = X_Minus_3_Q + " + str(tuple(row)[8]) \
                      + ", X_Minus_4_Q = X_Minus_4_Q + " + str(tuple(row)[9]) \
                      + ", X_Minus_5_Q = X_Minus_5_Q + " + str(tuple(row)[10]) \
                      + ", X_Minus_6_Q = X_Minus_6_Q + " + str(tuple(row)[11]) \
                      + ", X_Minus_7_Q = X_Minus_7_Q + " + str(tuple(row)[12]) \
                      + ", X_Minus_1_C = X_Minus_1_C + " + str(tuple(row)[13]) \
                      + ", X_Minus_2_C = X_Minus_2_C + " + str(tuple(row)[14]) \
                      + ", X_Minus_3_C = X_Minus_3_C + " + str(tuple(row)[15]) \
                      + ", X_Minus_4_C = X_Minus_4_C + " + str(tuple(row)[16]) \
                      + ", X_Minus_5_C = X_Minus_5_C + " + str(tuple(row)[17]) \
                      + ", X_Minus_6_C = X_Minus_6_C + " + str(tuple(row)[18]) \
                      + ", X_Minus_7_C = X_Minus_7_C + " + str(tuple(row)[19])

                # print(sql)
                cur.execute(sql, tuple(row))
                connection.commit()

        def makecountO2():

            connection = pymysql.connect(host=login['host'], port=login['port'],
                                         user=login['user'], passwd=login['passwd'], db=login['db'],
                                         charset=login['charset'])

            div_sql = "select  drug_code, year , month, org   from prescription_dataset_div"
            div = pd.read_sql(div_sql, connection)

            hosp_sql = "select drug_code, year , month, org   from prescription_dataset_hosp"
            hosp = pd.read_sql(hosp_sql, connection)

            df = pd.concat([div, hosp])

            df = df[df['year'] != 2012]

            # fixed 1223
            df['year'] = df.year.astype('int')
            df['month'] = df.month.astype('int')

            df = df[df['year'] < nowYears]

            maxYear = df['year'].max()

            numofMonth = len(df[df['year'] == int(maxYear)]['month'].value_counts().keys().tolist())

            addMonth = 12 - numofMonth
            if addMonth >= 1:
                addhosp = df[df['year'] == (int(maxYear) - 1)]
                mon = sorted(list(addhosp.month.value_counts().keys()), reverse=True)[:addMonth]
                addhosp = addhosp[addhosp['month'].isin(mon)]

                print('#################################################################')
                print(addhosp)
                print('#################################################################')
                addhosp['year'] = addhosp['year'].apply(lambda x: x + 1)
                df = pd.concat([df, addhosp], axis=0).reset_index()

            # print("maxYear : {}, numofMonth : {}, addMonth  : {} ".format(maxYear ,numofMonth , addMonth))

            ##

            df['year'] = df['year'].apply(lambda x: 'X_Minus_' + str((nowYears - x)))

            df_grouped = df.groupby(['drug_code', 'year'])['org'].nunique()

            df_grouped_corg = df_grouped.unstack(level=-1).fillna(0).reset_index()

            db_cols = ['seq', 'drug_code', 'X_Minus_1', 'X_Minus_2', 'X_Minus_3', 'X_Minus_4', 'X_Minus_5',
                       'X_Minus_6', 'X_Minus_7']

            addcols = [item for item in db_cols if item not in df_grouped_corg.columns.tolist()]

            for i in range(0, len(addcols)):
                df_grouped_corg[addcols[i]] = 0.0

            df_grouped_corg.drop('seq', axis=1, inplace=True)

            df_grouped_corg = df_grouped_corg.replace({np.nan: None})

            df_grouped_corg['year'] = datetime.date(nowYears, 12, 1)

            df_grouped_corg = df_grouped_corg[['drug_code', 'X_Minus_1', 'X_Minus_2', 'X_Minus_3', 'X_Minus_4', 'X_Minus_5',
                                               'X_Minus_6', 'X_Minus_7', 'year']]

            df_grouped_corg['hosp_div'] = '사단'

            cols = "`,`".join([str(i) for i in df_grouped_corg.columns.tolist()])

            for i, row in df_grouped_corg.iterrows():
                sql = "INSERT INTO `prescription_counto` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "X_Minus_1 = " + str(tuple(row)[1]) + " , " \
                      + "X_Minus_2 = " + str(tuple(row)[2]) + " , " \
                      + "X_Minus_3 = " + str(tuple(row)[3]) + " , " \
                      + "X_Minus_4 = " + str(tuple(row)[4]) + " , " \
                      + "X_Minus_5 = " + str(tuple(row)[5]) + " , " \
                      + "X_Minus_6 = " + str(tuple(row)[6]) + " , " \
                      + "X_Minus_7 = " + str(tuple(row)[7])

                print(sql)
                print(tuple(row))
                cur.execute(sql, tuple(row))
                connection.commit()

        makeSumqc2()
        makecountO2()

        cur.execute('update mngDate set `div` = now()')
        connection.commit()

    except Exception as e:
        connection.rollback()
        print(e)

    finally:
        engine.dispose()
        connection.close()
        cur.close()
        return "DONE"


@app.route('/centerlist', methods=['POST'])
def centerList():
    try:

        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the CenterList')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            niin_type = list(InputJson.values())[1]
            fileLocation = list(InputJson.values())[2]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {}, niin_type : {}, file : {} ".format(nowYears, niin_type, fileLocation))

        # sys.exit(0)

        # df = pd.read_excel(r'C:\Users\Gaion\Desktop\Gaion\[국방부]\데이터관리\dataset\군수사령부\중앙조달목록.xlsx' , dtype=str)
        df = pd.read_excel(locationString, dtype=str)
        df['NIIN'] = df.NIIN.astype('str')
        df['NIIN'] = df['NIIN'].apply(lambda x: x.zfill(9))

        central_niin = df.NIIN.tolist()

        # ptype의 max date를 가져오고, 이와 기준년도를 비교하여, 기준년도가 더 높으면, db 복사 , 이후 중앙조달 목록 데이터 업데이트

        ptpye_sql = "select * from ptype"
        ptype = pd.read_sql(ptpye_sql, connection)

        ptype.drop('seq', axis=1, inplace=True)
        ptype = ptype.replace({np.nan: None})

        ptype_NIIN = ptype.niin.tolist()
        diff_niin = list(set(central_niin) - set(ptype_NIIN))

        maxdate_sql = "select max(year) as dates from `ptype` "

        ptypeMD = pd.read_sql(maxdate_sql, connection)
        ptypeMD['dates'] = pd.to_datetime(ptypeMD['dates'], format="%Y-%m-%d")

        maxYear = int(str(ptypeMD.iloc[0, 0])[:4])
        print('maxYear : {} '.format(maxYear))

        # 기준년도가 기존데이터보다 더 클때 , X-1 Copy, update
        if nowYears > maxYear:
            print('Ptype Copy, Insert ')

            # 전년도 데이터만 가져와서 Copy, 년도 설정

            ptype = ptype[ptype['year'] == datetime.date(nowYears - 1, 12, 1)]

            ptype['year'] = datetime.datetime(nowYears, 12, 1)
            ptype["year"] = ptype.year.astype(str)

            # Copy & Insert
            cols = "`,`".join([str(i) for i in ptype.columns.tolist()])
            for i, row in ptype.iterrows():
                sql = "INSERT INTO `ptype` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
                cur.execute(sql, tuple(row))
                # connection.commit()

        # insert diff_niin
        # 중앙조달 목록 NIIN 중 기존 ptype에 없는 값들은 NIIN값 추가
        for i in range(0, len(diff_niin)):
            sql = "insert into `ptype` (niin, year , niin_type) values ( %s , %s , %s) "
            cur.execute(sql, (diff_niin[i], datetime.date(nowYears, 12, 1), niin_type))
            # connection.commit()

        # 부대구매 (2)로 모두 초기화
        init_types_sql = " update ptype set types = '2' where year = '{}' ".format(datetime.date(nowYears, 12, 1))
        cur.execute(init_types_sql)
        # connection.commit()

        # 중앙조달 (0) 으로 Update
        # 1126 fixed str(central_niin)
        update_sql = " update ptype set types = '0' where niin in {} and year = '{}' ".format(str(central_niin).replace('[', '(').replace(']', ')'),
                                                                                              datetime.date(nowYears, 12, 1))
        print(update_sql)
        cur.execute(update_sql)
        # connection.commit()

        update_niin_type = "update ptype set niin_type = '{}' where niin in {}".format(niin_type,
                                                                                       str(central_niin).replace('[', '(').replace(']', ')'))
        print(update_niin_type)
        cur.execute(update_niin_type)
        # connection.commit()

        update_type_temp = "update ptype set types_temp = types where year = '{}' ".format(datetime.date(nowYears, 12, 1))
        cur.execute(update_type_temp)
        # connection.commit()

        cur.execute('update mngDate set center = now()')

        connection.commit()
    except Exception as e:
        print(e)

    finally:
        cur.close()
        connection.close()
        return 'DONE'


@app.route('/mainlist', methods=['POST'])
def mainList():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the MainList')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            niin_type = list(InputJson.values())[1]
            fileLocation = list(InputJson.values())[2]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {}, niin_type : {}, file : {} ".format(nowYears, niin_type, fileLocation))

        # sys.exit(0)

        df = pd.read_excel(locationString, dtype=str)

        df['NIIN'] = df.NIIN.astype('str')
        df['NIIN'] = df['NIIN'].apply(lambda x: x.zfill(9))

        main_niin = df.NIIN.tolist()

        # ptype의 max date를 가져오고, 이와 기준년도를 비교하여, 기준년도가 더 높으면, db 복사 , 이후 중앙조달 목록 데이터 업데이트

        ptpye_sql = "select * from ptype"
        ptype = pd.read_sql(ptpye_sql, connection)

        ptype.drop('seq', axis=1, inplace=True)
        ptype = ptype.replace({np.nan: None})

        ptype_NIIN = ptype.niin.tolist()
        diff_niin = list(set(main_niin) - set(ptype_NIIN))

        print(diff_niin)

        maxdate_sql = "select max(year) as dates from `ptype` "

        ptypeMD = pd.read_sql(maxdate_sql, connection)
        ptypeMD['dates'] = pd.to_datetime(ptypeMD['dates'], format="%Y-%m-%d")

        maxYear = int(str(ptypeMD.iloc[0, 0])[:4])
        print('maxYear : {} '.format(maxYear))

        # 기준년도가 기존데이터보다 더 클때 , X-1 Copy, update
        if nowYears > maxYear:
            print('Ptype Copy, Insert ')

            # 전년도 데이터만 가져와서 Copy, 년도 설정

            ptype = ptype[ptype['year'] == datetime.date(nowYears - 1, 12, 1)]

            ptype['year'] = datetime.datetime(nowYears, 12, 1)
            ptype["year"] = ptype.year.astype(str)
            print(ptype)

            # Copy & Insert
            cols = "`,`".join([str(i) for i in ptype.columns.tolist()])
            for i, row in ptype.iterrows():
                sql = "INSERT INTO `ptype` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
                cur.execute(sql, tuple(row))
                # connection.commit()

        # insert diff_niin
        # 주공급자 목록 NIIN 중 기존 ptype에 없는 값들은 NIIN값 추가
        for i in range(0, len(diff_niin)):
            sql = "insert into `ptype` (niin, year , niin_type) values ( %s , %s , %s) "
            cur.execute(sql, (diff_niin[i], datetime.date(nowYears, 12, 1), niin_type))
            # connection.commit()

        # 주공급자에서는 초기화 하지 않음

        # # 부대구매 (2)로 모두 초기화
        # init_types_sql = " update test_ptype set types = '2' "
        # cur.execute(init_types_sql)
        # connection.commit()

        # 주공급자 (1) 으로 Update
        # 1126 fixed str(main_niin)
        update_sql = " update ptype set types = '1' where niin in {} and year = '{}'".format(str(main_niin).replace('[', '(').replace(']', ')'),
                                                                                             datetime.date(nowYears, 12, 1))
        print(update_sql)
        cur.execute(update_sql)
        # connection.commit()

        update_niin_type = "update ptype set niin_type = '{}' where niin in {}".format(niin_type, str(main_niin).replace('[', '(').replace(']', ')'))
        print(update_niin_type)
        cur.execute(update_niin_type)
        # connection.commit()

        update_type_temp = "update ptype set types_temp = types where year = '{}' ".format(datetime.date(nowYears, 12, 1))
        cur.execute(update_type_temp)
        # connection.commit()

        cur.execute('update mngDate set main = now()')

        connection.commit()
    except Exception as e:
        print(e)

    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/supplyplan', methods=['POST'])
def supplyPlan():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the SupplyPlan')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            niin_type = list(InputJson.values())[1]
            fileLocation = list(InputJson.values())[2]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {}, niin_type : {}, file : {} ".format(nowYears, niin_type, fileLocation))

        # sys.exit(0)

        df = pd.read_excel(locationString, header=[0, 1], sheet_name='Sheet1')

        print(df)
        print(locationString)

        df.columns = ['item_niin', 'item_name', 'item_unit', 'item_price', 'item_ME', 'item_ER', 'item_sOrigin',
                      'item_sLevelDay',
                      'demand_X_Minus_5', 'demand_X_Minus_4', 'demand_X_Minus_3', 'demand_X_Minus_2', 'demand_X_Minus_1',
                      'demand_period', 'demand_X_Plus_1', 'spend_X_Plus_1',
                      'spend_sLevel', 'spend_remain', 'spend_spend', 'spend_total', 'asset_now', 'asset_inPlan',
                      'asset_outPlan', 'asset_fieldDiff',
                      'asset_ttlQuantity', 'asset_price', 'difference_quantity', 'difference_price', 'X_Plus_1_quantity',
                      'X_Plus_1_price']

        df['item_niin'] = df.item_niin.astype('str')
        df['item_niin'] = df['item_niin'].apply(lambda x: x.zfill(9))

        df['x_year'] = datetime.datetime(nowYears, 12, 1)

        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        df["x_year"] = df.x_year.astype(str)

        # NULL 값 전처리 모든 컬럼 따로
        df["item_niin"].fillna('None', inplace=True)
        df["item_name"].fillna('None', inplace=True)
        df["item_unit"].fillna('None', inplace=True)
        df["item_price"].fillna(-99999999, inplace=True)
        df["item_ME"].fillna('None', inplace=True)
        df["item_ER"].fillna('None', inplace=True)
        df["item_sOrigin"].fillna('None', inplace=True)
        df["item_sLevelDay"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_5"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_4"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_3"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_2"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_1"].fillna(-99999999, inplace=True)
        df["demand_period"].fillna(-99999999, inplace=True)
        df["demand_X_Plus_1"].fillna(-99999999, inplace=True)
        df["spend_X_Plus_1"].fillna(-99999999, inplace=True)
        df["spend_sLevel"].fillna(-99999999, inplace=True)
        df["spend_remain"].fillna(-99999999, inplace=True)
        df["spend_spend"].fillna(-99999999, inplace=True)
        df["spend_total"].fillna(-99999999, inplace=True)
        df["asset_now"].fillna(-99999999, inplace=True)
        df["asset_inPlan"].fillna(-99999999, inplace=True)
        df["asset_outPlan"].fillna(-99999999, inplace=True)
        df["asset_fieldDiff"].fillna(-99999999, inplace=True)
        df["asset_ttlQuantity"].fillna(-99999999, inplace=True)
        df["asset_price"].fillna(-99999999, inplace=True)
        df["difference_quantity"].fillna(-99999999, inplace=True)
        df["difference_price"].fillna(-99999999, inplace=True)
        df["X_Plus_1_quantity"].fillna(-99999999, inplace=True)
        df["X_Plus_1_price"].fillna(-99999999, inplace=True)

        for i, row in df.iterrows():
            sql = "INSERT INTO `supply_plan` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                  + "item_name = '{}' , ".format(str(tuple(row)[1])) \
                  + "item_unit = '{}' , ".format(str(tuple(row)[2])) \
                  + "item_price = {} , ".format(str(tuple(row)[3])) \
                  + "item_ME = '{}' , ".format(str(tuple(row)[4])) \
                  + "item_ER = '{}' , ".format(str(tuple(row)[5])) \
                  + "item_sOrigin = '{}' , ".format(str(tuple(row)[6])) \
                  + "item_sLevelDay = {} , ".format(str(tuple(row)[7])) \
                  + "demand_X_Minus_5 = {} , ".format(str(tuple(row)[8])) \
                  + "demand_X_Minus_4 = {} , ".format(str(tuple(row)[9])) \
                  + "demand_X_Minus_3 = {} , ".format(str(tuple(row)[10])) \
                  + "demand_X_Minus_2 = {} , ".format(str(tuple(row)[11])) \
                  + "demand_X_Minus_1 = {} , ".format(str(tuple(row)[12])) \
                  + "demand_period = {} , ".format(str(tuple(row)[13])) \
                  + "demand_X_Plus_1 = {} , ".format(str(tuple(row)[14])) \
                  + "spend_X_Plus_1 = {} , ".format(str(tuple(row)[15])) \
                  + "spend_sLevel = {} , ".format(str(tuple(row)[16])) \
                  + "spend_remain = {} , ".format(str(tuple(row)[17])) \
                  + "spend_spend = {} , ".format(str(tuple(row)[18])) \
                  + "spend_total = {} , ".format(str(tuple(row)[19])) \
                  + "asset_now = {} , ".format(str(tuple(row)[20])) \
                  + "asset_inPlan = {} , ".format(str(tuple(row)[21])) \
                  + "asset_outPlan = {} , ".format(str(tuple(row)[22])) \
                  + "asset_fieldDiff = {} , ".format(str(tuple(row)[23])) \
                  + "asset_ttlQuantity = {} , ".format(str(tuple(row)[24])) \
                  + "asset_price = {} , ".format(str(tuple(row)[25])) \
                  + "difference_quantity = {} , ".format(str(tuple(row)[26])) \
                  + "difference_price = {} , ".format(str(tuple(row)[27])) \
                  + "X_Plus_1_quantity = {} , ".format(str(tuple(row)[28])) \
                  + "X_Plus_1_price = {} ".format(str(tuple(row)[29]))

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        update_null_sql_niin = "update supply_plan set item_niin = NULL where item_niin = 'None' "
        update_null_sql_name = "update supply_plan set item_name = NULL where item_name = 'None' "
        update_null_sql_unit = "update supply_plan set item_unit = NULL where item_unit = 'None' "
        update_null_sql_price = "update supply_plan set item_price = NULL where item_price = -99999999"
        update_null_sql_ME = "update supply_plan set item_ME = NULL where item_ME = 'None' "
        update_null_sql_ER = "update supply_plan set item_ER = NULL where item_ER = 'None' "
        update_null_sql_sOrigin = "update supply_plan set item_sOrigin = NULL where item_sOrigin = 'None' "
        update_null_sql_sLevelDay = "update supply_plan set item_sLevelDay = NULL where item_sLevelDay = -99999999"
        update_null_sql_X_5 = "update supply_plan set demand_X_Minus_5 = NULL where demand_X_Minus_5 = -99999999"
        update_null_sql_X_4 = "update supply_plan set demand_X_Minus_4 = NULL where demand_X_Minus_4 = -99999999"
        update_null_sql_X_3 = "update supply_plan set demand_X_Minus_3 = NULL where demand_X_Minus_3 = -99999999"
        update_null_sql_X_2 = "update supply_plan set demand_X_Minus_2 = NULL where demand_X_Minus_2 = -99999999"
        update_null_sql_X_1 = "update supply_plan set demand_X_Minus_1 = NULL where demand_X_Minus_1 = -99999999"
        update_null_sql_period = "update supply_plan set demand_period = NULL where demand_period = -99999999"
        update_null_sql_demand_X_p1 = "update supply_plan set demand_X_Plus_1 = NULL where demand_X_Plus_1 = -99999999"
        update_null_sql_spend_X_p1 = "update supply_plan set spend_X_Plus_1 = NULL where spend_X_Plus_1 = -99999999"
        update_null_sql_spend_sLevel = "update supply_plan set spend_sLevel = NULL where spend_sLevel = -99999999"
        update_null_sql_spend_remain = "update supply_plan set spend_remain = NULL where spend_remain = -99999999"
        update_null_sql_spend_spend = "update supply_plan set spend_spend = NULL where spend_spend = -99999999"
        update_null_sql_spend_total = "update supply_plan set spend_total = NULL where spend_total = -99999999"
        update_null_sql_asset_now = "update supply_plan set asset_now = NULL where asset_now = -99999999"
        update_null_sql_asset_inPlan = "update supply_plan set asset_inPlan = NULL where asset_inPlan = -99999999"
        update_null_sql_asset_outPlan = "update supply_plan set asset_outPlan = NULL where asset_outPlan = -99999999"
        update_null_sql_asset_fieldDiff = "update supply_plan set asset_fieldDiff = NULL where asset_fieldDiff = -99999999"
        update_null_sql_asset_ttlQuantity = "update supply_plan set asset_ttlQuantity = NULL where asset_ttlQuantity = -99999999"
        update_null_sql_asset_price = "update supply_plan set asset_price = NULL where asset_price = -99999999"
        update_null_sql_asset_difference_quantity = "update supply_plan set difference_quantity = NULL where difference_quantity = -99999999"
        update_null_sql_asset_difference_price = "update supply_plan set difference_price = NULL where difference_price = -99999999"
        update_null_sql_x_p1_q = "update supply_plan set X_Plus_1_quantity = NULL where X_Plus_1_quantity = -99999999"
        update_null_sql_x_p1_p = "update supply_plan set X_Plus_1_price = NULL where X_Plus_1_price = -99999999"

        cur.execute(update_null_sql_niin)
        cur.execute(update_null_sql_name)
        cur.execute(update_null_sql_unit)
        cur.execute(update_null_sql_price)
        cur.execute(update_null_sql_ME)
        cur.execute(update_null_sql_ER)
        cur.execute(update_null_sql_sOrigin)
        cur.execute(update_null_sql_sLevelDay)
        cur.execute(update_null_sql_X_5)
        cur.execute(update_null_sql_X_4)
        cur.execute(update_null_sql_X_3)
        cur.execute(update_null_sql_X_2)
        cur.execute(update_null_sql_X_1)
        cur.execute(update_null_sql_period)
        cur.execute(update_null_sql_demand_X_p1)
        cur.execute(update_null_sql_spend_X_p1)
        cur.execute(update_null_sql_spend_sLevel)
        cur.execute(update_null_sql_spend_remain)
        cur.execute(update_null_sql_spend_spend)
        cur.execute(update_null_sql_spend_total)
        cur.execute(update_null_sql_asset_now)
        cur.execute(update_null_sql_asset_inPlan)
        cur.execute(update_null_sql_asset_outPlan)
        cur.execute(update_null_sql_asset_fieldDiff)
        cur.execute(update_null_sql_asset_ttlQuantity)
        cur.execute(update_null_sql_asset_price)
        cur.execute(update_null_sql_asset_difference_quantity)
        cur.execute(update_null_sql_asset_difference_price)
        cur.execute(update_null_sql_x_p1_q)
        cur.execute(update_null_sql_x_p1_p)

        # connection.commit()

        # ptype update price , None을 NULL로

        sup_niin_list = df.item_niin.tolist()
        sup_price_list = df.item_price.tolist()

        print(len(sup_niin_list))
        print(len(sup_price_list))

        for i in range(0, len(sup_niin_list)):
            sql = "update ptype set price = {} where niin = '{}' ".format(sup_price_list[i], sup_niin_list[i])
            print(sql)
            cur.execute(sql)
            # connection.commit()

        cur.execute('update mngDate set sup = now()')

        connection.commit()
    except Exception as e:
        print(e)

    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/budgetplan', methods=['POST'])
def budgetPlan():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the BudgetPlan')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            fileLocation = list(InputJson.values())[1]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {}, file : {} ".format(nowYears, fileLocation))

        # sys.exit(0)

        df = pd.read_excel(locationString, header=[0, 1], sheet_name='Sheet1')

        df.columns = ['item_niin', 'item_name', 'item_unit', 'item_price', 'item_ME', 'item_ER', 'item_sOrigin',
                      'item_sLevelDay',
                      'demand_X_Minus_5', 'demand_X_Minus_4', 'demand_X_Minus_3', 'demand_X_Minus_2', 'demand_X_Minus_1',
                      'demand_period', 'demand_X_Plus_1', 'spend_X_Plus_2',
                      'spend_sLevel', 'spend_remain', 'spend_spend', 'spend_total', 'asset_now', 'asset_inPlan', 'asset_X_Plus_1',
                      'asset_outPlan', 'asset_fieldDiff',
                      'asset_ttlQuantity', 'asset_price', 'difference_quantity', 'difference_price', 'X_Plus_2_quantity',
                      'X_Plus_2_price', 'gvm_budget_quantity', 'gvm_budget_unitPrice', 'gvm_budget_price', 'X2_Minus_gvm_quantity',
                      'X2_Minus_gvm_price']

        df['item_niin'] = df.item_niin.astype('str')
        df['item_niin'] = df['item_niin'].apply(lambda x: x.zfill(9))

        df['x_year'] = datetime.datetime(nowYears, 12, 1)

        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        df["x_year"] = df.x_year.astype(str)

        df["item_niin"].fillna('None', inplace=True)
        df["item_name"].fillna('None', inplace=True)
        df["item_unit"].fillna('None', inplace=True)
        df["item_price"].fillna(-99999999, inplace=True)
        df["item_ME"].fillna('None', inplace=True)
        df["item_ER"].fillna('None', inplace=True)
        df["item_sOrigin"].fillna('None', inplace=True)
        df["item_sLevelDay"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_5"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_4"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_3"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_2"].fillna(-99999999, inplace=True)
        df["demand_X_Minus_1"].fillna(-99999999, inplace=True)
        df["demand_period"].fillna(-99999999, inplace=True)
        df["demand_X_Plus_1"].fillna(-99999999, inplace=True)
        df["spend_X_Plus_2"].fillna(-99999999, inplace=True)
        df["spend_sLevel"].fillna(-99999999, inplace=True)
        df["spend_remain"].fillna(-99999999, inplace=True)
        df["spend_spend"].fillna(-99999999, inplace=True)
        df["spend_total"].fillna(-99999999, inplace=True)
        df["asset_now"].fillna(-99999999, inplace=True)
        df["asset_inPlan"].fillna(-99999999, inplace=True)
        df["asset_X_Plus_1"].fillna(-99999999, inplace=True)
        df["asset_outPlan"].fillna(-99999999, inplace=True)
        df["asset_fieldDiff"].fillna(-99999999, inplace=True)
        df["asset_ttlQuantity"].fillna(-99999999, inplace=True)
        df["asset_price"].fillna(-99999999, inplace=True)
        df["difference_quantity"].fillna(-99999999, inplace=True)
        df["difference_price"].fillna(-99999999, inplace=True)
        df["X_Plus_2_quantity"].fillna(-99999999, inplace=True)
        df["X_Plus_2_price"].fillna(-99999999, inplace=True)
        df["gvm_budget_quantity"].fillna(-99999999, inplace=True)
        df["gvm_budget_unitPrice"].fillna(-99999999, inplace=True)
        df["gvm_budget_price"].fillna(-99999999, inplace=True)
        df["X2_Minus_gvm_quantity"].fillna(-99999999, inplace=True)
        df["X2_Minus_gvm_price"].fillna(-99999999, inplace=True)

        for i, row in df.iterrows():
            sql = "INSERT INTO `budget_plan` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                  + "item_name = '{}' , ".format(str(tuple(row)[1])) \
                  + "item_unit = '{}' , ".format(str(tuple(row)[2])) \
                  + "item_price = {} , ".format(str(tuple(row)[3])) \
                  + "item_ME = '{}' , ".format(str(tuple(row)[4])) \
                  + "item_ER = '{}' , ".format(str(tuple(row)[5])) \
                  + "item_sOrigin = '{}' , ".format(str(tuple(row)[6])) \
                  + "item_sLevelDay = {} , ".format(str(tuple(row)[7])) \
                  + "demand_X_Minus_5 = {} , ".format(str(tuple(row)[8])) \
                  + "demand_X_Minus_4 = {} , ".format(str(tuple(row)[9])) \
                  + "demand_X_Minus_3 = {} , ".format(str(tuple(row)[10])) \
                  + "demand_X_Minus_2 = {} , ".format(str(tuple(row)[11])) \
                  + "demand_X_Minus_1 = {} , ".format(str(tuple(row)[12])) \
                  + "demand_period = {} , ".format(str(tuple(row)[13])) \
                  + "demand_X_Plus_1 = {} , ".format(str(tuple(row)[14])) \
                  + "spend_X_Plus_2 = {} , ".format(str(tuple(row)[15])) \
                  + "spend_sLevel = {} , ".format(str(tuple(row)[16])) \
                  + "spend_remain = {} , ".format(str(tuple(row)[17])) \
                  + "spend_spend = {} , ".format(str(tuple(row)[18])) \
                  + "spend_total = {} , ".format(str(tuple(row)[19])) \
                  + "asset_now = {} , ".format(str(tuple(row)[20])) \
                  + "asset_inPlan = {} , ".format(str(tuple(row)[21])) \
                  + "asset_X_Plus_1 = {} , ".format(str(tuple(row)[22])) \
                  + "asset_outPlan = {} , ".format(str(tuple(row)[23])) \
                  + "asset_fieldDiff = {} , ".format(str(tuple(row)[24])) \
                  + "asset_ttlQuantity = {} , ".format(str(tuple(row)[25])) \
                  + "asset_price = {} , ".format(str(tuple(row)[26])) \
                  + "difference_quantity = {} , ".format(str(tuple(row)[27])) \
                  + "difference_price = {} , ".format(str(tuple(row)[28])) \
                  + "X_Plus_2_quantity = {} , ".format(str(tuple(row)[29])) \
                  + "X_Plus_2_price = {} , ".format(str(tuple(row)[30])) \
                  + "gvm_budget_quantity = {} , ".format(str(tuple(row)[31])) \
                  + "gvm_budget_unitPrice = {} , ".format(str(tuple(row)[32])) \
                  + "gvm_budget_price = {} , ".format(str(tuple(row)[33])) \
                  + "X2_Minus_gvm_quantity = {} , ".format(str(tuple(row)[34])) \
                  + "X2_Minus_gvm_price = {} ".format(str(tuple(row)[35]))

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        update_null_sql_niin = "update budget_plan set item_niin = NULL where item_niin = 'None' "
        update_null_sql_name = "update budget_plan set item_name = NULL where item_name = 'None' "
        update_null_sql_unit = "update budget_plan set item_unit = NULL where item_unit = 'None' "
        update_null_sql_price = "update budget_plan set item_price = NULL where item_price = -99999999"
        update_null_sql_ME = "update budget_plan set item_ME = NULL where item_ME = 'None' "
        update_null_sql_ER = "update budget_plan set item_ER = NULL where item_ER = 'None' "
        update_null_sql_sOrigin = "update budget_plan set item_sOrigin = NULL where item_sOrigin = 'None' "
        update_null_sql_sLevelDay = "update budget_plan set item_sLevelDay = NULL where item_sLevelDay = -99999999"
        update_null_sql_X_5 = "update budget_plan set demand_X_Minus_5 = NULL where demand_X_Minus_5 = -99999999"
        update_null_sql_X_4 = "update budget_plan set demand_X_Minus_4 = NULL where demand_X_Minus_4 = -99999999"
        update_null_sql_X_3 = "update budget_plan set demand_X_Minus_3 = NULL where demand_X_Minus_3 = -99999999"
        update_null_sql_X_2 = "update budget_plan set demand_X_Minus_2 = NULL where demand_X_Minus_2 = -99999999"
        update_null_sql_X_1 = "update budget_plan set demand_X_Minus_1 = NULL where demand_X_Minus_1 = -99999999"
        update_null_sql_period = "update budget_plan set demand_period = NULL where demand_period = -99999999"
        update_null_sql_demand_X_p1 = "update budget_plan set demand_X_Plus_1 = NULL where demand_X_Plus_1 = -99999999"
        update_null_sql_spend_X_p2 = "update budget_plan set spend_X_Plus_2 = NULL where spend_X_Plus_2 = -99999999"
        update_null_sql_spend_sLevel = "update budget_plan set spend_sLevel = NULL where spend_sLevel = -99999999"
        update_null_sql_spend_remain = "update budget_plan set spend_remain = NULL where spend_remain = -99999999"
        update_null_sql_spend_spend = "update budget_plan set spend_spend = NULL where spend_spend = -99999999"
        update_null_sql_spend_total = "update budget_plan set spend_total = NULL where spend_total = -99999999"
        update_null_sql_asset_now = "update budget_plan set asset_now = NULL where asset_now = -99999999"
        update_null_sql_asset_inPlan = "update budget_plan set asset_inPlan = NULL where asset_inPlan = -99999999"
        update_null_sql_asset_X_p1 = "update budget_plan set asset_outPlan = NULL where asset_X_Plus_1 = -99999999"
        update_null_sql_asset_outPlan = "update budget_plan set asset_outPlan = NULL where asset_outPlan = -99999999"
        update_null_sql_asset_fieldDiff = "update budget_plan set asset_fieldDiff = NULL where asset_fieldDiff = -99999999"
        update_null_sql_asset_ttlQuantity = "update budget_plan set asset_ttlQuantity = NULL where asset_ttlQuantity = -99999999"
        update_null_sql_asset_price = "update budget_plan set asset_price = NULL where asset_price = -99999999"
        update_null_sql_asset_difference_quantity = "update budget_plan set difference_quantity = NULL where difference_quantity = -99999999"
        update_null_sql_asset_difference_price = "update budget_plan set difference_price = NULL where difference_price = -99999999"
        update_null_sql_x_p2_q = "update budget_plan set X_Plus_2_quantity = NULL where X_Plus_2_quantity = -99999999"
        update_null_sql_x_p2_p = "update budget_plan set X_Plus_2_price = NULL where X_Plus_2_price = -99999999"
        update_null_sql_g_budget_q = "update budget_plan set gvm_budget_quantity = NULL where gvm_budget_quantity = -99999999"
        update_null_sql_g_budget_up = "update budget_plan set gvm_budget_unitPrice = NULL where gvm_budget_unitPrice = -99999999"
        update_null_sql_g_budget_p = "update budget_plan set gvm_budget_price = NULL where gvm_budget_price = -99999999"
        update_null_sql_x_m2_q = "update budget_plan set X2_Minus_gvm_quantity = NULL where X2_Minus_gvm_quantity = -99999999"
        update_null_sql_x_m2_p = "update budget_plan set X2_Minus_gvm_price = NULL where X2_Minus_gvm_price = -99999999"

        cur.execute(update_null_sql_niin)
        cur.execute(update_null_sql_name)
        cur.execute(update_null_sql_unit)
        cur.execute(update_null_sql_price)
        cur.execute(update_null_sql_ME)
        cur.execute(update_null_sql_ER)
        cur.execute(update_null_sql_sOrigin)
        cur.execute(update_null_sql_sLevelDay)
        cur.execute(update_null_sql_X_5)
        cur.execute(update_null_sql_X_4)
        cur.execute(update_null_sql_X_3)
        cur.execute(update_null_sql_X_2)
        cur.execute(update_null_sql_X_1)
        cur.execute(update_null_sql_period)
        cur.execute(update_null_sql_demand_X_p1)
        cur.execute(update_null_sql_spend_X_p2)
        cur.execute(update_null_sql_spend_sLevel)
        cur.execute(update_null_sql_spend_remain)
        cur.execute(update_null_sql_spend_spend)
        cur.execute(update_null_sql_spend_total)
        cur.execute(update_null_sql_asset_now)
        cur.execute(update_null_sql_asset_inPlan)
        cur.execute(update_null_sql_asset_X_p1)
        cur.execute(update_null_sql_asset_outPlan)
        cur.execute(update_null_sql_asset_fieldDiff)
        cur.execute(update_null_sql_asset_ttlQuantity)
        cur.execute(update_null_sql_asset_price)
        cur.execute(update_null_sql_asset_difference_quantity)
        cur.execute(update_null_sql_asset_difference_price)
        cur.execute(update_null_sql_x_p2_q)
        cur.execute(update_null_sql_x_p2_p)
        cur.execute(update_null_sql_g_budget_q)
        cur.execute(update_null_sql_g_budget_up)
        cur.execute(update_null_sql_g_budget_p)
        cur.execute(update_null_sql_x_m2_q)
        cur.execute(update_null_sql_x_m2_p)
        # connection.commit()

        sup_niin_list = df.item_niin.tolist()
        sup_price_list = df.item_price.tolist()

        print(len(sup_niin_list))
        print(len(sup_price_list))

        for i in range(0, len(sup_niin_list)):
            sql = "update ptype set price = {} where niin = '{}' ".format(sup_price_list[i], sup_niin_list[i])
            print(sql)
            cur.execute(sql)
            # connection.commit()

        cur.execute('update mngDate set bud = now()')

        connection.commit()
    except Exception as e:
        print(e)
    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/assetstatus', methods=['POST'])
def assetStatus():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the AssetStatus')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            niin_type = list(InputJson.values())[1]
            fileLocation = list(InputJson.values())[2]

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        print("nowYears : {}, niin_type : {}, file : {} ".format(nowYears, niin_type, fileLocation))

        # sys.exit(0)

        df = pd.read_excel(locationString, header=[0, 1])

        df.columns = ['rank', 'FSC', 'NIIN', 'demand', 'ASL', 'unit', 'price', 'sLevel1', 'assetTtl1', 'assetNow1',
                      'assetDiff1', 'sLevel2', 'assetTtl2', 'assetNow2', 'assetDiff2', 'sLevel3', 'assetTtl3', 'assetNow3',
                      'assetDiff3']

        import numpy as np

        df['assetInPlan1'] = np.nan
        df['assetInPlan2'] = np.nan
        df['assetInPlan3'] = np.nan
        df['assetOutPlan1'] = np.nan
        df['assetOutPlan2'] = np.nan
        df['assetOutPlan3'] = np.nan

        import datetime

        df['x_year'] = datetime.datetime(nowYears, 12, 1)

        df["x_year"] = df.x_year.astype(str)

        df['NIIN'] = df.NIIN.astype('str')
        df['NIIN'] = df['NIIN'].apply(lambda x: x.zfill(9))

        df = df[
            ['rank', 'FSC', 'NIIN', 'demand', 'ASL', 'unit', 'price', 'sLevel1', 'assetTtl1', 'assetNow1', 'assetDiff1',
             'assetInPlan1', 'assetOutPlan1', 'sLevel2', 'assetTtl2', 'assetNow2', 'assetDiff2', 'assetInPlan2',
             'assetOutPlan2', 'sLevel3', 'assetTtl3', 'assetNow3', 'assetDiff3', 'assetInPlan3', 'assetOutPlan3', 'x_year']]

        df["rank"].fillna(-99999999, inplace=True)
        df["FSC"].fillna(-99999999, inplace=True)
        df["NIIN"].fillna('None', inplace=True)
        df["demand"].fillna(-99999999, inplace=True)
        df["ASL"].fillna('None', inplace=True)
        df["unit"].fillna('None', inplace=True)
        df["price"].fillna(-99999999, inplace=True)
        df["sLevel1"].fillna(-99999999, inplace=True)
        df["assetTtl1"].fillna(-99999999, inplace=True)
        df["assetNow1"].fillna(-99999999, inplace=True)
        df["assetDiff1"].fillna(-99999999, inplace=True)
        df["assetInPlan1"].fillna(-99999999, inplace=True)
        df["assetOutPlan1"].fillna(-99999999, inplace=True)
        df["sLevel2"].fillna(-99999999, inplace=True)
        df["assetTtl2"].fillna(-99999999, inplace=True)
        df["assetNow2"].fillna(-99999999, inplace=True)
        df["assetDiff2"].fillna(-99999999, inplace=True)
        df["assetInPlan2"].fillna(-99999999, inplace=True)
        df["assetOutPlan2"].fillna(-99999999, inplace=True)
        df["sLevel3"].fillna(-99999999, inplace=True)
        df["assetTtl3"].fillna(-99999999, inplace=True)
        df["assetNow3"].fillna(-99999999, inplace=True)
        df["assetDiff3"].fillna(-99999999, inplace=True)
        df["assetInPlan3"].fillna(-99999999, inplace=True)
        df["assetOutPlan3"].fillna(-99999999, inplace=True)

        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        for i, row in df.iterrows():
            sql = "INSERT INTO `asset_status` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                  + "rank = {} , ".format(str(tuple(row)[0])) \
                  + "FSC = {} , ".format(str(tuple(row)[1])) \
                  + "demand = {} , ".format(str(tuple(row)[3])) \
                  + "ASL = '{}' , ".format(str(tuple(row)[4])) \
                  + "unit = '{}' , ".format(str(tuple(row)[5])) \
                  + "price = {} , ".format(str(tuple(row)[6])) \
                  + "sLevel1 = {} , ".format(str(tuple(row)[7])) \
                  + "assetTtl1 = {} , ".format(str(tuple(row)[8])) \
                  + "assetNow1 = {} , ".format(str(tuple(row)[9])) \
                  + "assetDiff1 = {} , ".format(str(tuple(row)[10])) \
                  + "assetInPlan1 = {} , ".format(str(tuple(row)[11])) \
                  + "assetOutPlan1 = {} , ".format(str(tuple(row)[12])) \
                  + "sLevel2 = {} , ".format(str(tuple(row)[13])) \
                  + "assetTtl2 = {} , ".format(str(tuple(row)[14])) \
                  + "assetNow2 = {} , ".format(str(tuple(row)[15])) \
                  + "assetDiff2 = {} , ".format(str(tuple(row)[16])) \
                  + "assetInPlan2 = {} , ".format(str(tuple(row)[17])) \
                  + "assetOutPlan2 = {} , ".format(str(tuple(row)[18])) \
                  + "sLevel3 = {} , ".format(str(tuple(row)[19])) \
                  + "assetTtl3 = {} , ".format(str(tuple(row)[20])) \
                  + "assetNow3 = {} , ".format(str(tuple(row)[21])) \
                  + "assetDiff3 = {} , ".format(str(tuple(row)[22])) \
                  + "assetInPlan3 = {} , ".format(str(tuple(row)[23])) \
                  + "assetOutPlan3 = {} ".format(str(tuple(row)[24]))

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        update_null_sql_rank = "update asset_status set rank = NULL where rank = -99999999 "
        update_null_sql_fsc = "update asset_status set FSC = NULL where FSC =- 99999999 "
        update_null_sql_niin = "update asset_status set NIIN = NULL where NIIN = 'None' "
        update_null_sql_demand = "update asset_status set demand = NULL where demand = -99999999 "
        update_null_sql_ASL = "update asset_status set ASL = NULL where ASL = 'None' "
        update_null_sql_unit = "update asset_status set unit = NULL where unit = 'None' "
        update_null_sql_price = "update asset_status set price = NULL where price = -99999999 "
        update_null_sql_sLevel1 = "update asset_status set sLevel1 = NULL where sLevel1 = -99999999 "
        update_null_sql_assetTtl1 = "update asset_status set assetTtl1 = NULL where assetTtl1 = -99999999 "
        update_null_sql_assetNow1 = "update asset_status set assetNow1 = NULL where assetNow1 = -99999999 "
        update_null_sql_assetDiff1 = "update asset_status set assetDiff1 = NULL where assetDiff1 = -99999999 "
        update_null_sql_assetInPlan1 = "update asset_status set assetInPlan1 = NULL where assetInPlan1 = -99999999 "
        update_null_sql_assetOutPlan1 = "update asset_status set assetOutPlan1 = NULL where assetOutPlan1 = -99999999 "
        update_null_sql_sLevel2 = "update asset_status set sLevel2 = NULL where sLevel2 = -99999999 "
        update_null_sql_assetTtl2 = "update asset_status set assetTtl2 = NULL where assetTtl2 = -99999999 "
        update_null_sql_assetNow2 = "update asset_status set assetNow2 = NULL where assetNow2 = -99999999 "
        update_null_sql_assetDiff2 = "update asset_status set assetDiff2 = NULL where assetDiff2 = -99999999 "
        update_null_sql_assetInPlan2 = "update asset_status set assetInPlan2 = NULL where assetInPlan2 = -99999999 "
        update_null_sql_assetOutPlan2 = "update asset_status set assetOutPlan2 = NULL where assetOutPlan2 = -99999999 "
        update_null_sql_sLevel3 = "update asset_status set sLevel3 = NULL where sLevel3 = -99999999 "
        update_null_sql_assetTtl3 = "update asset_status set assetTtl3 = NULL where assetTtl3 = -99999999 "
        update_null_sql_assetNow3 = "update asset_status set assetNow3 = NULL where assetNow3 = -99999999 "
        update_null_sql_assetDiff3 = "update asset_status set assetDiff3 = NULL where assetDiff3 = -99999999 "
        update_null_sql_assetInPlan3 = "update asset_status set assetInPlan3 = NULL where assetInPlan3 = -99999999 "
        update_null_sql_assetOutPlan3 = "update asset_status set assetOutPlan3 = NULL where assetOutPlan3 = -99999999 "

        cur.execute(update_null_sql_rank)
        cur.execute(update_null_sql_fsc)
        cur.execute(update_null_sql_niin)
        cur.execute(update_null_sql_demand)
        cur.execute(update_null_sql_ASL)
        cur.execute(update_null_sql_unit)
        cur.execute(update_null_sql_price)
        cur.execute(update_null_sql_sLevel1)
        cur.execute(update_null_sql_assetTtl1)
        cur.execute(update_null_sql_assetNow1)
        cur.execute(update_null_sql_assetDiff1)
        cur.execute(update_null_sql_assetInPlan1)
        cur.execute(update_null_sql_assetOutPlan1)
        cur.execute(update_null_sql_sLevel2)
        cur.execute(update_null_sql_assetTtl2)
        cur.execute(update_null_sql_assetNow2)
        cur.execute(update_null_sql_assetDiff2)
        cur.execute(update_null_sql_assetInPlan2)
        cur.execute(update_null_sql_assetOutPlan2)
        cur.execute(update_null_sql_sLevel3)
        cur.execute(update_null_sql_assetTtl3)
        cur.execute(update_null_sql_assetNow3)
        cur.execute(update_null_sql_assetDiff3)
        cur.execute(update_null_sql_assetInPlan3)
        cur.execute(update_null_sql_assetOutPlan3)

        # connection.commit()

        cur.execute('update mngDate set asset = now()')

        connection.commit()
    except Exception as e:
        print(e)
    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/orggroup', methods=['POST'])
def orgGroup():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the OrgGroup')

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = int(list(InputJson.values())[0])
            ptype = list(InputJson.values())[1]
            fileLocation1 = list(InputJson.values())[2]
            fileLocation2 = list(InputJson.values())[3]
            fileLocation3 = list(InputJson.values())[4]

        locationString1 = r'C:\UploadData\{}'.format(fileLocation1)
        locationString2 = r'C:\UploadData\{}'.format(fileLocation2)
        locationString3 = r'C:\UploadData\{}'.format(fileLocation3)

        print("nowYears : {}, ptype : {}, file1 : {} , file2 : {}, file3 : {} ".format(nowYears, ptype, fileLocation1, fileLocation2, fileLocation3))

        # sys.exit(0)

        # 부대코드와 주공급자/부대구매 목록을 join하여 org_group 테이블 생성
        df = pd.read_excel(locationString1)  # 부대목록
        df1 = pd.read_excel(locationString2)  # 주공급자
        df2 = pd.read_excel(locationString3)  # 중앙조달
        df = df.drop_duplicates()
        df1 = df1.drop_duplicates()
        df2 = df2.drop_duplicates()

        mapping = pd.concat([df1, df2], keys=['1', '2'])
        mapping = mapping.reset_index(level=0)
        mapping = mapping.drop_duplicates()

        result = pd.merge(left=df, right=mapping, left_on='code', right_on='code', how='left')

        result.rename(columns={'level_0': 'org_ptype'}, inplace=True)

        result['year'] = datetime.datetime(nowYears, 12, 1)

        result["year"] = result.year.astype(str)

        result.columns = ['org_code', 'location', 'group_ptype', 'grouped', 'year']

        result = result[["org_code", "grouped", "group_ptype", "location", "year"]]

        result["org_code"].fillna('None', inplace=True)
        result["grouped"].fillna('None', inplace=True)
        result["group_ptype"].fillna(-99999999, inplace=True)
        result["location"].fillna('None', inplace=True)

        cols = "`,`".join([str(i) for i in result.columns.tolist()])

        update_null_sql_org_code = "update org_group set org_code = NULL where org_code = 'None' "
        update_null_sql_grouped = "update org_group set grouped = NULL where grouped = 'None' "
        update_null_sql_group_ptype = "update org_group set group_ptype = NULL where group_ptype = -99999999 "
        update_null_sql_location = "update org_group set location = NULL where location = 'None' "

        print(result)

        for i, row in result.iterrows():
            sql = "INSERT INTO `org_group` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                  + "grouped = '{}' , ".format(str(tuple(row)[1])) \
                  + "group_ptype = {} , ".format(str(tuple(row)[2])) \
                  + "location = '{}' ".format(str(tuple(row)[3]))
            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        cur.execute(update_null_sql_org_code)
        cur.execute(update_null_sql_grouped)
        cur.execute(update_null_sql_group_ptype)
        cur.execute(update_null_sql_location)
        # connection.commit()

        cur.execute('update mngDate set org = now()')

        connection.commit()
    except Exception as e:
        print(e)

    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/ddrug', methods=['POST'])
def ddrug():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the Ddrug')

        test = request.get_json("testJson")

        if request.method == 'POST':
            nowYears = int(list(test.values())[0])
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

        # sys.exit(0)

        ddrug = pd.read_excel(locationString, thousands=',', dtype=str)

        ddrug.columns = ['fsc', 'niin', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                         'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11']

        # ddrug_fsc = ddrug['fsc']

        # ddrug.drop('fsc', inplace=True, axis=1)
        ddrug['niin'] = ddrug.niin.astype('str')
        ddrug['niin'] = ddrug['niin'].apply(lambda x: x.zfill(9))

        # fsc , niin , name, price 가 같은 항목 sum (목적 컬럼에 따라 여러 행수로 나누어진거 합침)
        ddrug = ddrug.groupby(['fsc', 'niin', 'niin_name', 'price']).agg(lambda x: pd.to_numeric(x, errors='coerce').sum()).reset_index()

        ddrug['type'] = 'drug'
        ddrug['x_year'] = datetime.date(int(nowYears), 12, 1)

        print('===================================')
        print(ddrug.head(5))

        # TODO: 웹에서 기준년도 가져온 값을 nowYear에 assign

        ddrug = ddrug[['fsc', 'niin', 'type', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                       'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11', 'x_year']]

        ddrug.columns = ['fsc', 'niin', 'type', 'niin_name', 'price', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4', 'x_minus_5',
                         'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']

        #### update PTYPE ####
        update_ptype_drug = ddrug[['fsc', 'niin', 'type', 'niin_name', 'price', 'x_year']]
        update_ptype_drug.rename(columns={'type': 'niin_type'}, inplace=True)
        update_ptype_drug.rename(columns={'x_year': 'year'}, inplace=True)
        update_ptype_drug = update_ptype_drug.replace({np.nan: None})

        print('===================================')
        print(update_ptype_drug)

        # TODO : 1214
        # for i, row in update_ptype_drug.iterrows():
        #     sql = "update `ptype` set fsc = %s , niin_type = %s , niin_name = %s , price = %s where niin = %s and year = %s"
        #     print(sql, tuple(row))
        #     cur.execute(sql, (tuple(row)[0] , tuple(row)[2] ,  tuple(row)[3] ,tuple(row)[4] ,tuple(row)[1] , tuple(row)[5]) )
        #     connection.commit()

        cols = "`,`".join([str(i) for i in update_ptype_drug.columns.tolist()])

        # ptype에 없는 새로운 niin이 들어오면, 해당정보 (5컬럼), ptype에 추가

        for i, row in update_ptype_drug.iterrows():
            sql = "INSERT INTO `ptype` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE fsc ='{}' , niin_type = '{}' , niin_name ='{}', price ={} ".format(
                str(tuple(row)[0]), str(tuple(row)[2]), str(tuple(row)[3]), str(tuple(row)[4]))
            print(sql, tuple(row))
            cur.execute(sql, tuple(row))

        cur.execute(lpad_niin.format('ptype', 'niin'))
        # connection.commit()

        #########################
        print('done')

        print(ddrug.head(5))

        ddrug = ddrug[['type', 'fsc', 'niin', 'niin_name', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4', 'x_minus_5',
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
                  + "niin_name = '{}' , ".format(str(tuple(row)[3])) \
                  + "x_minus_1 = " + str(tuple(row)[4]) + " , " \
                  + "x_minus_2 = " + str(tuple(row)[5]) + " , " \
                  + "x_minus_3 = " + str(tuple(row)[6]) + " , " \
                  + "x_minus_4 = " + str(tuple(row)[7]) + " , " \
                  + "x_minus_5 = " + str(tuple(row)[8]) + " , " \
                  + "x_minus_6 = " + str(tuple(row)[9]) + " , " \
                  + "x_minus_7 = " + str(tuple(row)[10]) + " , " \
                  + "x_minus_8 = " + str(tuple(row)[11]) + " , " \
                  + "x_minus_9 = " + str(tuple(row)[12]) + " , " \
                  + "x_minus_10 = " + str(tuple(row)[13]) + " , " \
                  + "x_minus_11 = " + str(tuple(row)[14])

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        cur.execute(lpad_niin.format('demand', 'niin'))
        # connection.commit()

        cur.execute('update mngDate set ddrug = now()')

        connection.commit()
    except Exception as e:
        print(e)

    finally:
        connection.close()
        cur.close()
        return 'DONE'

    # sys.exit(0)


@app.route('/dgoods', methods=['POST'])
def dgoods():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        print('Enter the Dgoods')

        test = request.get_json("testJson")

        if request.method == 'POST':
            nowYears = int(list(test.values())[0])
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

        # sys.exit(0)

        ddrug = pd.read_excel(locationString, thousands=',', dtype=str)

        ddrug.columns = ['fsc', 'niin', 'niin_name', 'price', 'x-1', 'x-2', 'x-3', 'x-4', 'x-5',
                         'x-6', 'x-7', 'x-8', 'x-9', 'x-10', 'x-11']

        ddrug['niin'] = ddrug.niin.astype('str')
        ddrug['niin'] = ddrug['niin'].apply(lambda x: x.zfill(9))

        # ddrug_fsc = ddrug['fsc']

        # ddrug.drop('fsc', inplace=True, axis=1)

        ddrug = ddrug.groupby(['fsc', 'niin', 'niin_name', 'price']).agg(lambda x: pd.to_numeric(x, errors='coerce').sum()).reset_index()

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
        update_ptype_drug.rename(columns={'x_year': 'year'}, inplace=True)
        update_ptype_drug = update_ptype_drug.replace({np.nan: None})

        print('===================================')
        print(update_ptype_drug)

        # for i, row in update_ptype_drug.iterrows():
        #     sql = "update `ptype` set fsc = %s , niin_type = %s , niin_name = %s , price = %s where niin = %s and year = %s"
        #     print(sql, tuple(row))
        #     cur.execute(sql, (tuple(row)[0], tuple(row)[2], tuple(row)[3], tuple(row)[4], tuple(row)[1], tuple(row)[5]))
        #     connection.commit()

        cols = "`,`".join([str(i) for i in update_ptype_drug.columns.tolist()])

        for i, row in update_ptype_drug.iterrows():
            sql = "INSERT INTO `ptype` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE fsc ='{}' , niin_type = '{}' , niin_name ='{}', price ={} ".format(
                str(tuple(row)[0]), str(tuple(row)[2]), str(tuple(row)[3]), str(tuple(row)[4]))
            print(sql, tuple(row))
            cur.execute(sql, tuple(row))

        cur.execute(lpad_niin.format('ptype', 'niin'))
        # connection.commit()
        #########################
        print('done')

        print(ddrug.head(5))

        ddrug = ddrug[['type', 'fsc', 'niin', 'niin_name', 'x_minus_1', 'x_minus_2', 'x_minus_3', 'x_minus_4', 'x_minus_5',
                       'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']]

        # ddrug.columns = ['fsc', 'niin', 'x_minus_1', 'x_minus_2', 'x_minus_3',
        #                  'x_minus_4', 'x_minus_5',
        #                  'x_minus_6', 'x_minus_7', 'x_minus_8', 'x_minus_9', 'x_minus_10', 'x_minus_11', 'x_year']

        # 년도가 같고, type이 같고 , niin이 같으면 update(replace) , 다른게 있으면 insert =>upsert , replace

        cols = "`,`".join([str(i) for i in ddrug.columns.tolist()])
        print('===================================')
        ddrug = ddrug.replace({np.nan: None})
        print(ddrug)
        print(cols)

        # ddrug.to_csv('demand_drug.csv' , encoding ='CP949' , index = False)

        for i, row in ddrug.iterrows():
            sql = "INSERT INTO `demand` (`" + cols + "`) VALUES (" + "%s," * (
                    len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                  + "niin_name = '{}' , ".format(str(tuple(row)[3])) \
                  + "x_minus_1 = " + str(tuple(row)[4]) + " , " \
                  + "x_minus_2 = " + str(tuple(row)[5]) + " , " \
                  + "x_minus_3 = " + str(tuple(row)[6]) + " , " \
                  + "x_minus_4 = " + str(tuple(row)[7]) + " , " \
                  + "x_minus_5 = " + str(tuple(row)[8]) + " , " \
                  + "x_minus_6 = " + str(tuple(row)[9]) + " , " \
                  + "x_minus_7 = " + str(tuple(row)[10]) + " , " \
                  + "x_minus_8 = " + str(tuple(row)[11]) + " , " \
                  + "x_minus_9 = " + str(tuple(row)[12]) + " , " \
                  + "x_minus_10 = " + str(tuple(row)[13]) + " , " \
                  + "x_minus_11 = " + str(tuple(row)[14])

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        cur.execute(lpad_niin.format('demand', 'niin'))
        # connection.commit()

        cur.execute('update mngDate set dgoods = now()')

        connection.commit()
    except Exception as e:
        print(e)
    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/druginfo', methods=['POST'])
def drug_info():
    # dcode_dname_unit : 새로운 데이터의 drug_code와 dcode_dname_unit테이블의 drug_code를 비교하여, 새로운 drug_code 이면 해당 데이터 INSERT
    # niin_dcode : 새로운 데이터의 niin과 drug_code를 CCD와 조인하여 나온 모든 drug_code_niin의 조합을 UPSERT(PK : niin, dcode)
    # C:\1201data\druginfo
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            nowYears = list(InputJson.values())[0]
            fileLocation = list(InputJson.values())[1]

        print("nowYears : {} , fileLocation : {} ".format(nowYears, fileLocation))

        locationString = r'C:\UploadData\{}'.format(fileLocation)

        # print(locationString)

        # sys.exit(0)

        df = pd.read_csv(locationString, encoding='CP949')

        df_dcode_dname_unit = df[['약품코드', '조제명', '성분', '처방단위']]
        df_dcode_dname_unit.columns = ['drug_code', 'drug_name', 'ingr_name', 'unit']

        ccd = pd.read_excel(r'C:\UploadData\CCD.xlsx')
        ccd = ccd[['NIIN', 'CCD']].drop_duplicates()

        df_dcode_niin = df[['NIIN', '약품코드']]
        df_dcode_niin.columns = ['niin', 'drug_code']

        df_dcode_niin['niin'] = df_dcode_niin.niin.astype('str')

        df_dcode_niin['niin'] = df_dcode_niin['niin'].apply(lambda x: x.zfill(9) if x != 'nan' else x)

        print(len(df_dcode_niin))
        df_dcode_niin = pd.merge(left=df_dcode_niin, right=ccd, left_on='drug_code', right_on='CCD', how='left')
        print(len(df_dcode_niin))

        df_dcode_niin_melt = df_dcode_niin[['NIIN', 'CCD']]
        df_dcode_niin_melt.columns = ['niin', 'drug_code']

        df_dcode_niin.drop(['NIIN', 'CCD'], inplace=True, axis=1)

        df_dcode_niin = pd.concat([df_dcode_niin, df_dcode_niin_melt], axis=0)
        df_dcode_niin.dropna(inplace=True)
        df_dcode_niin = df_dcode_niin[df_dcode_niin.niin != 'nan']
        df_dcode_niin.drop_duplicates(inplace=True)
        df_dcode_niin['niin'] = df_dcode_niin['niin'].apply(lambda x: x.zfill(9))

        raw_sql = "select  *  from dcode_dname_unit"
        raw = pd.read_sql(raw_sql, connection)
        raw.drop('seq', axis=1, inplace=True)

        raw_dcode = raw.drug_code.tolist()
        df_dcode = df_dcode_dname_unit.drug_code.tolist()

        diff_dcode = list(set(df_dcode) - set(raw_dcode))
        # print("DIFF DCODE : {} ".format(diff_dcode))

        df_dcode_dname_unit = df_dcode_dname_unit[df_dcode_dname_unit['drug_code'].isin(diff_dcode)]

        print('##################### INSERT ########################')
        print(df_dcode_dname_unit)
        print('#####################################################')

        cols = "`,`".join([str(i) for i in df_dcode_dname_unit.columns.tolist()])

        for i, row in df_dcode_dname_unit.iterrows():
            sql = "INSERT INTO `dcode_dname_unit` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            # connection.commit()

        # niin_dcode

        print('##################### niin_dcode  ########################')
        print(df_dcode_niin)
        print('#########################################################')

        cols = "`,`".join([str(i) for i in df_dcode_niin.columns.tolist()])

        for i, row in df_dcode_niin.iterrows():
            sql = "INSERT INTO `niin_dcode` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s) ON DUPLICATE KEY UPDATE niin = '{0}' , drug_code = '{1}' ".format(str(tuple(row)[0]), str(tuple(row)[1]))

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))

            cur.execute('update mngDate set dinfo = now()')
            connection.commit()

        # icode_dcode테이블 최신화
        icode_dcode()
    except Exception as e:
        print(e)
    finally:
        connection.close()
        cur.close()
        return 'DONE'


@app.route('/ingrmaster', methods=['POST'])
def ingr_master():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        InputJson = request.get_json("InputJson")

        if request.method == 'POST':
            fileLocation1 = list(InputJson.values())[0]
            fileLocation2 = list(InputJson.values())[1]

        locationString1 = r'C:\UploadData\{}'.format(fileLocation1)
        locationString2 = r'C:\UploadData\{}'.format(fileLocation2)

        chief = pd.read_csv(locationString1, encoding='CP949')
        master = pd.read_csv(locationString2, encoding='CP949')

        chief_data = chief[['코드', '주성분명칭', '함량', '단위']].dropna(axis=0).reset_index(drop=True)
        master_data = master[['한글상품명', '일반명코드(성분명코드)']].dropna(axis=0).reset_index(drop=True)

        chief_col_list = chief_data.columns
        for c in range(len(chief_col_list)):
            chief_data = chief_data.astype({chief_col_list[c]: 'str'})

        master_col_list = master_data.columns
        for c in range(len(master_col_list)):
            master_data = master_data.astype({master_col_list[c]: 'str'})

        # chief_data['성분함량단위'] = chief_data['주성분명칭'] + ' ' + chief_data['함량'] + chief_data['단위']
        chief_data['성분함량단위'] = chief_data['주성분명칭'] + '' + chief_data['함량'] + chief_data['단위']

        c_df = chief_data[['코드', '성분함량단위']].dropna(axis=0).drop_duplicates().reset_index(drop=True)
        m_df = master_data[['한글상품명', '일반명코드(성분명코드)']].dropna(axis=0).drop_duplicates().reset_index(drop=True)

        c_df.columns = ['ingr_code', 'ingr_name']
        m_df.columns = ['ingr_drug_name', 'ingr_code']

        m_c_df = pd.merge(c_df, m_df, on='ingr_code').dropna(axis=0).drop_duplicates().reset_index(drop=True)

        result = m_c_df[['ingr_code', 'ingr_drug_name', 'ingr_name']].drop_duplicates().reset_index(drop=True)

        print(result)

        # result insert to ingr_master , drop_d(ingr_code , ingr_drug_code, ingr_name)

        cols = "`,`".join([str(i) for i in result.columns.tolist()])

        for i, row in result.iterrows():
            sql = "INSERT INTO `ingr_master` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s) ON DUPLICATE KEY UPDATE ingr_code = '{0}' , ingr_drug_name = '{1}' , ingr_name = '{2}' ".format(
                str(tuple(row)[0]), str(tuple(row)[1]), str(tuple(row)[2]))

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))

            cur.execute('update mngDate set imaster = now()')
            connection.commit()

        # icode_dcode테이블 최신화
        icode_dcode()

    except Exception as e:
        print(e)

    finally:
        connection.close()
        cur.close()
        return 'DONE'


def icode_dcode():
    # TODO: DB에서 불러오기 (약품정보현황, 주성분&마스터)
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        info_sql = "select  drug_code, drug_name, ingr_name from dcode_dname_unit"
        info = pd.read_sql(info_sql, connection)

        info_df = info[['drug_code', 'drug_name', 'ingr_name']]

        info_df.columns = ['drug_code', 'ingr_drug_name', 'ingr_name']

        imaster_sql = "select  ingr_code , ingr_drug_name , ingr_name from ingr_master "
        ingred_master = pd.read_sql(imaster_sql, connection)

        print(ingred_master)

        # print('@@@@@@@@@@@@')
        # imaster_sql_test = "select * from ingr_master where ingr_code ='AAAAA'"
        #
        # ingred_master = pd.read_sql(imaster_sql_test, connection)
        # print(ingred_master)
        #
        # sys.exit(0)

        def icode_dcode_01(ingr_master=ingred_master, drug_info=info_df):
            cnt = 0

            ingr_master_col_list = list(ingr_master.columns)
            for n in range(len(ingr_master_col_list)):
                ingr_master = ingr_master.astype({ingr_master_col_list[n]: 'str'})

            drug_info_col_list = list(drug_info.columns)
            for n in range(len(drug_info_col_list)):
                drug_info = drug_info.astype({drug_info_col_list[n]: 'str'})

            icode_dcode_list = []
            icode_dcode_list_append = icode_dcode_list.append

            iname_list = []
            iname_list_append = iname_list.append

            ingr_code = ingr_master['ingr_code'].values
            ingr_name = ingr_master['ingr_name'].values

            drug_info_code = drug_info['drug_code'].values
            drug_info_ingr_name = drug_info['ingr_name'].values

            for i in range(len(ingr_master)):
                cnt += 1
                if cnt % 1000 == 0:
                    print(cnt)
                    print(round((cnt / len(ingr_master)) * 100, 3))
                for j in range(len(drug_info)):
                    if jf.jaro_distance(ingr_name[i].lower(), drug_info_ingr_name[j].lower()) > 0.9:
                        icode_dcode_list_append([ingr_code[i], drug_info_code[j]])
                        iname_list_append([ingr_name[i], drug_info_ingr_name[j]])

            icode_dcode_df = pd.DataFrame(icode_dcode_list,
                                          columns=['ingr_code', 'drug_code'])
            iname_df = pd.DataFrame(iname_list,
                                    columns=['ingredient_name_ingred', 'ingredient_name_info'])

            result_icode_dcode_df = icode_dcode_df.drop_duplicates().reset_index(drop=True)
            result_iname_df = iname_df.drop_duplicates().reset_index(drop=True)

            return result_icode_dcode_df, result_iname_df

        def icode_dcode_02(ingr_master=ingred_master, drug_info=info_df):
            cnt = 0

            ingr_col_list = list(ingr_master.columns)
            for n in range(len(ingr_col_list)):
                ingr_master = ingr_master.astype({ingr_col_list[n]: 'str'})

            drug_info_col_list = list(drug_info.columns)
            for n in range(len(drug_info_col_list)):
                drug_info = drug_info.astype({drug_info_col_list[n]: 'str'})

            icode_dcode_list = []
            icode_dcode_list_append = icode_dcode_list.append

            ingr_name_list = []
            ingr_name_list_append = ingr_name_list.append

            ingr_code = ingr_master['ingr_code'].values
            ingr_name = ingr_master['ingr_drug_name'].values

            drug_info_drug_code = drug_info['drug_code'].values
            drug_info_drug_name = drug_info['ingr_drug_name'].values

            for i in range(len(ingr_master)):
                cnt += 1
                if cnt % 1000 == 0:
                    print(cnt)
                    print(round((cnt / len(ingr_master)) * 100, 3))
                for j in range(len(drug_info)):
                    if jf.jaro_distance(ingr_name[i].lower(), drug_info_drug_name[j].lower()) > 0.9:
                        icode_dcode_list_append([ingr_code[i], drug_info_drug_code[j]])
                        ingr_name_list_append([ingr_name[i], drug_info_drug_name[j]])

            main_ingred_code_drug_code_df = pd.DataFrame(icode_dcode_list,
                                                         columns=['ingr_code', 'drug_code'])
            main_ingred_name_df = pd.DataFrame(ingr_name_list,
                                               columns=['ingr_drug_name_master', 'ingr_drug_name_info'])

            result_main_ingred_code_drug_code_df = main_ingred_code_drug_code_df.drop_duplicates().reset_index(drop=True)
            result_main_ingred_name_df = main_ingred_name_df.drop_duplicates().reset_index(drop=True)

            return result_main_ingred_code_drug_code_df, result_main_ingred_name_df

        # ingred_master_00 = ingred_master.iloc[:10]
        # info_df_00 = info_df.iloc[-10:]
        # df_ingr_drug_code_01, df_main_ingred_name_01 = icode_dcode_01(ingred_master_00, info_df_00)
        # df_ingr_drug_code_02, df_main_ingred_name_02 = icode_dcode_02(ingred_master_00, info_df_00)

        df_ingr_drug_code_01, df_main_ingred_name_01 = icode_dcode_01(ingred_master, info_df)
        df_ingr_drug_code_02, df_main_ingred_name_02 = icode_dcode_02(ingred_master, info_df)

        result_dataframe = pd.concat([df_ingr_drug_code_01, df_ingr_drug_code_02], axis=0)

        result_dataframe_clear = result_dataframe.drop_duplicates().reset_index(drop=True)

        # print(ingred_master_00)
        # print('========================')
        # print(info_df_00)
        # print('========================')
        # print(result_dataframe)
        # print('========================')
        # print(result_dataframe_clear)

        # TODO : result_dataframe_clear insert to icode_dcode (PK : ingr_code , drug_code)

        cols = "`,`".join([str(i) for i in result_dataframe_clear.columns.tolist()])

        for i, row in result_dataframe_clear.iterrows():
            sql = "INSERT INTO `icode_dcode` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s) ON DUPLICATE KEY UPDATE ingr_code = '{0}' , drug_code = '{1}' ".format(str(tuple(row)[0]),
                                                                                                                    str(tuple(row)[1]))

            print(sql)
            print(tuple(row))
            cur.execute(sql, tuple(row))
            connection.commit()


    except Exception as e:
        print(e)

    finally:
        connection.close()
        cur.close()
        return "DONE"


@app.route('/weather', methods=['POST'])
def weather():
    try:
        # connection
        connection = pymysql.connect(host=login['host'], port=login['port'],
                                     user=login['user'], passwd=login['passwd'], db=login['db'],
                                     charset=login['charset'])
        # cursor
        cur = connection.cursor()

        celsius_file_path = r'C:\UploadData\weather\celsius'
        rain_file_path = r'C:\UploadData\weather\rain'
        dust_file_path = r'C:\UploadData\weather\dust'

        select_list = request.get_json("testJson")

        if request.method == 'POST':
            fileLocation_celsius = list(select_list.values())[0]
            fileLocation_rain = list(select_list.values())[1]
            fileLocation_dust = list(select_list.values())[2]

            print(fileLocation_celsius)
            print(fileLocation_rain)
            print(fileLocation_dust)

            # CSV 파일 한번에 읽어와 하나의 데이터프레임으로
            def get_celsius_rain_merged_csv(file_list, **kwargs):
                return pd.concat([pd.read_csv(f, **kwargs, encoding='CP949', skiprows=[0, 1, 2, 3, 4, 5, 6], header=0)
                                  for f in file_list], ignore_index=True)

            def get_dust_csv(file_list, **kwargs):
                return pd.concat([pd.read_csv(f, **kwargs, encoding='CP949') for f in file_list], ignore_index=True)

            celsius_file_list = []
            for c in range(len(fileLocation_celsius)):
                celsius_file = os.path.join(celsius_file_path, '{}').format(fileLocation_celsius[c])
                celsius_file_list.append(celsius_file)
            celsius_df = get_celsius_rain_merged_csv(celsius_file_list, index_col=None)

            rain_file_list = []
            for r in range(len(fileLocation_rain)):
                rain_file = os.path.join(rain_file_path, '{}').format(fileLocation_rain[r])
                rain_file_list.append(rain_file)
            rain_df = get_celsius_rain_merged_csv(rain_file_list, index_col=None)

            dust_file_list = []
            for d in range(len(fileLocation_dust)):
                dust_file = os.path.join(dust_file_path, '{}').format(fileLocation_dust)
                dust_file_list.append(dust_file)
            dust_df = get_dust_csv(dust_file_list, index_col=None)

            celsius_t = celsius_df[['년월', '지점', '평균기온(℃)']].sort_values(['년월']).reset_index(drop=True)
            rain_t = rain_df[['년월', '지점', '강수량(mm)']].sort_values('년월').reset_index(drop=True)
            dust_t = dust_df[['일시', '지점명', '월 미세먼지 농도(㎍/㎥)']].sort_values('일시').reset_index(drop=True)

            celsius_t.columns = ['dates', 'location', 'celsius']
            rain_t.columns = ['dates', 'location', 'rain']
            dust_t.columns = ['dates', 'location', 'fine_dust']

            c_col_list = celsius_t.columns
            for col in range(len(c_col_list)):
                celsius_t = celsius_t.astype(({c_col_list[col]: 'str'}))

            r_col_list = rain_t.columns
            for col in range(len(r_col_list)):
                rain_t = rain_t.astype(({r_col_list[col]: 'str'}))

            d_col_list = dust_t.columns
            for col in range(len(d_col_list)):
                dust_t = dust_t.astype(({d_col_list[col]: 'str'}))

            def celsius_rain_loc_list(_list):
                for i in range(len(_list)):
                    if _list[i] == '108':
                        _list[i] = '서울'
                    elif _list[i] == '경남':
                        _list[i] = '경상도'
                    elif _list[i] == '119':
                        _list[i] = '경기도'
                    elif _list[i] == '강원영동':
                        _list[i] = '강원도'
                    elif _list[i] == '충남':
                        _list[i] = '충청도'
                    elif _list[i] == '제주':
                        _list[i] = '제주도'
                    elif _list[i] == '전남':
                        _list[i] = '전라도'

                return _list

            def dust_loc_list(_list):
                for i in range(len(_list)):
                    if _list[i] == '대관령':
                        _list[i] = '강원도'
                    elif _list[i] == '서울':
                        _list[i] = '서울'
                    elif _list[i] == '수원':
                        _list[i] = '경기도'
                    elif _list[i] == '안면도(감)':
                        _list[i] = '충청도'
                    elif _list[i] == '광주':
                        _list[i] = '전라도'
                    elif _list[i] == '구덕산':
                        _list[i] = '경상도'
                    elif _list[i] == '고산':
                        _list[i] = '제주도'

                return _list

            l_list_01 = celsius_rain_loc_list(celsius_t['location'].values)
            l_list_02 = celsius_rain_loc_list(rain_t['location'].values)
            l_list_03 = dust_loc_list(dust_t['location'].values)

            celsius_t['location'] = l_list_01
            rain_t['location'] = l_list_02
            dust_t['location'] = l_list_03

            cesius_rain = pd.merge(celsius_t, rain_t, on=['dates', 'location']).drop_duplicates().reset_index(drop=True)

            cesius_rain_dust = pd.merge(cesius_rain, dust_t, how='left').drop_duplicates().reset_index(drop=True)
            cesius_rain_dust = cesius_rain_dust[['location', 'dates', 'rain', 'celsius', 'fine_dust']]

            cesius_rain_dust["fine_dust"].fillna(-999999999, inplace=True)
            cesius_rain_dust = cesius_rain_dust.replace({'nan': -999999999})

            print(cesius_rain_dust.dtypes)
            print('#######REULST##########')
            print(cesius_rain_dust)
            print('#######################')

            cesius_rain_dust['dates'] = cesius_rain_dust['dates'].apply(lambda x: str(x) + str('-01'))

            cols = "`,`".join([str(i) for i in cesius_rain_dust.columns.tolist()])

            for i, row in cesius_rain_dust.iterrows():
                sql = "INSERT INTO `weather` (`" + cols + "`) VALUES (" + "%s," * (
                        len(row) - 1) + "%s)" + " ON DUPLICATE KEY UPDATE " \
                      + "rain = " + str(tuple(row)[2]) + " , " \
                      + "celsius = " + str(tuple(row)[3]) + " , " \
                      + "fine_dust = " + str(tuple(row)[4])
                print(sql)
                print(tuple(row))
                cur.execute(sql, tuple(row))

            # TODO :
            update_null_sql_findust = "update weather set fine_dust = NULL where fine_dust = -999999999"
            update_null_sql_celsius = "update weather set celsius = NULL where celsius = -999999999"

            cur.execute(update_null_sql_celsius)
            cur.execute(update_null_sql_findust)

            cur.execute('update mngDate set weather = now()')
            connection.commit()



    except Exception as e:
        print(e)
    finally:
        connection.close()
        cur.close()
        return "DONE"


##


if __name__ == '__main__':
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run(host='0.0.0.0', port='5000')
