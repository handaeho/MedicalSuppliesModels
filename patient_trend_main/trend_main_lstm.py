import pandas as pd
import numpy as np

from dataframe_pretreatment.trend_dataframe_pretreatment import select_hosp, select_div, select_all_loc_hosp, select_all_loc_div
from patient_trend_model.trend_model_lstm import PatientTrendLSTMModel
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class PatientTrendRunLSTM:
    def __init__(self):
        print('******************** RUN TREND LSTM ********************')
        # 클래스 인스턴스
        self.hosp_lstm = PatientTrendLSTMModel(5)
        self.eval = Evaluation()

    def run_lstm_all_loc_hosp(self, userselect):
        """
        All of hospital dataset's range is base year - 7.
        (hospital: 2012-05 ~ 2019-12 / division: 2017-03 ~ 2019-12)
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of user's select
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            df = select_all_loc_hosp(diag_code=i, in_out=inout, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            try:
                forecast = self.hosp_lstm.lstm(x=true_m)
                forecast = float(forecast)
                if forecast < 0:
                    forecast = 0

            except:  # If there is no data you are looking for in the dataset.
                forecast = 'None'

            try:
                mae, rmsle = self.eval.eval(true_m[-1], forecast)

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae, rmsle = 'None', 'None'

            result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json

    def run_lstm_all_loc_div(self, userselect):
        """
        All of division dataset's range is base year - 7.
        (hospital: 2012-05 ~ 2019-12 / division: 2017-03 ~ 2019-12)
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of user's select
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            if year == '2020':
                div_lstm = PatientTrendLSTMModel(1)

                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2021':
                div_lstm = PatientTrendLSTMModel(2)

                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2022':
                div_lstm = PatientTrendLSTMModel(3)

                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2023':
                div_lstm = PatientTrendLSTMModel(4)

                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            else:
                div_lstm = PatientTrendLSTMModel(5)

                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json

    def run_lstm_hosp(self, userselect):
        """
        hospital dataset's range is base year - 7.
        (hospital: 2012-05 ~ 2019-12 / division: 2017-03 ~ 2019-12)
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of user's select
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            df = select_hosp(diag_code=i, in_out=inout, location=loc,  year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            try:
                forecast = self.hosp_lstm.lstm(x=true_m)
                forecast = float(forecast)
                if forecast < 0:
                    forecast = 0

            except:  # If there is no data you are looking for in the dataset.
                forecast = 'None'

            try:
                mae, rmsle = self.eval.eval(true_m[-1], forecast)

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae, rmsle = 'None', 'None'

            result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json

    def run_lstm_div(self, userselect):
        """
        division dataset's range is base year - 7.
        (hospital: 2012-05 ~ 2019-12 / division: 2017-03 ~ 2019-12)
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of user's select
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            if year == '2020':
                div_lstm = PatientTrendLSTMModel(1)

                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2021':
                div_lstm = PatientTrendLSTMModel(2)

                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2022':
                div_lstm = PatientTrendLSTMModel(3)

                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2023':
                div_lstm = PatientTrendLSTMModel(4)

                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            else:
                div_lstm = PatientTrendLSTMModel(5)

                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json


if __name__ == '__main__':
    t = PatientTrendRunLSTM()

    r1 = t.run_lstm_all_loc_hosp([["J00"], "('외래', '입원')", 2020])
    r2 = t.run_lstm_hosp([["J00"], "('외래', '입원')", "강원도", 2020])
    r3 = t.run_lstm_all_loc_div([["J00", "A04.9-01"], "('외래', '입원')", 2020])
    r4 = t.run_lstm_div([["J00", "A04.9-01"], "('외래', '입원')", "강원도", 2020])

    print(r1)
    print(r2)
    print(r3)
    print(r4)
