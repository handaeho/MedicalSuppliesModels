import pandas as pd
import numpy as np

from dataframe_pretreatment.trend_dataframe_pretreatment import select_hosp, select_div, select_all_loc_hosp, select_all_loc_div
from patient_trend_model.trend_model_arima import PatientTrendARIMAModel
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class PatientTrendRunARIMA:
    def __init__(self):
        print('******************** RUN TREND ARIMA ********************')

        # 클래스 인스턴스
        self.arima = PatientTrendARIMAModel()
        self.evalu = Evaluation()

    def run_arima_all_loc_hosp(self, userselect):
        """
        All of hospital dataset's range is base year - 7.
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of diagcode, div, inout, location (from web)
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
                forecast, prediction = self.arima.arima(true_m)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except:  # If there is no data you are looking for in the dataset.
                forecast, prediction = 'None', 'None'

            try:
                mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae_m, rmsle_m = 'None', 'None'

            result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_arima_all_loc_div(self, userselect):
        """
        All of division dataset's range is base year - 7.
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of diagcode, div, inout, location (from web)
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            if year == '2020':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2021':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2022':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2023':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            else:
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(result_list,

                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_arima_hosp(self, userselect):
        """
        hospital dataset's range is base year - 7.
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of diagcode, div, inout, location (from web)
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            df = select_hosp(diag_code=i, in_out=inout, location=loc, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            try:
                forecast, prediction = self.arima.arima(true_m)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except:  # If there is no data you are looking for in the dataset.
                forecast, prediction = 'None', 'None'

            try:
                mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae_m, rmsle_m = 'None', 'None'

            result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_arima_div(self, userselect):
        """
        division dataset's range is base year - 7.
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param userselect: List of diagcode, div, inout, location (from web)
        :return: result prediction (type is json)
        """
        result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            if year == '2020':
                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2021':
                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2022':
                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2023':
                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

            else:
                df = select_div(diag_code=i, in_out=inout, location=loc, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.arima.arima(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.evalu.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                result_list.append(['ARIMA', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json


if __name__ == '__main__':
    t = PatientTrendRunARIMA()

    r1 = t.run_arima_all_loc_hosp([["J00"], "('외래', '입원')", 2020])
    r2 = t.run_arima_hosp([["J00"], "('입원')", "강원도", 2020])
    r3 = t.run_arima_all_loc_div([["J00", "A04.9-01"], "('외래', '입원')", 2020])
    r4 = t.run_arima_div([["J00", "A04.9-01"], "('외래', '입원')", "강원도", 2020])

    print(r1)
    print(r2)
    print(r3)
    print(r4)
