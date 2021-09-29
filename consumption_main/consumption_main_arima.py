import pandas as pd
import numpy as np

from consumption_models.consumption_model_arima import ConsumptionARIMAModel
from dataframe_pretreatment.consumption_dataframe_pretreatment import select_hosp, select_div
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class ConsumptionRunARIMA:
    def __init__(self):
        print('******************** RUN CONSUM ARIMA ********************')

        # 클래스 인스턴스
        self.arima = ConsumptionARIMAModel()
        self.evalu = Evaluation()

    def hosp_run_arima(self, drugcode_list, year):
        """
        hospital dataset's range is 'base year'-7.
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param drugcode_list: List of drug codes you want to predict
        :param year: base year
        :return: result prediction (type is json)
        """
        result_list = []

        for i in drugcode_list:
            df = select_hosp(drugcode=i, year=year)

            df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float',
                              'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

            true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

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
                            columns=['modelName', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def div_run_arima(self, drugcode_list, year):
        """
        division dataset's range is 'base year'-7.
        So, the monthly data is input to the model to predict,
        and the monthly data are summed and returned as a value for each year.

        :param drugcode_list: List of drug codes you want to predict
        :param year: base year
        :return: result prediction (type is json)
        """
        result_list = []

        for i in drugcode_list:
            if year == '2020':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

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
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

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
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

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
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

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
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q',
                                        'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

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
                            columns=['modelName', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json


if __name__ == '__main__':
    t = ConsumptionRunARIMA()
    # r1 = t.hosp_run_arima(['PAAAPSS32'], 2020)
    # r2 = t.div_run_arima(['PAAAPSS32'], 2020)
    # print(r1)
    # print(r2)

    # r1 = t.hosp_run_arima(['PAAAPTR650'], 2020)
    r2 = t.div_run_arima(['PAAAPTR650', 'PAAAPTB500', 'PAAAPSS32'], 2020)
    # print(r1)
    print(r2)
