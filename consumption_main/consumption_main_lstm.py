import pandas as pd
import numpy as np

from dataframe_pretreatment.consumption_dataframe_pretreatment import select_hosp, select_div
from consumption_models.consumption_model_lstm import ConsumptionLSTMModel
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class ConsumptionRunLSTM:
    def __init__(self):
        print('******************** RUN CONSUM LSTM ********************')
        # 클래스 인스턴스
        self.hosp_lstm = ConsumptionLSTMModel(5)
        # self.div_lstm = ConsumptionLSTMModel(1)
        # self.div_lstm = ConsumptionLSTMModel(1)
        self.eval = Evaluation()

    def hosp_run_lstm(self, drugcode_list, year):
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
                forecast = self.hosp_lstm.lstm(x=true_m)
                forecast = float(forecast)
                if forecast < 0:
                    forecast = 0

            except:
                forecast = 'None'

            try:
                mae, rmsle = self.eval.eval(true_m[-1], forecast)

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae, rmsle = 'None', 'None'

            result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json

    def div_run_lstm(self, drugcode_list, year):
        """
        division dataset's range is base year - 7.
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

                div_lstm = ConsumptionLSTMModel(1)

                df_t = df.astype({'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2021':
                df = select_div(drugcode=i, year=year)

                div_lstm = ConsumptionLSTMModel(2)

                df_t = df.astype({'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2022':
                df = select_div(drugcode=i, year=year)

                div_lstm = ConsumptionLSTMModel(3)

                df_t = df.astype({'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            elif year == '2023':
                df = select_div(drugcode=i, year=year)

                div_lstm = ConsumptionLSTMModel(4)

                df_t = df.astype({'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

            else:
                df = select_div(drugcode=i, year=year)

                div_lstm = ConsumptionLSTMModel(5)

                df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q',
                                        'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast = div_lstm.lstm(x=true_m)
                    forecast = float(forecast)
                    if forecast < 0:
                        forecast = 0

                except:
                    forecast = 'None'

                try:
                    mae, rmsle = self.eval.eval(true_m[-1], forecast)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae, rmsle = 'None', 'None'

                result_list.append(['LSTM', i, true_m, forecast, rmsle, mae])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json


if __name__ == '__main__':
    t = ConsumptionRunLSTM()

    # r1 = t.hosp_run_lstm(['PAAAPTR650'], 2020)
    # r2 = t.div_run_lstm(['PAAAPTR650'], 2020)
    # print(r1)
    # print(r2)

    # r1 = t.hosp_run_lstm(['PAAAPTR650'], 2020)
    r2 = t.div_run_lstm(['PAAAPTR650', 'PAAAPTB500', 'PAAAPSS32'], 2020)
    # print(r1)
    print(r2)
