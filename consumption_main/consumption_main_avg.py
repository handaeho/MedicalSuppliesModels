import pandas as pd
import numpy as np

from dataframe_pretreatment.consumption_dataframe_pretreatment import select_hosp, select_div
from consumption_models.consumption_models_average import ConsumptionAverageModel
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class ConsumptionRunAvg:
    def __init__(self):
        print('******************** RUN CONSUM Avg, MA, OLS ********************')
        self.avg = ConsumptionAverageModel()
        self.eval = Evaluation()

    def hosp_run_avg(self, drugcode_list, year):
        avg_result_list = []

        for i in drugcode_list:
            df = select_hosp(drugcode=i, year=year)

            df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float',
                              'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

            true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

            avg_pred_result = self.avg.calculate_avg(values_list=true_m)

            avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelname', 'drugcode', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def div_run_avg(self, drugcode_list, year):
        avg_result_list = []

        for i in drugcode_list:
            if year == '2020':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2021':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2022':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2023':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            else:
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q',

                                        'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelname', 'drugcode', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def hosp_run_ma(self, drugcode_list, year):
        ma_result_list = []

        for i in drugcode_list:
            df = select_hosp(drugcode=i, year=year)

            df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float',
                              'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

            true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

            forecast = self.avg.calculate_ma(true_m)

            try:
                mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

            except:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_m, rmsle_m = 'None', 'None'

            ma_result_list.append(['Moving window', i, true_m, forecast[-1], rmsle_m, mae_m])

        r_df = pd.DataFrame(ma_result_list, columns=['modelName', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def div_run_ma(self, drugcode_list, year):
        ma_result_list = []

        for i in drugcode_list:
            if year == '2020':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-1:], forecast[-1:])

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving window', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2021':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-2:], forecast[-2:])

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving window', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2022':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving window', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2023':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving window', i, true_m, forecast[-1], rmsle_m, mae_m])

            else:
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q',
                                        'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving window', i, true_m, forecast[-1], rmsle_m, mae_m])

        r_df = pd.DataFrame(ma_result_list, columns=['modelName', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def hosp_run_ols(self, drugcode_list, year):
        ols_result_list = []

        for i in drugcode_list:
            df = select_hosp(drugcode=i, year=year)

            df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float',
                              'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

            true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

            try:
                forecast, prediction = self.avg.calculate_ols(true_m)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except:  # If there is no data you are looking for in the dataset.
                forecast, prediction = 'None', 'None'

            try:
                mae_m, rmsle_m = self.eval.eval(true_m, prediction)

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae_m, rmsle_m = 'None', 'None'

            ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(ols_result_list, columns=['modelname', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def div_run_ols(self, drugcode_list, year):
        ols_result_list = []

        for i in drugcode_list:
            if year == '2020':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2021':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_4_q': 'float', 'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2022':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2023':
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_6_q', 'x_5_q', 'x_4_q', 'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            else:
                df = select_div(drugcode=i, year=year)

                df_t = df.astype({'x_7_q': 'float', 'x_6_q': 'float', 'x_5_q': 'float', 'x_4_q': 'float',
                                  'x_3_q': 'float', 'x_2_q': 'float', 'x_1_q': 'float'})

                true_m = np.array(df_t[['x_7_q', 'x_6_q', 'x_5_q', 'x_4_q',
                                        'x_3_q', 'x_2_q', 'x_1_q']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is no data you are looking for in the dataset.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(ols_result_list, columns=['modelname', 'drugcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json


if __name__ == '__main__':
    t = ConsumptionRunAvg()

    # r1 = t.hosp_run_avg(['PAA5L100'], 2020)
    # r2 = t.div_run_avg(['PAA5L100'], 2020)
    # r3 = t.hosp_run_ma(['PAAAPSS32'], 2020)
    # r4 = t.div_run_ma(['PAAAPSS32'], 2020)
    # r5 = t.hosp_run_ols(['PAAAPSS32'], 2020)
    # r6 = t.div_run_ols(['PAAAPSS32'], 2020)
    # print(r1)
    # print(r2)
    # print(r3)
    # print(r4)
    # print(r5)
    # print(r6)

    # r1 = t.hosp_run_avg(['PAAAPTR650'], 2020)
    r2 = t.div_run_avg(['PAAAPTR650', 'PAAAPTB500', 'PAAAPSS32'], 2020)
    # r3 = t.hosp_run_ma(['PAAAPTR650'], 2020)
    r4 = t.div_run_ma(['PAAAPTR650', 'PAAAPTB500', 'PAAAPSS32'], 2020)
    # r5 = t.hosp_run_ols(['PAAAPTR650'], 2020)
    r6 = t.div_run_ols(['PAAAPTR650', 'PAAAPTB500', 'PAAAPSS32'], 2020)
    # print(r1)
    print(r2)
    # print(r3)
    print(r4)
    # print(r5)
    print(r6)