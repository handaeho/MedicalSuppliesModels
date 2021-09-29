import pandas as pd
import numpy as np

from dataframe_pretreatment.trend_dataframe_pretreatment import select_hosp, select_div, select_all_loc_hosp, select_all_loc_div
from patient_trend_model.trend_model_average import PatientTrendAverageModel
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class PatientTrendRunAvg:
    def __init__(self):
        print('******************** RUN TREND AVG ********************')

        self.avg = PatientTrendAverageModel()
        self.eval = Evaluation()

    def run_avg_all_loc_hosp(self, userselect):
        avg_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            df = select_all_loc_hosp(diag_code=i, in_out=inout, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            avg_pred_result = self.avg.calculate_avg(values_list=true_m)

            avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelname', 'diagcode', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_avg_all_loc_div(self, userselect):
        avg_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            if year == '2020':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2021':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2022':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2023':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            else:
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelname', 'diagcode', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_avg_hosp(self, userselect):
        avg_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            df = select_hosp(diag_code=i, in_out=inout, location=loc, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            avg_pred_result = self.avg.calculate_avg(values_list=true_m)

            avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelname', 'diagcode', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_avg_div(self, userselect):
        avg_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            if year == '2020':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2021':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2022':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            elif year == '2023':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

            else:
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                avg_pred_result = self.avg.calculate_avg(values_list=true_m)

                avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelname', 'diagcode', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ma_all_loc_hosp(self, userselect):
        ma_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            df = select_all_loc_hosp(diag_code=i, in_out=inout, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            forecast = self.avg.calculate_ma(true_m)

            try:
                mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

            except:   # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae_m, rmsle_m = 'None', 'None'

            ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

        r_df = pd.DataFrame(ma_result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ma_all_loc_div(self, userselect):
        ma_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            if year == '2020':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-1:], forecast[-1:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2021':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-2:], forecast[-2:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2022':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2023':
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            else:
                df = select_all_loc_div(diag_code=i, in_out=inout, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

        r_df = pd.DataFrame(ma_result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ma_hosp(self, userselect):
        ma_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            df = select_hosp(diag_code=i, in_out=inout, location=loc, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

            forecast = self.avg.calculate_ma(true_m)

            try:
                mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

            except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                mae_m, rmsle_m = 'None', 'None'

            ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

        r_df = pd.DataFrame(ma_result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ma_div(self, userselect):
        ma_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            if year == '2020':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-1:], forecast[-1:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2021':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-2:], forecast[-2:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2022':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            elif year == '2023':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

            else:
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                forecast = self.avg.calculate_ma(true_m)

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m[-3:], forecast[-3:])

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ma_result_list.append(['Moving Average', i, true_m, forecast[-1], rmsle_m, mae_m])

        r_df = pd.DataFrame(ma_result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ols_all_loc_hosp(self, userselect):
        ols_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            df = select_all_loc_hosp(in_out=inout, diag_code=i, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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

        r_df = pd.DataFrame(ols_result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ols_all_loc_div(self, userselect):
        ols_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        year = userselect[2]

        for i in diagcode_list:
            if year == '2020':
                df = select_all_loc_div(in_out=inout, diag_code=i, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2021':
                df = select_all_loc_div(in_out=inout, diag_code=i, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2022':
                df = select_all_loc_div(in_out=inout, diag_code=i, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            elif year == '2023':
                df = select_all_loc_div(in_out=inout, diag_code=i, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

            else:
                df = select_all_loc_div(in_out=inout, diag_code=i, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

                try:
                    forecast, prediction = self.avg.calculate_ols(true_m)
                    forecast = float(forecast)
                    prediction = list(map(float, prediction))
                    if forecast < 0:
                        forecast = 0

                except:  # If there is a negative number in the prediction, it cannot be calculated.
                    forecast, prediction = 'None', 'None'

                try:
                    mae_m, rmsle_m = self.eval.eval(true_m, prediction)

                except:  # If there is a negative or complex number in the prediction, it cannot be calculated.
                    mae_m, rmsle_m = 'None', 'None'

                ols_result_list.append(['Least Square Method', i, true_m, forecast, rmsle_m, mae_m])

        r_df = pd.DataFrame(ols_result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ols_hosp(self, userselect):
        ols_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            df = select_hosp(in_out=inout, location=loc, diag_code=i, year=year)

            df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float',
                              'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

            true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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

        r_df = pd.DataFrame(ols_result_list, columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_ols_div(self, userselect):
        ols_result_list = []

        diagcode_list = userselect[0]
        inout = userselect[1]
        loc = userselect[2]
        year = userselect[3]

        for i in diagcode_list:
            if year == '2020':
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_4_c': 'float', 'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_6_c', 'x_5_c', 'x_4_c', 'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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
                df = select_div(in_out=inout, location=loc, diag_code=i, year=year)

                df_t = df.astype({'x_7_c': 'float', 'x_6_c': 'float', 'x_5_c': 'float', 'x_4_c': 'float',
                                  'x_3_c': 'float', 'x_2_c': 'float', 'x_1_c': 'float'})

                true_m = np.array(df_t[['x_7_c', 'x_6_c', 'x_5_c', 'x_4_c',
                                        'x_3_c', 'x_2_c', 'x_1_c']].values).flatten()

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

        r_df = pd.DataFrame(ols_result_list,
                            columns=['modelName', 'diagcode', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json


if __name__ == '__main__':
    t = PatientTrendRunAvg()

    r1 = t.run_avg_all_loc_hosp([["J00"], "('외래', '입원')", 2020])
    r2 = t.run_avg_hosp([["J00"], "('외래', '입원')", "강원도", 2020])
    r3 = t.run_avg_all_loc_div([["J00", "A04.9-01"], "('외래', '입원')", 2020])
    r4 = t.run_avg_div([["J00", "A04.9-01"], "('외래', '입원')", "강원도", 2020])

    r5 = t.run_ma_all_loc_hosp([["J00"], "('외래', '입원')", 2020])
    r6 = t.run_ma_hosp([["J00"], "('외래', '입원')", "강원도", 2020])
    r7 = t.run_ma_all_loc_div([["J00", "A04.9-01"], "('외래', '입원')", 2020])
    r8 = t.run_ma_div([["J00", "A04.9-01"], "('외래', '입원')", "강원도", 2020])

    r9 = t.run_ols_all_loc_hosp([["J00"], "('외래', '입원')", 2020])
    r10 = t.run_ols_hosp([["J00"], "('외래', '입원')", "강원도", 2020])
    r11 = t.run_ols_all_loc_div([["J00", "A04.9-01"], "('외래', '입원')", 2020])
    r12 = t.run_ols_div([["J00", "A04.9-01"], "('외래', '입원')", "강원도", 2020])

    print(r1)
    print(r2)
    print(r3)
    print(r4)
    print(r5)
    print(r6)
    print(r7)
    print(r8)
    print(r9)
    print(r10)
    print(r11)
    print(r12)