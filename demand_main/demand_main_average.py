import pandas as pd
import numpy as np

from dataframe_pretreatment.demand_dataframe_pretreatment import drug_demand_pret, goods_demand_pret, train_target_split
from demand_models.demand_models_average import DemandAverageModel
from eval.evalulation import Evaluation

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class DemandRunAverage:
    def __init__(self):
        print('********** RUN DEMAND AVERAGE MODEL **********')
        self.avg = DemandAverageModel()
        self.evalu = Evaluation()

    def run_calculate_avg_drug(self, niin_list, year):
        avg_result_list = []

        for i in niin_list:
            df = drug_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            avg_pred_result = self.avg.calculate_avg(values_list=niin_data)

            avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelName', 'niin', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_calculate_avg_goods(self, niin_list, year):
        avg_result_list = []

        for i in niin_list:
            df = goods_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            avg_pred_result = self.avg.calculate_avg(values_list=niin_data)

            avg_result_list.append(['Average', i, avg_pred_result])

        r_df = pd.DataFrame(avg_result_list, columns=['modelName', 'niin', 'forecast'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_calculate_ma_drug(self, niin_list, year):
        ma_result_list = []

        for i in niin_list:
            df = drug_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:  # Since the data matrix of the niin can be Singular_matrix
                forecast = self.avg.calculate_ma(train=niin_data)

            except Exception:
                forecast = 'None', 'None'

            try:
                mae_y, rmsle_y = self.evalu.eval(niin_data[-3:], forecast[-3:])

            except Exception:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_y, rmsle_y = 'None', 'None'

            ma_result_list.append(['Moving_Average', i, niin_data, forecast[-1], rmsle_y, mae_y])

        r_df = pd.DataFrame(ma_result_list,
                            columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_calculate_ma_goods(self, niin_list, year):
        ma_result_list = []

        for i in niin_list:
            df = goods_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:  # Since the data matrix of the niin can be Singular_matrix
                forecast = self.avg.calculate_ma(train=niin_data)

            except Exception:
                forecast = 'None'

            try:
                mae_y, rmsle_y = self.evalu.eval(niin_data[-3:], forecast[-3:])

            except Exception:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_y, rmsle_y = 'None', 'None'

            ma_result_list.append(['Moving_Average', i, niin_data, forecast[-1], rmsle_y, mae_y])

        r_df = pd.DataFrame(ma_result_list,
                            columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_calculate_ols_drug(self, niin_list, year):
        ols_result_list = []

        for i in niin_list:
            df = drug_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:
                forecast, prediction = self.avg.calculate_ols(values_list=niin_data)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except:
                forecast, prediction = 'None', 'None'

            try:
                mae_y, rmsle_y = self.evalu.eval(niin_data, prediction)

            except Exception:  # If there is a negative number in the prediction, it cannot be calculated.
                rmsle_y, mae_y = 'None', 'None'

            ols_result_list.append(['Least_Square', i, niin_data, forecast, rmsle_y, mae_y])

        r_df = pd.DataFrame(ols_result_list,
                            columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_calculate_ols_goods(self, niin_list, year):
        ols_result_list = []

        for i in niin_list:
            df = goods_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:
                forecast, prediction = self.avg.calculate_ols(values_list=niin_data)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except:
                forecast, prediction = 'None', 'None'

            try:
                mae_y, rmsle_y = self.evalu.eval(niin_data, prediction)

            except Exception:  # If there is a negative number in the prediction, it cannot be calculated.
                rmsle_y, mae_y = 'None', 'None'

            ols_result_list.append(['Least_Square', i, niin_data, forecast, rmsle_y, mae_y])

        r_df = pd.DataFrame(ols_result_list,
                            columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json


if __name__ == '__main__':
    t = DemandRunAverage()

    print(t.run_calculate_avg_drug(['375020107'], 2020))
    print(t.run_calculate_avg_goods(['37X041427'], 2020))
    print(t.run_calculate_ma_drug(['375020107'], 2020))
    print(t.run_calculate_ma_goods(['37X041427'], 2020))
    print(t.run_calculate_ols_drug(['375020107'], 2020))
    print(t.run_calculate_ols_goods(['37X041427'], 2020))