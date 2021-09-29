import pandas as pd
import numpy as np

from eval.evalulation import Evaluation
from dataframe_pretreatment.demand_dataframe_pretreatment import drug_demand_pret, goods_demand_pret, train_target_split
from demand_models.demand_model_arma import DemandARMAModel

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class DemandRunARMA:
    def __init__(self):
        print('******************** RUN DEMAND ARMA ********************')

        # 클래스 인스턴스
        self.arma = DemandARMAModel()
        self.evalu = Evaluation()

    def run_arma_drug(self, niin_list, year):
        result_list = []

        for i in niin_list:
            df = drug_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:  # Since the data matrix of the niin can be Singular_matrix
                forecast, prediction = self.arma.arma(niin_data)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except Exception:
                forecast, prediction = 'None', 'None'

            try:
                mae_y, rmsle_y = self.evalu.eval(niin_data, prediction)

            except Exception:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_y, rmsle_y = 'None', 'None'

            result_list.append(['ARMA', i, niin_data, forecast, rmsle_y, mae_y])

        r_df = pd.DataFrame(result_list,
                            columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json

    def run_arma_goods(self, niin_list, year):
        result_list = []

        for i in niin_list:
            df = goods_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:  # Since the data matrix of the niin can be Singular_matrix
                forecast, prediction = self.arma.arma(niin_data)
                forecast = float(forecast)
                prediction = list(map(float, prediction))
                if forecast < 0:
                    forecast = 0

            except:
                forecast, prediction = 'Singular_matrix', 'Singular_matrix'

            try:
                mae_y, rmsle_y = self.evalu.eval(niin_data, prediction)

            except Exception:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_y, rmsle_y = 'None', 'None'

            result_list.append(['ARMA', i, niin_data, forecast, rmsle_y, mae_y])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        r_df_json = r_df.to_json(orient='records')

        return r_df_json


if __name__ == '__main__':
    t = DemandRunARMA()

    d_result = t.run_arma_drug(['001288035'], 2020)
    # g_result = t.run_arma_goods(['37X041427'])

    print(d_result)
    # print(g_result)