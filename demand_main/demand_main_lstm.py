import pandas as pd
import numpy as np

from dataframe_pretreatment.demand_dataframe_pretreatment import drug_demand_pret, goods_demand_pret, train_target_split
from eval.evalulation import Evaluation
from demand_models.demand_model_lstm import DemandLstmModel

# 지수표현식으로 출력 X
pd.options.display.float_format = '{:.5f}'.format


class DemandRunLSTM:
    def __init__(self):
        print('******************** RUN DEMAND LSTM ********************')

        # 클래스 인스턴스
        self.lstm = DemandLstmModel(5)
        self.eval = Evaluation()

    def run_lstm_drug(self, niin_list, year):
        result_list = []

        for i in niin_list:
            df = drug_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:
                forecast = self.lstm.lstm(x=niin_data)
                forecast = float(forecast)
                if forecast < 0:
                    forecast = 0

            except:
                forecast = 'None'

            try:
                mae_y, rmsle_y = self.eval.eval(niin_data[-1], forecast)

            except:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_y, rmsle_y = 'None', 'None'

            result_list.append(['LSTM', i, niin_data, forecast, rmsle_y, mae_y])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json

    def run_lstm_goods(self, niin_list, year):
        result_list = []

        for i in niin_list:
            df = goods_demand_pret(niin=i, year=year)

            niin_data = np.array(df[['x_11', 'x_10', 'x_9', 'x_8', 'x_7', 'x_6',
                                     'x_5', 'x_4', 'x_3', 'x_2', 'x_1']]).flatten()

            try:
                forecast = self.lstm.lstm(x=niin_data)
                forecast = float(forecast)
                if forecast < 0:
                    forecast = 0

            except:
                forecast = 'None'

            try:
                mae_y, rmsle_y = self.eval.eval(niin_data[-1], forecast)

            except:  # If there is a negative number in the prediction, it cannot be calculated.
                mae_y, rmsle_y = 'None', 'None'

            result_list.append(['LSTM', i, niin_data, forecast, rmsle_y, mae_y])

        r_df = pd.DataFrame(result_list, columns=['modelName', 'niin', 'true', 'forecast', 'rmsle', 'mae'])

        result_json = r_df.to_json(orient='records')

        return result_json


if __name__ == '__main__':
    t = DemandRunLSTM()

    d_result = t.run_lstm_drug(['001288035'], 2020)
    # g_result = t.run_lstm_goods(['37X041427'])

    print(d_result)
    # print(g_result)
