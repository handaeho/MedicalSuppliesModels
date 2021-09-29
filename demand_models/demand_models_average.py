import pandas as pd
import statsmodels.api as sm
import numpy as np

np.random.seed(2)


class DemandAverageModel:
    """
    Since demand data only exists annual data, it is processed according to the annual data.
    """
    def __init__(self):
        print('******************** DEMAND AVG Model ********************')

    def calculate_avg(self, values_list):
        """
        A method that predicts the 'target' through the 'arithmetic mean' of the input object.

        :param values_list: training data(list of values) to be trained on the model.
        :return: list of predicted values using arithmetic average and actual values
        """
        y_pred = sum(values_list) / len(values_list)

        return y_pred

    def calculate_ma(self, train):
        """
        The moving average model method to be used with the yearly demand dataset coming in

        A method that predicts the 'future value' through the 'moving average' of the input object.

        :param train: training data(list of values) to be trained on the model.
        :return: predicted values using moving average and actual values
        """
        train_series = pd.Series(train)

        ma_result = train_series.rolling(window=3).mean().tolist()

        return ma_result

    def calculate_ols(self, values_list):
        """
        A method that predicts the 'future value' through the 'least squares' of the input object.

        :param values_list: training data(list of values) to be trained on the model.
        :return: predicted values using moving average and actual values
        """
        # x축
        x = list(range(1, len(values_list)+1, 1))

        # 예측하고자 하는 기간 'x+1'의 새로운 x축 상의 좌표
        x_new = list(range(len(x)+1, len(x)+2, 1))

        # y축: 사용량
        y = values_list

        olsmod = sm.OLS(y, x)
        olsres = olsmod.fit()

        # 학습기간의 회귀직선 추정치
        y_pred = olsres.predict(x)

        # 예측기간의 회귀직선 추정치
        y_pred_new = olsres.predict(x_new)

        return y_pred_new, y_pred
