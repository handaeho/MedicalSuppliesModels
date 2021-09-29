import pandas as pd
import numpy as np
import statsmodels.api as sm

np.random.seed(1)


class PatientTrendAverageModel:
    def __init__(self):
        print('******************** RUN Average / Moving average / OLS ********************')

    def calculate_avg(self, values_list):
        """
        A method that predicts the 'future value' through the 'arithmetic mean' of the input object.

        :param values_list: training data(list of values) to be trained on the model.
        :return: list of predicted values using arithmetic average and actual values
        """
        y_pred = sum(values_list) / len(values_list)

        return y_pred

    def calculate_ma(self, train):
        """
        The moving average model method to be used with the 'monthly' patient trend dataset coming in

        A method that predicts the 'future value' through the 'moving average' of the input object.

        :param train: training dataframe to be trained on the model.
        :return: predicted values using moving average and actual values
        """
        train_series = pd.Series(train)

        ma_result = train_series.rolling(window=3).mean().tolist()

        return ma_result

    def calculate_ols(self, train):
        """
        The least squares model method to be used with the 'monthly' patient trend dataset coming in

        A method that predicts the 'future value' through the 'least squares' of the input object.

        :param train: training dataframe to be trained on the model.
        :return: predicted values using moving average and actual values
        """
        # x축
        x = list(range(1, len(train) + 1, 1))

        # # 예측하고자 하는 기간 'x+1'의 새로운 x축 상의 좌표
        x_new = list(range(len(x) + 1, len(x) + 2, 1))

        # y축: 사용량
        y = train

        olsmod = sm.OLS(y, x)
        olsres = olsmod.fit()

        # 학습기간의 회귀직선 추정치
        y_pred = olsres.predict(x)

        # 예측기간의 회귀직선 추정치
        y_fore_new = olsres.predict(x_new)
        y_fore = sum(y_fore_new)

        return y_fore, y_pred
