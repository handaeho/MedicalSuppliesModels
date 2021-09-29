import warnings
import pandas as pd
import statsmodels.api as sm
import numpy as np

warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:.5f}'.format

np.random.seed(2)


class DemandARMAModel:
    def __init__(self):
        print('******************** DEMAND ARMA Model ********************')

    def arma(self, train):
        """
        ARMA Modeling
        """
        model_ARMA = sm.tsa.ARMA(train, order=(1, 0))

        result_ARMA = model_ARMA.fit(trend='nc', disp=0)

        # forecast values
        forecast = list(result_ARMA.forecast())[0]

        # prediction values
        prediction = result_ARMA.predict().tolist()

        return forecast, prediction
