import warnings
import pandas as pd
import numpy as np
import statsmodels.api as sm

warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:.5f}'.format

np.random.seed(1)


class PatientTrendARMAModel:
    def __init__(self):
        print('******************** ARMA ********************')

    def arma(self, train):
        """
        ARMA Modeling

        :param train: model training set
        :return: prediction / truth
        """
        model_ARMA = sm.tsa.ARMA(train, order=(1, 0))

        result_ARMA = model_ARMA.fit(trend='nc', disp=0)  # trend=트렌드(추세), disp=수렴정보

        # forecast values
        forecast = list(result_ARMA.forecast())[0]

        # prediction values
        prediction = result_ARMA.predict()

        return forecast, prediction
