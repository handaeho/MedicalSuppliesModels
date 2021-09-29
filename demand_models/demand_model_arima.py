import warnings
import itertools
import pandas as pd
import numpy as np
import statsmodels.api as sm

warnings.filterwarnings("ignore")

np.random.seed(2)


class DemandARIMAModels:
    def __init__(self):
        print('******************** DEMAND ARIMA Model ********************')

    def arima(self, train):
        """
        ARIMA Model.

        :param train: model training set
        :return: prediction / truth
        """
        ################
        def aic(data):
            """
            AIC: How the least information billing model is chosen as the most data
            and cost model A low AIC value means a good fit.
            """
            p = d = q = range(0, 2)
            pdq = list(itertools.product(p, d, q))
            seasonal_pdq = [[x[0], x[1], x[2], 2] for x in list(itertools.product(p, d, q))]

            aic = []

            for param in pdq:
                for param_seasonal in seasonal_pdq:
                    mod = sm.tsa.statespace.SARIMAX(data,
                                                    order=param,
                                                    seasonal_order=param_seasonal,
                                                    enforce_stationarity=False,
                                                    enforce_invertibility=True,
                                                    initialize='approximate_diffuse',
                                                    trend='n')

                    start_param = np.r_[[0] * (mod.k_params - 1), 1]

                    result = mod.fit(start_params=start_param, method='bfgs', disp=False)

                    aic.append([param, param_seasonal, result.aic])

            aic_df = pd.DataFrame(aic, columns=['ARIMA_param', 'param_seasonal', 'AIC'])
            best_aic = min(aic_df['AIC'].astype({'AIC': 'float'}))
            best_param = aic_df[aic_df['AIC'] == best_aic]

            param_list = list(best_param['ARIMA_param'].values[0])

            param_season_list = list(best_param['param_seasonal'].values[0])

            return param_list, param_season_list

        ################
        param_list, param_season_list = aic(train)

        p1 = param_list[0]
        p2 = param_list[1]
        p3 = param_list[2]

        p_s_1 = param_season_list[0]
        p_s_2 = param_season_list[1]
        p_s_3 = param_season_list[2]
        p_s_4 = param_season_list[3]

        mod = sm.tsa.statespace.SARIMAX(train,
                                        order=(p1, p2, p3),
                                        seasonal_order=(p_s_1, p_s_2, p_s_3, p_s_4),
                                        enforce_stationarity=False,
                                        enforce_invertibility=True,
                                        initialize='approximate_diffuse',
                                        trend='n')

        start_params = np.r_[[0] * (mod.k_params - 1), 1]

        results = mod.fit(start_params=start_params, method='bfgs', disp=False)

        # forecast 예측
        m_fore = results.forecast()
        forecast = sum(m_fore)

        # prediction 예측
        prediction = results.predict()

        return forecast, prediction
