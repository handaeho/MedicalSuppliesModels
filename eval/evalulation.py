import warnings
import numpy as np
import math

from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_squared_log_error, r2_score

warnings.filterwarnings("ignore")


class Evaluation:
    def __init__(self):
        print('******************** Evaluation ********************')

    def eval(self, true, pred):
        """
        Result Eval

        entered truth values and prediction values are must be of type list
        so, if it's not a list, the parameter are enter will change its type to list.

        mse: mean_squared_error
        rmse: root mean_squared_error
        mae: mean_squared_error
        rmsle: mean_squared_log_error
        r2: r2 score

        :param true: True Values
        :param pred: Prediction Values
        :return: MAE, MSE, RMSLE
        """
        # Make input true and predicted values into 1D list type
        true_arr = np.array(true).flatten()
        pred_arr = np.array(pred).flatten()

        mae_val = mean_absolute_error(true_arr, pred_arr)
        mse_val = mean_squared_error(true_arr, pred_arr)
        rmse_val = math.sqrt(mse_val)
        rmsle_val = mean_squared_log_error(true_arr, pred_arr)
        r2_val = r2_score(true_arr, pred_arr)

        return mae_val, rmsle_val
