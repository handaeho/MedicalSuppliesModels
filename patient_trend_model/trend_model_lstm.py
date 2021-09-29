import warnings
import pandas as pd
import numpy as np
import time
import tensorflow as tf

from tensorflow.python.keras.callbacks import EarlyStopping
from tensorflow.python.keras.layers import Dense, LSTM
from tensorflow.python.keras import Sequential
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:.5f}'.format

sc = MinMaxScaler()

start_time = time.time()

tf.random.set_seed(1)
np.random.seed(1)


class PatientTrendLSTMModel:
    def __init__(self, n_steps):
        print('******************** LSTM ********************')
        self.n_steps = n_steps
        self.n_features = 1
        self.n_targets = 1

    def split_sequence(self, values_list):
        """
        return X: train_set / y: label_set(target_set)

        :param values_list: list of drugcode & values dict entered by the user([{drugcode: demand values}]
        :return: train_set X / target_set y
        """
        x, y = list(), list()

        for i in range(len(values_list)):
            # find the end of this pattern
            end_ix = i + self.n_steps
            # check if we are beyond the sequence
            if end_ix > len(values_list) - self.n_targets:
                break
            # gather input and output parts of the pattern
            seq_x, seq_y = values_list[i:end_ix], values_list[end_ix:(end_ix + self.n_targets)]

            x.append(seq_x)
            y.append(seq_y)

        return np.array(x), np.array(y)

    def lstm(self, x):
        """
        LSTM model.

        Learn with x sliced in 'n_steps'.
        The label of X is y expressed as the value of 'n_features'.
        x is a value from 'x-7 ~ x-1', divided by 'n_steps' value.
        y is 'x', which is equal to the value of 'n_features' value.
        And the model trained in this way receives 'x-7 ~ x' again, predicts 'x+1', and returns.

        :param x: train dataset
        :return: prediction of x+1
        """
        best_pred_list = []
        best_node_list = []
        best_loss_list = []

        _x = np.reshape(x, (-1, 1))

        x_sc = sc.fit_transform(_x)

        x_train, y_train = self.split_sequence(x_sc)
        x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], self.n_features))
        y_train = np.reshape(y_train, (y_train.shape[0], y_train.shape[1]))

        x_input = x[-self.n_steps:]
        x_input_sc = np.reshape(x_input, (-1, 1))
        x_input_fin = sc.transform(x_input_sc).reshape((1, self.n_steps, self.n_features))

        start_nodenum = 2
        end_nodenum = 11

        for i in range(start_nodenum, end_nodenum):
            model = Sequential()

            # input shape: (train values list length, target value list length)
            model.add(LSTM(i, activation='relu', return_sequences=True, input_shape=(self.n_steps, self.n_features)))

            model.add(LSTM(10, activation='relu', return_sequences=False))

            model.add(Dense(self.n_targets))  # output

            model.compile(loss='mse', optimizer='adam')

            early_stop = EarlyStopping(monitor='loss', patience=0, verbose=0,
                                       mode='auto', baseline=None, restore_best_weights=False)

            # hist = model.fit(x_train, y_train, epochs=300, batch_size=self.n_steps, verbose=1, callbacks=[early_stop])
            hist = model.fit(x_train, y_train, epochs=300, batch_size=self.n_steps, verbose=1)

            # predict
            globals()['prediction_{}'.format(i)] = model.predict(x_input_fin)

            # loss
            globals()['min_loss_{}'.format(i)] = min(hist.history['loss'])

        list_loss = []
        for m in range(start_nodenum, end_nodenum):
            list_loss.append(globals()['min_loss_{}'.format(m)])

        # index_loss_list = [node, loss, prediction]
        index_loss_list = []
        for i, n in enumerate(list_loss, start=start_nodenum):
            index_loss_list.append([i, n,
                                    sc.inverse_transform(globals()['prediction_{}'.format(i)])[0]])

        index_loss_df = pd.DataFrame(index_loss_list, columns=['node', 'loss', 'pred'])

        # lowest loss
        best_loss_pred = index_loss_df[index_loss_df['loss'] == min(index_loss_df['loss'])]
        best_loss_val = best_loss_pred['loss'].values[0]

        # save to lowest loss's node
        best_loss_node = best_loss_pred['node'].values[0]

        # lowest loss's prediction and return as result
        p_result = globals()['prediction_{}'.format(best_loss_node)]

        # result to inverse transform(true value)
        pred_data = sc.inverse_transform(p_result)

        # result list
        best_pred_list.append(pred_data[0][0])
        # result's node list
        best_node_list.append(best_loss_node)
        # result's loss list
        best_loss_list.append(best_loss_val)

        # Running Time
        end_time = time.time()
        print("WorkingTime: {:.2f} sec".format(end_time - start_time))

        return best_pred_list[0]
