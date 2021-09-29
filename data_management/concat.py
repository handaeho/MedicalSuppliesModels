import pandas as pd

avg_r = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_avg.xlsx')
ma_r = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_ma.xlsx')
ols_r = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_ols.xlsx')
arma_r = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_arma.xlsx')
arima_r = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_arima.xlsx')
lstm_r_01 = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_lstm_01.xlsx')
lstm_r_02 = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_lstm_02.xlsx')
lstm_r_03 = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_lstm_03.xlsx')
lstm_r_04 = pd.read_excel('C:\\Users\\gaion\\PycharmProjects\\runModel_ver3.1\\result_consum_div_1209\\result_consum_div_lstm_04.xlsx')

lstm_01_02 = pd.concat([lstm_r_01, lstm_r_02], axis=0).reset_index(drop=True)
lstm_01_02_03 = pd.concat([lstm_01_02, lstm_r_03], axis=0).reset_index(drop=True)
lstm_01_02_03_04 = pd.concat([lstm_01_02_03, lstm_r_04], axis=0).reset_index(drop=True)

avg_r.columns = ['modelname_avg', 'drugcode', 'before_avg', 'true_avg', 'forecast_avg']
ma_r.columns = ['modelname_ma', 'drugcode', 'before_ma', 'true_ma', 'forecast_ma', 'rmsle_ma', 'mae_ma']
ols_r.columns = ['modelnameols', 'drugcode', 'before_ols', 'true_ols', 'forecast_ols', 'rmsle_ols', 'mae_ols']
arma_r.columns = ['modelname_arma', 'drugcode', 'before_arma', 'true_arma', 'forecast_arma', 'rmsle_arma', 'mae_arma']
arima_r.columns = ['modelname_arima', 'drugcode', 'before_arima', 'true_arima', 'forecast_arima', 'rmsle_arima', 'mae_arima']
lstm_01_02_03_04.columns = ['modelname_lstm', 'drugcode', 'before_lstm', 'true_lstm', 'forecast_lstm', 'rmsle_lstm', 'mae_lstm']

avg_ma = pd.merge(avg_r, ma_r, on='drugcode')
avg_ma_ols = pd.merge(avg_ma, ols_r, on='drugcode')
avg_ma_ols_arma = pd.merge(avg_ma_ols, arma_r, on='drugcode')
avg_ma_ols_arma_arima = pd.merge(avg_ma_ols_arma, arima_r, on='drugcode')
avg_ma_ols_arma_arima_lstm = pd.merge(avg_ma_ols_arma_arima, lstm_01_02_03_04, on='drugcode')

print(avg_ma_ols_arma_arima_lstm)
avg_ma_ols_arma_arima_lstm.to_excel('t.xlsx')

