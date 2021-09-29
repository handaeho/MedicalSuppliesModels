import pandas as pd


class Correlation:
    def __init__(self):
        print('****************** Correlation ******************')

    def corr(self, celsius_list, rain_list, dust_list, quantity_list):
        weather_df = pd.DataFrame(
            {'celsius': celsius_list, 'rain': rain_list, 'fine_dust': dust_list, 'quantity': quantity_list})

        pearson_corr = weather_df.corr()

        corr_celsius_rain_dust = pearson_corr['quantity']

        result_json = corr_celsius_rain_dust.to_json(orient='columns')

        return result_json
