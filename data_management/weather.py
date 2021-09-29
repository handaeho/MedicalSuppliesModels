import os
import pandas as pd

from flask import Flask, request, session


def weather():
    celsius_file_path = r'C:\UploadData\weather\celsius'
    rain_file_path = r'C:\UploadData\weather\rain'
    dust_file_path = r'C:\UploadData\weather\dust'

    select_list = request.get_json("testJson")

    if request.method == 'POST':
        fileLocation_celsius = list(select_list.values())[0]
        fileLocation_rain = list(select_list.values())[1]
        fileLocation_dust = list(select_list.values())[2]

    # fileLocation_celsius = ['ta_20201215172935.csv', 'ta_20201215172949.csv']
    # fileLocation_rain = ['rn_20201215173140.csv', 'rn_20201215173147.csv']
    # fileLocation_dust = ['OBS_부유분진_MNH_20201215173722.csv']

        # CSV 파일 한번에 읽어와 하나의 데이터프레임으로
        def get_celsius_rain_merged_csv(file_list, **kwargs):
            return pd.concat([pd.read_csv(f, **kwargs, encoding='CP949', skiprows=[0, 1, 2, 3, 4, 5, 6], header=0)
                              for f in file_list], ignore_index=True)

        def get_dust_csv(file_list, **kwargs):
            return pd.concat([pd.read_csv(f, **kwargs, encoding='CP949') for f in file_list], ignore_index=True)

        celsius_file_list = []
        for c in range(len(fileLocation_celsius)):
            celsius_file = os.path.join(celsius_file_path, '{}').format(fileLocation_celsius[c])
            celsius_file_list.append(celsius_file)
        celsius_df = get_celsius_rain_merged_csv(celsius_file_list, index_col=None)

        rain_file_list = []
        for r in range(len(fileLocation_rain)):
            rain_file = os.path.join(rain_file_path, '{}').format(fileLocation_rain[r])
            rain_file_list.append(rain_file)
        rain_df = get_celsius_rain_merged_csv(rain_file_list, index_col=None)

        dust_file_list = []
        for d in range(len(fileLocation_dust)):
            dust_file = os.path.join(dust_file_path, '{}').format(fileLocation_dust[d])
            dust_file_list.append(dust_file)
        dust_df = get_dust_csv(dust_file_list, index_col=None)

        celsius_t = celsius_df[['년월', '지점', '평균기온(℃)']].sort_values(['년월']).reset_index(drop=True)
        rain_t = rain_df[['년월', '지점', '강수량(mm)']].sort_values('년월').reset_index(drop=True)
        dust_t = dust_df[['일시', '지점명', '월 미세먼지 농도(㎍/㎥)']].sort_values('일시').reset_index(drop=True)

        celsius_t.columns = ['dates', 'location', 'celsius']
        rain_t.columns = ['dates', 'location', 'rain']
        dust_t.columns = ['dates', 'location', 'fine_dust']

        c_col_list = celsius_t.columns
        for col in range(len(c_col_list)):
            celsius_t = celsius_t.astype(({c_col_list[col]: 'str'}))

        r_col_list = rain_t.columns
        for col in range(len(r_col_list)):
            rain_t = rain_t.astype(({r_col_list[col]: 'str'}))

        d_col_list = dust_t.columns
        for col in range(len(d_col_list)):
            dust_t = dust_t.astype(({d_col_list[col]: 'str'}))

        def celsius_rain_loc_list(_list):
            for i in range(len(_list)):
                if _list[i] == '108':
                    _list[i] = '서울'
                elif _list[i] == '경남':
                    _list[i] = '경상도'
                elif _list[i] == '119':
                    _list[i] = '경기도'
                elif _list[i] == '강원영동':
                    _list[i] = '강원도'
                elif _list[i] == '충남':
                    _list[i] = '충청도'
                elif _list[i] == '제주':
                    _list[i] = '제주도'
                elif _list[i] == '전남':
                    _list[i] = '전라도'

            return _list

        def dust_loc_list(_list):
            for i in range(len(_list)):
                if _list[i] == '대관령':
                    _list[i] = '강원도'
                elif _list[i] == '서울':
                    _list[i] = '서울'
                elif _list[i] == '수원':
                    _list[i] = '경기도'
                elif _list[i] == '안면도(감)':
                    _list[i] = '충청도'
                elif _list[i] == '광주':
                    _list[i] = '전라도'
                elif _list[i] == '구덕산':
                    _list[i] = '경상도'
                elif _list[i] == '고산':
                    _list[i] = '제주도'

            return _list

        l_list_01 = celsius_rain_loc_list(celsius_t['location'].values)
        l_list_02 = celsius_rain_loc_list(rain_t['location'].values)
        l_list_03 = dust_loc_list(dust_t['location'].values)

        celsius_t['location'] = l_list_01
        rain_t['location'] = l_list_02
        dust_t['location'] = l_list_03

        cesius_rain = pd.merge(celsius_t, rain_t, on=['dates', 'location']).drop_duplicates().reset_index(drop=True)

        cesius_rain_dust = pd.merge(cesius_rain, dust_t,  how='left').drop_duplicates().reset_index(drop=True)

        return cesius_rain_dust


if __name__ == '__main__':
    print(weather())
