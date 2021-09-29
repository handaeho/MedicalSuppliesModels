import jellyfish as jf
import pandas as pd

file_path = '/\\'


def ingr_master():
    # file_path = 'C:\\UploadData\\'
    chief = pd.read_csv(file_path + '건강보험심사평가원_의약품_주성분_정보_2019_12.csv', encoding='CP949')
    master = pd.read_csv(file_path + '건강보험심사평가원_의약품_표준코드_마스터_2019_11.csv', encoding='CP949')

    chief_data = chief[['코드', '주성분명칭', '함량', '단위']]
    master_data = master[['한글상품명', '일반명코드(성분명코드)']]

    chief_data['성분함량단위'] = chief_data['주성분명칭'] + ' ' + chief_data['함량'] + chief_data['단위']

    c_df = chief_data[['코드', '성분함량단위']].dropna(axis=0).drop_duplicates().reset_index(drop=True)
    m_df = master_data[['한글상품명', '일반명코드(성분명코드)']].dropna(axis=0).drop_duplicates().reset_index(drop=True)

    c_df.columns = ['ingr_code', 'ingr_name']
    m_df.columns = ['ingr_drug_name', 'ingr_code']

    m_c_df = pd.merge(c_df, m_df, on='ingr_code').dropna(axis=0).drop_duplicates().reset_index(drop=True)

    result = m_c_df[['ingr_code', 'ingr_drug_name', 'ingr_name']].drop_duplicates().reset_index(drop=True)

    return result


def dcode_dname_unit():
    info = pd.read_csv(file_path + '약품정보현황1_수도_회사삭제.csv', encoding='CP949')

    info_df = info[['NIIN', '약품코드', '성분', '조제명']]

    info_df.columns = ['niin', 'drug_code', 'ingr_name', 'ingr_drug_name']

    return info_df


def icode_dcode():
    # file_path = 'C:\\UploadData\\'

    # TODO: DB에서 불러오기 (약품정보현황, 주성분&마스터)
    info = pd.read_csv(file_path + '약품정보현황1_수도_회사삭제.csv', encoding='CP949')

    info_df = info[['NIIN', '약품코드', '성분', '조제명']]

    info_df.columns = ['niin', 'drug_code', 'ingr_name', 'ingr_drug_name']

    ingred_master = pd.read_csv(file_path + 'test001.csv')

    def icode_dcode_01(ingr_master=ingred_master, drug_info=info_df):
        cnt = 0

        ingr_master_col_list = list(ingr_master.columns)
        for n in range(len(ingr_master_col_list)):
            ingr_master = ingr_master.astype({ingr_master_col_list[n]: 'str'})

        drug_info_col_list = list(drug_info.columns)
        for n in range(len(drug_info_col_list)):
            drug_info = drug_info.astype({drug_info_col_list[n]: 'str'})

        icode_dcode_list = []
        icode_dcode_list_append = icode_dcode_list.append

        iname_list = []
        iname_list_append = iname_list.append

        ingr_code = ingr_master['ingr_code'].values
        ingr_name = ingr_master['ingr_name'].values

        drug_info_code = drug_info['drug_code'].values
        drug_info_ingr_name = drug_info['ingr_name'].values

        for i in range(len(ingr_master)):
            cnt += 1
            if cnt % 1000 == 0:
                print(cnt)
                print(round((cnt / len(ingr_master)) * 100, 3))
            for j in range(len(drug_info)):
                if jf.jaro_distance(ingr_name[i].lower(), drug_info_ingr_name[j].lower()) > 0.985:
                    icode_dcode_list_append([ingr_code[i], drug_info_code[j]])
                    iname_list_append([ingr_name[i], drug_info_ingr_name[j]])

        icode_dcode_df = pd.DataFrame(icode_dcode_list,
                                      columns=['ingr_code', 'drug_code'])
        iname_df = pd.DataFrame(iname_list,
                                columns=['ingredient_name_ingred', 'ingredient_name_info'])

        result_icode_dcode_df = icode_dcode_df.drop_duplicates().reset_index(drop=True)
        result_iname_df = iname_df.drop_duplicates().reset_index(drop=True)

        return result_icode_dcode_df, result_iname_df

    def icode_dcode_02(ingr_master=ingred_master, drug_info=info_df):
        cnt = 0

        ingr_col_list = list(ingr_master.columns)
        for n in range(len(ingr_col_list)):
            ingr_master = ingr_master.astype({ingr_col_list[n]: 'str'})

        drug_info_col_list = list(drug_info.columns)
        for n in range(len(drug_info_col_list)):
            drug_info = drug_info.astype({drug_info_col_list[n]: 'str'})

        icode_dcode_list = []
        icode_dcode_list_append = icode_dcode_list.append

        ingr_name_list = []
        ingr_name_list_append = ingr_name_list.append

        ingr_code = ingr_master['ingr_code'].values
        ingr_name = ingr_master['ingr_drug_name'].values

        drug_info_drug_code = drug_info['drug_code'].values
        drug_info_drug_name = drug_info['ingr_drug_name'].values

        for i in range(len(ingr_master)):
            cnt += 1
            if cnt % 1000 == 0:
                print(cnt)
                print(round((cnt / len(ingr_master)) * 100, 3))
            for j in range(len(drug_info)):
                if jf.jaro_distance(ingr_name[i].lower(), drug_info_drug_name[j].lower()) > 0.98:
                    icode_dcode_list_append([ingr_code[i], drug_info_drug_code[j]])
                    ingr_name_list_append([ingr_name[i], drug_info_drug_name[j]])

        main_ingred_code_drug_code_df = pd.DataFrame(icode_dcode_list,
                                                     columns=['ingr_code', 'drug_code'])
        main_ingred_name_df = pd.DataFrame(ingr_name_list,
                                           columns=['ingr_drug_name_master', 'ingr_drug_name_info'])

        result_main_ingred_code_drug_code_df = main_ingred_code_drug_code_df.drop_duplicates().reset_index(drop=True)
        result_main_ingred_name_df = main_ingred_name_df.drop_duplicates().reset_index(drop=True)

        return result_main_ingred_code_drug_code_df, result_main_ingred_name_df

    df_ingr_drug_code_01, df_ingred_name_01 = icode_dcode_01(ingred_master, info_df)
    df_ingr_drug_code_02, df_ingred_name_02 = icode_dcode_02(ingred_master, info_df)

    result_dataframe = pd.concat([df_ingr_drug_code_01, df_ingr_drug_code_02], axis=0)

    icode_dcode_clear = result_dataframe.drop_duplicates().reset_index(drop=True)

    return icode_dcode_clear


if __name__ == '__main__':
    x = icode_dcode()
    print(x)
