# -*- coding: utf-8 -*-
import pandas as pd


class Actions:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t')

            daily_extraction = daily_extraction.groupby(['id_unico', 'action_category_id', 'threshold'], sort=False, as_index=False).sum()
            daily_extraction.insert(5, "avg_score", (daily_extraction["score_sum"]/daily_extraction["count_sum"]), True)
            daily_extraction.insert(6, "avg_score_OK", (daily_extraction["sum_score_OK"]/daily_extraction["result_OK"]), True)
            daily_extraction.insert(11, "avg_score_KO", (daily_extraction["sum_score_KO"]/daily_extraction["result_KO"]), True)
            daily_extraction.insert(12, "percentage_OK_vs_KO",
                                    daily_extraction["result_OK"] / daily_extraction["count_sum"] * 100, True)
            daily_extraction.insert(15, "ratio_Success_Fail",
                                    daily_extraction["Successful"] / daily_extraction["count_sum"] * 100, True)

            daily_extraction = daily_extraction.round(decimals=2)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            filename = pd.read_csv(path_merged + "/" + filename, sep=';')
            filename = filename.drop(["avg_score", "avg_score_OK", "avg_score_KO", "percentage_OK_vs_KO", "ratio_Success_Fail"], axis=1)

            daily_extraction = pd.read_csv(daily_extraction, sep='\t')
            daily_extraction = daily_extraction.groupby(['id_unico', 'action_category_id', 'threshold'], sort=False, as_index=False).sum()

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(['id_unico', 'action_category_id', 'threshold'], sort=False, as_index=False).sum()
            combined_csv.insert(5, "avg_score", (combined_csv["score_sum"]/combined_csv["count_sum"]), True)
            combined_csv.insert(8, "avg_score_OK", (combined_csv["sum_score_OK"]/combined_csv["result_OK"]), True)
            combined_csv.insert(11, "avg_score_KO", (combined_csv["sum_score_KO"]/combined_csv["result_KO"]), True)
            combined_csv.insert(12, "percentage_OK_vs_KO",
                                    combined_csv["result_OK"] / combined_csv["count_sum"] * 100, True)
            combined_csv.insert(15, "ratio_Success_Fail",
                                    combined_csv["Successful"] / combined_csv["count_sum"] * 100, True)

            combined_csv = combined_csv.round(decimals=2)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")


class Mature:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            df = pd.read_csv(daily_extraction, sep='\t', index_col=False)
            df.insert(0, "Concatenate", df['template_key'] + ", " + df['channel'] + ", " + df['template_repository_version'].map(str))
            df = df.groupby(["Concatenate"], sort=True, as_index=False).sum()
            df = df.drop(["template_repository_version"], axis=1)
            df.insert(4, "avg_score", df['sum_score']/df['action_count'])

            df = df.round(decimals=2)
            df.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)
            filename = filename.drop(["avg_score"], axis=1)

            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)
            daily_extraction.insert(0, "Concatenate", daily_extraction['template_key'].map(str) + ", " + daily_extraction['channel'].map(str)
                                    + ", " + daily_extraction['template_repository_version'].map(str))

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(["Concatenate"], sort=True, as_index=False).sum()
            combined_csv = combined_csv.drop(["template_repository_version"], axis=1)
            combined_csv.insert(4, "avg_score", combined_csv['sum_score']/combined_csv['action_count'])

            combined_csv = combined_csv.round(decimals=2)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")


class SimpleConcatenate:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            daily_extraction.fillna(0, inplace=True)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv.fillna(0, inplace=True)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")