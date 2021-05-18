import pandas as pd


class DveCount:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)
            daily_extraction = daily_extraction.groupby(["auth_user", "customer", "response_key"], sort=True, as_index=False).sum()

            daily_extraction.fillna(0, inplace=True)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(["auth_user", "customer", "response_key"], sort=True, as_index=False).sum()

            combined_csv.fillna(0, inplace=True)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")


class FveCount:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)
            daily_extraction = daily_extraction.groupby(["customer", "subkey"], sort=True, as_index=False).sum()

            daily_extraction.fillna(0, inplace=True)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(["customer", "subkey"], sort=True, as_index=False).sum()

            combined_csv.fillna(0, inplace=True)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")


class ImgCount:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)
            daily_extraction = daily_extraction.groupby(['auth_user', 'customer', 'response_key'], sort=False, as_index=False).sum()

            daily_extraction.fillna(0, inplace=True)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(['auth_user', 'customer', 'response_key'], sort=False, as_index=False).sum()

            combined_csv.fillna(0, inplace=True)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")


class LsvCount:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)
            daily_extraction = daily_extraction.groupby(['auth_user', 'channel', 'result'], sort=False, as_index=False).sum()

            daily_extraction.fillna(0, inplace=True)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(['auth_user',  'channel', 'result'], sort=False, as_index=False).sum()

            combined_csv.fillna(0, inplace=True)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';', index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")
