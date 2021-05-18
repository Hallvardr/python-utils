# -*- coding: utf-8 -*-
import pandas as pd
#set working directory


class VeriReport:
    def __init__(self):
        pass

    def create_merge(path_merged, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            daily_extraction.fillna(0, inplace=True)

            daily_extraction.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';',
                                    index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")

    def merge_csv(path_merged, filename, daily_extraction, sql, tenant, mes):

        try:
            daily_extraction = pd.read_csv(daily_extraction, sep='\t', index_col=False)

            filename = pd.read_csv(path_merged + "/" + filename, sep=';', index_col=False)

            combined_csv = pd.concat([filename, daily_extraction])

            combined_csv = combined_csv.groupby(["time", "period_from", "period_to", "template_name", "document_type",
                                                 "country"], sort=True, as_index=False).sum()

            combined_csv.fillna(0, inplace=True)

            combined_csv.to_csv(path_merged + "/" + "dv_saas_" + tenant + "." + sql + "." + mes + ".csv", sep=';',
                                index=False)

        except pd.errors.EmptyDataError:
            print("File is empty and has been skipped.")
