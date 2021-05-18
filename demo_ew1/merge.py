from datetime import date, timedelta, datetime
from scripts.QoS import *
from scripts.billing import *
from scripts.report import *
import pandas as pd
import os
import os.path

'''TODO: Automtizar lista tenants con un nohup a la BBDD para que se actualize la lista de tenants dinamicamente'''

tenants = ["test"]

path_merged = "path"
path_merged_output = path_merged + "/output"
path_merged_sql = path_merged + "/sql"

path_extractions = path_merged + "/input"
path_extractions_QoS = path_extractions + "/QoS"
path_extractions_billing = path_extractions + "/billing"
path_extractions_report = path_extractions + "/report"


def sql_queries(path_sql_queries):
    '''For loop para busque dentro de la carpeta de sql todas las queries existentes'''
    sqls = []
    for root, dirs, files in os.walk(path_sql_queries):
        for f in files:
            sqls.append(f)
    return sqls


def extraction_files():
    '''Crea una variable global que es una lista que contiene todos los archivos extraidos del report.sh'''
    global extraction_files
    extraction_files = []
    for root, dirs, files in os.walk(path_merged_output):
        for archivo in files:
            extraction_files.append(archivo)

    return extraction_files


def tenant_paths(directory, clients):

    paths = []
    for client in clients:
        paths.append(directory + "/" + client)

    return paths


yesterday = pd.to_datetime('today') - timedelta(days=1)
mes_extraccion = yesterday.strftime('%Y.%m.%d').rsplit(".", 1)[0]


def main():
    for tenant in tenants:
        for sql in sql_queries(path_merged_sql):
            extraction_file = sql + "." + tenant + "." + yesterday.strftime('%Y.%m.%d') + ".csv"

            for path in tenant_paths(path_merged_output, tenants):
                query = sql.split(".", 1)[0]
                if not os.path.isdir(path + "/" + query):
                    os.makedirs(path + "/" + query)

            mes = str(datetime.today().strftime('%Y.%m'))
            if mes_extraccion == mes:
                print("Mes actual: " + mes)
            else:
                mes = mes_extraccion
                print(mes)

            filename = "dv_saas_" + tenant + "." + sql + "." + mes + ".csv"

            merge_csv = {
                # "actions.sql": (lambda: Actions.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "actions_new.sql": (lambda: ActionsNew.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "mature.sql": (lambda: Mature.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "qa.sql": (lambda: SimpleConcatenate.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "metadata.sql": (lambda: SimpleConcatenate.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "img_count.sql": (lambda: ImgCount.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "dve_count.sql": (lambda: DveCount.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "fve_count.sql": (lambda: FveCount.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "lsv_count.sql": (lambda: LsvCount.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "verification_count.sql": (lambda: VeriReport.merge_csv(path_merged_output + "/" + tenant + "/" + query, filename, path_extractions_report + "/" + extraction_file, sql, tenant, mes))
            }

            create_csv = {
                # "actions.sql": (lambda: Actions.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "actions_new.sql": (lambda: ActionsNew.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_QoS + "/" + extraction_file, sql, tenant,mes)),
                "mature.sql": (lambda: Mature.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "qa.sql": (lambda: SimpleConcatenate.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "metadata.sql": (lambda: SimpleConcatenate.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_QoS + "/" + extraction_file, sql, tenant, mes)),
                "img_count.sql": (lambda: ImgCount.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "dve_count.sql": (lambda: DveCount.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "fve_count.sql": (lambda: FveCount.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "lsv_count.sql": (lambda: LsvCount.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_billing + "/" + extraction_file, sql, tenant, mes)),
                "verification_count.sql": (lambda: VeriReport.create_merge(path_merged_output + "/" + tenant + "/" + query, path_extractions_report + "/" + extraction_file, sql, tenant, mes))
            }

            if filename in extraction_files:
                for fila, func in merge_csv.items():
                    if fila == sql:
                        try:
                            func()
                            print(filename + " + " + extraction_file + ": Merged completed\n")
                        except Exception as exception:
                            print(exception)
                    else:
                        pass
            else:
                for fila, func in create_csv.items():
                    print(fila)
                    if fila == sql:
                        try:
                            func()
                            print(filename + ": Merged created\n")
                        except Exception as exception:
                            print(exception)


if __name__ == '__main__':
    if os.path.exists(path_merged_output) and os.path.isdir(path_merged_output):
        if not os.listdir(path_merged_output):
            print("Output directory is empty")
            os.system("aws s3 sync " + os.environ['s3_path'] + "/cron_task/monthly_extraction/ " + path_merged_output + " --exclude '*' --include '*.csv'")
        else:
            print("Output directory is not empty")
    else:
        print("Given Directory don't exists")
        os.mkdir(path_merged_output)
        os.system("aws s3 sync " + os.environ['s3_path'] + "/cron_task/monthly_extraction/ " + path_merged_output + " --exclude '*' --include '*.csv'")

    extraction_files()

    main()

    for client in tenant_paths(path_merged_output, tenants):
        cliente = client.rsplit("/", 1)[1]
        os.system("aws s3 sync " + client + " " + os.environ['s3_path'] + "/cron_task/monthly_extraction/" + cliente + " --exclude '*' --include '*.csv'")

    for root, dir, file in os.walk(path_extractions):
        for list in dir:
            os.system("rm -f " + path_extractions + "/" + list + "/*.csv")
