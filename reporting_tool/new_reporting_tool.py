import os
import win32com.client as win32
import time
import pandas as pd
import shutil


def copy_persona_xlsb():
    personal = "PERSONAL.XLSB"
    cwd = os.getcwd()
    cwd_short = cwd.split("\\", 4)[2]
    path = "C:/Users/" + cwd_short + "/AppData/Roaming/Microsoft/Excel/XLSTART/"

    for root, dirs, files in os.walk(cwd):
        if personal in files:
            try:
                shutil.copy(os.path.join(root, personal), path)
            except shutil.SameFileError:
                print("Error")
                break

        else:
            break


def vlookup(csv_infra):
    df = pd.read_csv(csv_infra, sep=',').replace(";",'').replace("\\N", '0')
    df.loc[:, "duration_prod":] = df.loc[:, "duration_prod":].apply(pd.to_numeric, downcast='integer', errors='coerce')
    df.to_excel(csv_infra.replace(".csv", '') + ".xlsx", index=False, engine='xlsxwriter')


def pivot_tables_macro(file_path):
    """
    Execute an Excel macro
    :param file_path: path to the Excel file holding the macro
    :param separator_char: the character used by the operating system to separate pathname components
    :return: None
    """
    xl = win32.Dispatch('Excel.Application')
    xl.Application.visible = True  # change to True if you are desired to make Excel visible

    try:
        user = os.getcwd().split("\\", 4)[2]
        path = "C:/Users/" + user + "/AppData/Roaming/Microsoft/Excel/XLSTART/PERSONAL.XLSB"
        path = path.replace("/", "\\")
        wo = xl.Workbooks.Open(path)
        wb = xl.Workbooks.Open(os.path.abspath(file_path))
        # print(path.split(sep=separator_char)[-1])
        xl.Application.run("PERSONAL.XLSB!Reporting_tool.Performance_Pivot")
        xl.Application.run("PERSONAL.XLSB!Reporting_tool.Top_Templates")
        xl.Application.run("PERSONAL.XLSB!Reporting_tool.SubKey_Templates_Pivot")
        wb.Save()
        # wb.Close()
        wo.Close()

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

    # xl.Application.Quit()
    # del xl


def main():
    copy_persona_xlsb()

    t0 = time.time()
    csv_in = input("Introduce .csv: ")
    csv_in = csv_in.replace('"', '')
    vlookup(csv_in)

    pivot_tables_macro(csv_in.replace(".csv", '') + ".xlsx")

    t1 = time.time()
    total_time = t1 - t0
    print(total_time)


if __name__ == '__main__':
    main()
