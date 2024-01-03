## This script is used to analyze the results of the greatest per group microbenchmark.
## Script is used just for ROW storage format.
## It reads the results from the file rank_algorithms_DBMS.txt and creates a boxplots for each DBMS.
## The boxplots show the runtime ratio Tsj/Tln for each DBMS.


import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np
from result_analysis.manipulation import read_data, compute_means


def agg_analysis(dbms_list, has_cost, index):
    global file, lines, columns, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, equal1_data, equalN_data, lessN_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X

    data = {}

    for (dbms, alg) in dbms_list:
        # Read the CSV file into a DataFrame, reading each row as a string
        with open('agg_' + dbms + '.txt', 'r') as file:
            lines = file.readlines()
        columns = ['Fun', 'Constructs', 'Sel']
        data[dbms] = read_data(has_cost, lines, columns)
        if dbms != 'Hyper':
            data[dbms] = data[dbms][data[dbms]['Storage'] == "ROW"]


    plt.rcParams['font.size'] = 14


    for (dbms,bd) in dbms_list:
        selected_data = data[dbms]
        filtered_selected_data = selected_data[
            (selected_data['Par'] == 'parallel_ON') &
            (selected_data['Padding'] == 'padding_OFF') &
            (selected_data['IDX'] == index) &
            (selected_data['Fun'] == 'min') &
            (selected_data['Constructs'] == 'PB_OB') &
            (selected_data['PB'] == bd)
            ]

        # pb_values = [10, 30, 100, 300, 1000, 3000, 10000, 30000]
        sel_values = [1, 2, 4, 8, 16, 32]
        division_result_list = (filtered_selected_data['T1'] / filtered_selected_data['T2']).tolist()
        plt.plot(sel_values, division_result_list, marker='o', label=dbms + ', bd: ' + str(bd))

    plt.axhline(y=1, color='red', linestyle='--')
    plt.ylim(0.001, 1000)
    plt.xscale('log')
    plt.yscale('log')
    plt.legend(handlelength=0, fontsize='small')

    # Label axes and add title
    plt.xlabel('PB Values')
    plt.ylabel('T1/T2')
    plt.title('Line Plot for T1/T2 with Logarithmic Axes')

    # Show the plot
    plt.show()

def without_index():
    global dbms_list, alg, result
    dbms_list = ['MSSql', 'Postgres', 'Oracle', 'Hyper']
    bdistinct = [10, 30000]
    result = [(d, bd) for bd in bdistinct for d in dbms_list]
    agg_analysis(result, False, " ")

def with_index():
    global dbms_list, alg, result
    dbms_list = ['MSSql', 'Postgres', 'Oracle']
    bdistinct = [10, 30000]
    result = [(d, bd) for bd in bdistinct for d in dbms_list]
    agg_analysis(result, False, "I(B)")
    agg_analysis(result, False, "I(A)")

without_index()
with_index()

