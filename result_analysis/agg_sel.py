## This script is used to analyze the results of the greatest per group microbenchmark.
## Script is used just for ROW storage format.
## It reads the results from the file rank_algorithms_DBMS.txt and creates a boxplots for each DBMS.
## The boxplots show the runtime ratio Tsj/Tln for each DBMS.


import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np
from result_analysis.manipulation import read_data, compute_means

counter = 0
display_dbms_text = {
    'Hyper': 'Hyper',
    'Postgres': 'PostgreSQL',
    'MSSql': 'DBMS1',
    'Oracle': 'DBMS2'
}
display_bdistinct = {
    10: '0.001',
    30000: '3'
}
display_point = {
    10: 'o',
    30000: 'x'
}

def agg_analysis(dbms_list, index, print_legend):
    global counter, file, lines, columns, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, equal1_data, equalN_data, lessN_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X

    data = {}

    for (dbms, alg, color) in dbms_list:
        # Read the CSV file into a DataFrame, reading each row as a string
        with open('agg_' + dbms + '_largesel.txt', 'r') as file:
            lines = file.readlines()

        has_cost = ('query_cost' in lines[0]) # check if the first line contains 'query_cost' substring
        columns = ['Fun', 'Constructs', 'Sel']
        data[dbms] = read_data(has_cost, lines, columns)
        if dbms != 'Hyper':
            data[dbms] = data[dbms][data[dbms]['Storage'] == "ROW"]


    plt.rcParams['font.size'] = 14


    for (dbms,bd, color) in dbms_list:
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
        sel_values = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
        division_result_list = (filtered_selected_data['T1'] / filtered_selected_data['T2']).tolist()
        plt.plot(sel_values, division_result_list, marker=display_point[bd], label=display_dbms_text[dbms] + ', bdistinct: ' + display_bdistinct[bd], color=color)

    plt.axhline(y=1, color='red', linestyle='--')
    plt.ylim(0.001, 1000)
    plt.xscale('log')
    plt.yscale('log')
    if print_legend:
        plt.legend(handlelength=0, fontsize='x-small')
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$', fontsize=20)

    # Label axes and add title
    plt.xlabel('Selectivity', fontsize=18)
    if index == " ":
        plt.title('No index', fontsize=24)
    else:
        plt.title(index + ' index', fontsize=24)

    # plt.subplots_adjust(left=0.15, right=0.97, top=0.93, bottom=0.12)
    plt.tight_layout()
    plt.savefig('agg_sel_' + str(counter) + '.pdf', format='pdf')
    counter += 1
    plt.show()

def without_index():
    global dbms_list, alg, result
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    dbms_list = ['MSSql', 'Postgres', 'Oracle', 'Hyper']
    bdistinct = [10, 30000]
    result = [(d, bd) for bd in bdistinct for d in dbms_list]
    result = [(val1, val2, color) for color, (val1, val2) in zip(colors, result)]
    agg_analysis(result, " ", True)

def with_index():
    global dbms_list, alg, result
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#1f77b4', '#ff7f0e', '#2ca02c',]
    dbms_list = ['MSSql', 'Postgres', 'Oracle']
    bdistinct = [10, 30000]
    result = [(d, bd) for bd in bdistinct for d in dbms_list]
    result = [(val1, val2, color) for color, (val1, val2) in zip(colors, result)]
    agg_analysis(result, "I(A);I(B)", False)
    agg_analysis(result, "I(BA)", False)

# without_index()
with_index()

