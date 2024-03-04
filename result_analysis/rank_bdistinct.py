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

display_algorithm_name = {
    'JoinMin' : 'JoinMin',
    'LateralAgg' : 'LateralAgg',
    'LateralLimit' : 'LateralLimitTies',
    'LateralDistinctLimit' : 'LateralDistinctLimitTies',
}
display_point = {
    'JoinMin' : 'o',
    'LateralDistinctLimit' : 'x'
}

def rank_bdistinct_analysis(dbms_list, has_cost, storage, index, print_legend):
    global counter, file, lines, columns, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, equal1_data, equalN_data, lessN_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X

    data = {}

    for (dbms, alg, color) in dbms_list:
        # Read the CSV file into a DataFrame, reading each row as a string
        with open('rank_algorithms_' + dbms + '.txt', 'r') as file:
            lines = file.readlines()
        columns = ['Cmp']
        data[dbms] = read_data(has_cost, lines, columns)
        # data[dbms] = data[dbms][data[dbms]['Storage'] == storage]


    plt.rcParams['font.size'] = 14

    for (dbms,alg, color) in dbms_list:
        selected_data = data[dbms]
        filtered_selected_data = selected_data[
            (selected_data['Par'] == 'parallel_ON') &
            (selected_data['Padding'] == 'padding_OFF') &
            (selected_data['IDX'] == index) &
            (selected_data['Alg'] == alg)
            ]

        # pb_values = [10, 30, 100, 300, 1000, 3000, 10000, 30000]
        pb_values = [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1, 3]
        division_result_list = (filtered_selected_data['T1'] / filtered_selected_data['T2']).tolist()
        plt.plot(pb_values, division_result_list, marker=display_point[alg], label=display_dbms_text[dbms] + ' ' + display_algorithm_name[alg], color=color)

    plt.axhline(y=1, color='red', linestyle='--')
    plt.ylim(0.0001, 20)
    plt.xscale('log')
    plt.yscale('log')
    if print_legend:
        plt.legend(handlelength=0, fontsize='x-small')
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$', fontsize=20)

    # Label axes and add title
    plt.xlabel('BDistinct [%]', fontsize=18)
    if index == " ":
        plt.title('No index', fontsize=24)
    else:
        plt.title(index + ' index', fontsize=24)


    # plt.subplots_adjust(left=0.15, right=0.97, top=0.93, bottom=0.12)
    counter += 1
    # Show the plot
    plt.tight_layout()
    plt.savefig('rank_bdistinct_' + str(counter) + '.pdf', format='pdf')
    plt.show()

def without_index():
    global dbms_list, alg, result
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    # colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    dbms_list = ['MSSql', 'Postgres', 'Oracle', "Hyper"]
    # alg = ['LateralAgg', 'LateralLimit']
    alg = ['JoinMin', 'LateralDistinctLimit']
    result = [(d, a) for a in alg for d in dbms_list]
    result = [(val1, val2, color) for color, (val1, val2) in zip(colors, result)]

    rank_bdistinct_analysis(result, False, "ROW", " ", True)

def with_index():
    global dbms_list, alg, result
    # colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#9467bd', '#8c564b', '#e377c2']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#1f77b4', '#ff7f0e', '#2ca02c',]
    dbms_list = ['MSSql', 'Postgres', 'Oracle']
    # alg = ['LateralAgg', 'LateralLimit']
    alg = ['JoinMin', 'LateralDistinctLimit']
    result = [(d, a) for a in alg for d in dbms_list]
    result = [(val1, val2, color) for color, (val1, val2) in zip(colors, result)]
    # rank_bdistinct_analysis(result, False, "ROW", "I(B)", False)
    # rank_bdistinct_analysis(result, False, "ROW", "I(A)", False)
    rank_bdistinct_analysis(result, False, "ROW", "I(A);I(B)", False)
    # rank_bdistinct_analysis(result, False, "ROW", "I(AB)", False)
    rank_bdistinct_analysis(result, False, "ROW", "I(BA)", False)

without_index()
with_index()

