## This script is used to analyze the results of the JoinNMin experiments on PostgreSQL

import pandas as pd
import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np
from result_analysis.manipulation import read_data, compute_means


titlesize = 25
xlabelsize = 17

def dbms_results(dbms, print_caption, has_column):
    global titlesize, xlabelsize, file, lines, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, equal1_data, equalN_data, lessN_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X
    # Read the CSV file into a DataFrame, reading each row as a string
    with open('rank_NMIN_' + dbms + '.txt', 'r') as file:
        lines = file.readlines()
    # Define the regular expression pattern for valid rows
    pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'
    # Filter out rows that do not match the pattern
    valid_lines = [line.strip().split(',') for line in lines if re.match(pattern, line.strip())]
    wrong_lines = [line for line in valid_lines if not len(line) == 11]
    # Create a DataFrame from the filtered lines
    data = pd.DataFrame(valid_lines, columns=['T1', 'T2', 'PB', 'RS', 'Storage', 'IDX', 'Padding', 'Par', 'Alg', 'Cmp'])
    # Convert numeric columns to the appropriate data types
    numeric_cols = ['T1', 'T2', 'PB', 'RS']
    data[numeric_cols] = data[numeric_cols].astype(int)

    # If column data['T2'] is larger than 300000, then set it to 300000
    data.loc[data['T2'] > 300000, 'T2'] = 300000

    # Filter out results of column storage
    if has_column:
        data = data[data['Storage'] == 'ROW']
        has_column = False

    ###########################################################################
    # Filter rows based on conditions and compute statistics
    column_data = data[data['Storage'] == 'COLUMN']
    row_data = data[data['Storage'] == 'ROW']
    parallel_on = data[data['Par'] == 'parallel_ON']
    parallel_off = data[data['Par'] == 'parallel_OFF']
    padding_on = data[data['Padding'] == 'padding_ON']
    padding_off = data[data['Padding'] == 'padding_OFF']
    lateraldistinctlimit_data = data[data['Alg'] == 'LateralDistinctLimit']
    joinmin_data = data[data['Alg'] == 'JoinNMin']
    NoIndex_data = data[data['IDX'].str.startswith(' ')]
    SomeIndex_data = data[~data['IDX'].str.startswith(' ')]

    # IB_data = data[data['IDX'].str.contains('I\(B')]
    # Not_IB_data = data[~data['IDX'].str.contains('I\(B')]
    # IA_data = data[data['IDX'].str.contains('I\(A')]
    # Not_IA_data = data[~data['IDX'].str.contains('I\(A')]
    # IBA_data = data[data['IDX'].str.contains('I\(BA\)')]
    # IAB_data = data[data['IDX'].str.contains('I\(AB\)')]



    # compute_means(joinmin_data, print_caption, has_cost)
    compute_means(data, dbms, False, "Greatest Per Group Microbenchmark, all data")
    compute_means(data[data['T2'] < 300000], dbms, False, "Greatest Per Group Microbenchmark, less than 5 minutes data")

    print("-------------------------------------")
    print("Algorithms over 3000s for each algorithm:")
    print("LateralDistinctLimitTies: ", len(lateraldistinctlimit_data[lateraldistinctlimit_data['T2'] >= 300000])/len(lateraldistinctlimit_data)*100 , "%", "(", len(lateraldistinctlimit_data[lateraldistinctlimit_data['T2'] >= 300000]), "/", len(lateraldistinctlimit_data), ")" )
    print("JoinMin: ", len(joinmin_data[joinmin_data['T2'] >= 300000])/len(joinmin_data)*100 , "%", "(", len(joinmin_data[joinmin_data['T2'] >= 300000]), "/", len(joinmin_data), ")" )

    def all_parameters():
        attrcount = 9

        plt.figure(figsize=(8, 7.5))
        boxplot_dict = plt.boxplot([np.log10(data['T1'] / data['T2']),
                                    np.log10(lateraldistinctlimit_data['T1'] / lateraldistinctlimit_data['T2']),
                                    np.log10(joinmin_data['T1'] / joinmin_data['T2']),
                                    np.log10(parallel_on['T1'] / parallel_on['T2']),
                                    np.log10(parallel_off['T1'] / parallel_off['T2']),
                                    np.log10(padding_on['T1'] / padding_on['T2']),
                                    np.log10(padding_off['T1'] / padding_off['T2']),
                                    np.log10(NoIndex_data['T1'] / NoIndex_data['T2']),
                                    np.log10(SomeIndex_data['T1'] / SomeIndex_data['T2'])
                                    ],

                                   showfliers=True,
                                   positions=[i for i in range(attrcount)],
                                   labels=['ALL', 'LateralDistinctLimitTies', 'JoinMin',
                                           'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING',
                                           'NO INDEX', 'SOME INDEX']
                                   )

        # Modify the fliers marker style
        flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
        for flier in boxplot_dict['fliers']:
            flier.set(**flier_marker_style)

        # Define the colors for the desired boxes
        colors = ['orange', 'orange', 'blue', 'blue', 'green', 'green', 'brown', 'brown', 'magenta', 'magenta']

        # Loop through the desired boxes and modify their color
        for i in range(1, attrcount):
            box = boxplot_dict['boxes'][i]
            box.set(color=colors[i - 1])

        plt.xticks(rotation=80, fontsize=xlabelsize)
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$', fontsize=20)
        plt.title(print_caption, fontsize=titlesize)

        # plt.yscale('log')  # show the y-axis in log scale
        # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        # if we use np.log10, we need to use the following lines
        plt.yticks(np.arange(-3, 3), 10.0 ** np.arange(-3, 3), fontsize=15)
        plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1

        # plt.subplots_adjust(left=0.13, right=0.97, top=0.94, bottom=0.34)  # Adjust the values as per your requirements
        plt.tight_layout()
        plt.savefig(print_caption + '_rank_NMIN.pdf', format='pdf')

        # Show the plot
        plt.show()

    # Create the box plot
    all_parameters()


dbms_results('Postgres', 'PostgreSql', False)