import pandas as pd
import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np


def dbms_results(dbms_row, dbms_column, print_caption):
    global file, lines, pattern, line, row_data, X
    # Read the CSV file into a DataFrame, reading each row as a string
    with open('rank_algorithms_' + dbms_row + '.txt', 'r') as file:
        lines = file.readlines()
    # Define the regular expression pattern for valid rows
    pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'
    # Filter out rows that do not match the pattern
    valid_lines = [line.strip().split(',') for line in lines if re.match(pattern, line.strip())]
    wrong_lines = [line for line in valid_lines if not len(line) == 11]
    # Create a DataFrame from the filtered lines
    row_data = pd.DataFrame(valid_lines, columns=['T1', 'T2', 'PB', 'RS', 'Storage', 'IDX', 'Padding', 'Par', 'Alg', 'Cmp'])
    # Convert numeric columns to the appropriate data types
    numeric_cols = ['T1', 'T2', 'PB', 'RS']
    row_data[numeric_cols] = row_data[numeric_cols].astype(int)
    row_data.loc[row_data['T2'] > 300000, 'T2'] = 300000

    with open('rank_algorithms_' + dbms_column + '.txt', 'r') as file:
        lines = file.readlines()
    # Define the regular expression pattern for valid rows
    pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'
    # Filter out rows that do not match the pattern
    valid_lines = [line.strip().split(',') for line in lines if re.match(pattern, line.strip())]
    wrong_lines = [line for line in valid_lines if not len(line) == 11]
    # Create a DataFrame from the filtered lines
    column_data = pd.DataFrame(valid_lines, columns=['T1', 'T2', 'PB', 'RS', 'Storage', 'IDX', 'Padding', 'Par', 'Alg', 'Cmp'])
    # Convert numeric columns to the appropriate data types
    numeric_cols = ['T1', 'T2', 'PB', 'RS']
    column_data[numeric_cols] = column_data[numeric_cols].astype(int)
    column_data.loc[column_data['T2'] > 300000, 'T2'] = 300000

    plt.rcParams['font.size'] = 14
    def all_parameters():
        boxplot_dict = plt.boxplot([np.log10(row_data['T1'] / row_data['T2']),
                                    np.log10(column_data['T1'] / column_data['T2'])
                                    ],
                                   showfliers=True,
                                   positions=[1, 2],
                                   labels=['Row', 'Column']
                                   )

        # Modify the fliers marker style
        flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
        for flier in boxplot_dict['fliers']:
            flier.set(**flier_marker_style)


        plt.xticks(rotation=80)
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$')
        plt.title(print_caption)

        # plt.yscale('log')  # show the y-axis in log scale
        # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        # if we use np.log10, we need to use the following lines
        plt.yticks(np.arange(-4, 4), 10.0 ** np.arange(-4, 4))
        plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.18, right=0.97, top=0.94, bottom=0.24)  # Adjust the values as per your requirements

        plt.savefig(print_caption + '_rank_algorithms.pdf', format='pdf')

        # Show the plot
        plt.show()


    ###################################################################
    # Create the box plot
    all_parameters()

dbms_results('MSSql', 'column_MSSql', 'DBMS1 Comparison With Column Store')
