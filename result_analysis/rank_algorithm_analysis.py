import pandas as pd
import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np


def dbms_results(dbms, print_caption, storage):
    global file, lines, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, equal1_data, equalN_data, lessN_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X
    # Read the CSV file into a DataFrame, reading each row as a string
    with open('rank_algorithms_' + dbms + '.txt', 'r') as file:
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
    data = data[data['Storage'] == storage]

    ###########################################################################
    # Filter rows based on conditions and compute statistics
    column_data = data[data['Storage'] == 'COLUMN']
    row_data = data[data['Storage'] == 'ROW']
    parallel_on = data[data['Par'] == 'parallel_ON']
    parallel_off = data[data['Par'] == 'parallel_OFF']
    padding_on = data[data['Padding'] == 'padding_ON']
    padding_off = data[data['Padding'] == 'padding_OFF']
    lateralagg_data = data[data['Alg'] == 'LateralAgg']
    laterallimit_data = data[data['Alg'] == 'LateralLimit']
    lateraldistinctlimit_data = data[data['Alg'] == 'LateralDistinctLimit']
    joinmin_data = data[data['Alg'] == 'JoinMin']
    IB_data = data[data['IDX'].str.contains('I\(B')]
    Not_IB_data = data[~data['IDX'].str.contains('I\(B')]
    IA_data = data[data['IDX'].str.contains('I\(A')]
    Not_IA_data = data[~data['IDX'].str.contains('I\(A')]
    IBA_data = data[data['IDX'].str.contains('I\(BA\)')]
    IAB_data = data[data['IDX'].str.contains('I\(AB\)')]



    # compute average value of T1
    print("----------------------------")
    print("DBMS: " + dbms)
    print("Average WF strategy time: ", data['T1'].mean() / 1000.0)
    print("Average Self-join strategy time: ", data['T2'].mean() / 1000.0)

    # Compute the geometric mean of T1/T2
    geometric_mean = stats.gmean(data['T1'] / data['T2'].replace(0, 1))
    print("Geometric Mean of T1/T2:", geometric_mean)

    # find number of rows where T2 >= 300000 for LateralAgg
    print("Number of rows reaching the 300s limit")
    print("LateralAgg: ", len(lateralagg_data[lateralagg_data['T2'] >= 300000])/len(lateralagg_data)*100 , "% (", len(lateralagg_data[lateralagg_data['T2'] >= 300000]), "/", len(lateralagg_data), ")")
    print("LateralLimit: ", len(laterallimit_data[laterallimit_data['T2'] >= 300000])/len(laterallimit_data)*100 , "%", "(", len(laterallimit_data[laterallimit_data['T2'] >= 300000]), "/", len(laterallimit_data), ")")
    print("LateralDistinctLimit: ", len(lateraldistinctlimit_data[lateraldistinctlimit_data['T2'] >= 300000])/len(lateraldistinctlimit_data)*100 , "%", "(", len(lateraldistinctlimit_data[lateraldistinctlimit_data['T2'] >= 300000]), "/", len(lateraldistinctlimit_data), ")" )
    print("JoinMin: ", len(joinmin_data[joinmin_data['T2'] >= 300000])/len(joinmin_data)*100 , "%", "(", len(joinmin_data[joinmin_data['T2'] >= 300000]), "/", len(joinmin_data), ")" )

    plt.rcParams['font.size'] = 14
    def all_parameters():
        boxplot_dict = plt.boxplot([np.log10(data['T1'] / data['T2']),
                                    np.log10(lateralagg_data['T1'] / lateralagg_data['T2']),
                                    np.log10(laterallimit_data['T1'] / laterallimit_data['T2']),
                                    np.log10(lateraldistinctlimit_data['T1'] / lateraldistinctlimit_data['T2']),
                                    np.log10(joinmin_data['T1'] / joinmin_data['T2']),
                                    np.log10(parallel_on['T1'] / parallel_on['T2']),
                                    np.log10(parallel_off['T1'] / parallel_off['T2']),
                                    np.log10(padding_on['T1'] / padding_on['T2']),
                                    np.log10(padding_off['T1'] / padding_off['T2']),
                                    np.log10(IBA_data['T1'] / IBA_data['T2']),
                                    np.log10(IAB_data['T1'] / IAB_data['T2']),
                                    np.log10(IB_data['T1'] / IB_data['T2']),
                                    np.log10(Not_IB_data['T1'] / Not_IB_data['T2']),
                                    np.log10(IA_data['T1'] / IA_data['T2']),
                                    np.log10(Not_IA_data['T1'] / Not_IA_data['T2'])
                                    ],

                                   showfliers=True,
                                   positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                                   labels=['ALL', 'LateralAgg', 'LateralLimit', 'LateralDistinctLimit', 'JoinMin',
                                           'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING',
                                           'I(BA)', 'I(AB)', 'I(B)', '~I(B)', 'I(A)', '~I(A)']
                                   )

        # Modify the fliers marker style
        flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
        for flier in boxplot_dict['fliers']:
            flier.set(**flier_marker_style)

        # Define the colors for the desired boxes
        colors = ['orange', 'orange', 'orange', 'orange', 'blue', 'blue', 'green', 'green', 'brown', 'brown', 'cyan', 'cyan',
                  'magenta', 'magenta']

        # Loop through the desired boxes and modify their color
        for i in range(1, 15):
            box = boxplot_dict['boxes'][i]
            box.set(color=colors[i - 1])

        plt.xticks(rotation=80)
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$')
        plt.title(print_caption)

        # plt.yscale('log')  # show the y-axis in log scale
        # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        # if we use np.log10, we need to use the following lines
        plt.yticks(np.arange(-4, 3), 10.0 ** np.arange(-4, 3))
        plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.18, right=0.97, top=0.94, bottom=0.43)  # Adjust the values as per your requirements

        plt.savefig(print_caption + '_rank_algorithms.pdf', format='pdf')

        # Show the plot
        plt.show()

    def bvalues(input_data, print_caption):
        global i
        pb_data = []
        pb_values = [10, 30, 100, 300, 1000, 3000, 10000, 30000]
        for i in range(len(pb_values)):
            pb_data.append(input_data[input_data['PB'] == pb_values[i]])
        # Create the box plots for bp_data
        boxplot_dict = plt.boxplot([np.log10(pb_data[0]['T1'] / pb_data[0]['T2']),
                     np.log10(pb_data[1]['T1'] / pb_data[1]['T2']),
                     np.log10(pb_data[2]['T1'] / pb_data[2]['T2']),
                     np.log10(pb_data[3]['T1'] / pb_data[3]['T2']),
                     np.log10(pb_data[4]['T1'] / pb_data[4]['T2']),
                     np.log10(pb_data[5]['T1'] / pb_data[5]['T2']),
                     np.log10(pb_data[6]['T1'] / pb_data[6]['T2']),
                     np.log10(pb_data[7]['T1'] / pb_data[7]['T2'])
                     ],
                    showfliers=True,
                    positions=[1, 2, 3, 4, 5, 6, 7, 8],
                    labels=pb_values
                    )

        # Modify the fliers marker style
        flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
        for flier in boxplot_dict['fliers']:
            flier.set(**flier_marker_style)

        plt.xticks(rotation=45)
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$')
        plt.title(print_caption)
        plt.xlabel('BDistinct')

        # plt.yscale('log')  # show the y-axis in log scale
        # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        # if we use np.log10, we need to use the following lines
        plt.yticks(np.arange(-2, 3), 10.0 ** np.arange(-2, 3))
        plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1


        plt.subplots_adjust(left=0.15, right=0.97, top=0.94, bottom=0.2)  # Adjust the values as per your requirements
        plt.savefig(print_caption + '_bdistinct.pdf', format='pdf')

        # Show the plot
        plt.show()


    def times(input_data, print_caption):
        # Create the box plots for bp_data
        boxplot_dict = plt.boxplot([np.log10(input_data['T1']),
                     np.log10(input_data['T2'])
                     ],
                    showfliers=True,
                    positions=[1, 2],
                    labels=[r'$T_{lin}$', r'$T_{sj}$']
                    )

        # Modify the fliers marker style
        flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
        for flier in boxplot_dict['fliers']:
            flier.set(**flier_marker_style)

        # plt.xticks(rotation=45)
        plt.ylabel(r'$T$ [ms]')
        plt.title(print_caption)

        # plt.yscale('log')  # show the y-axis in log scale
        # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        # if we use np.log10, we need to use the following lines
        plt.yticks(np.arange(0, 6), 10.0 ** (np.arange(0, 6) - 1))
        # plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1


        plt.subplots_adjust(left=0.2, right=0.97, top=0.94, bottom=0.1)  # Adjust the values as per your requirements
        plt.savefig(print_caption + '_times.pdf', format='pdf')

        # Show the plot
        plt.show()


    ###################################################################
    # Create the box plot
    all_parameters()
    ###################################################################
    # Create the next box plot for T1/T2 based on PB value
    # bvalues(joinmin_data, print_caption + ' JoinMin')
    # bvalues(lateraldistinctlimit_data, print_caption + ' LateralDistinctLimit')

    ###################################################################
    times(data, print_caption + ' Query Processing Times')

dbms_results('MSSql', 'DBMS1', "ROW")
# dbms_results('Postgres', 'PostgreSql', "ROW")
# dbms_results('Oracle', 'DBMS2', "ROW")
dbms_results('column_MSSql', 'DBMS1 Column Store', "COLUMN")
