## This script is used to analyze the results of the greatest per group microbenchmark.
## It reads the results from the file rank_algorithms_DBMS.txt and creates a boxplots for each DBMS.
## The boxplots show the runtime ratio Tsj/Tln for each DBMS.


import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np
from result_analysis.manipulation import read_data, compute_means


def dbms_results(dbms, print_caption, has_cost, storage, plt):
    global file, lines, columns, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, equal1_data, equalN_data, lessN_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X
    # Read the CSV file into a DataFrame, reading each row as a string
    with open('rank_algorithms_' + dbms + '.txt', 'r') as file:
        lines = file.readlines()
    columns = ['Cmp']
    data = read_data(has_cost, lines, columns)

    # Select results of specified storage
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
    NoIndex_data = data[data['IDX'].str.startswith(' ')]
    SomeIndex_data = data[~data['IDX'].str.startswith(' ')]

    # IB_data = data[data['IDX'].str.contains('I\(B')]
    # Not_IB_data = data[~data['IDX'].str.contains('I\(B')]
    # IA_data = data[data['IDX'].str.contains('I\(A')]
    # Not_IA_data = data[~data['IDX'].str.contains('I\(A')]
    # IBA_data = data[data['IDX'].str.contains('I\(BA\)')]
    # IAB_data = data[data['IDX'].str.contains('I\(AB\)')]


    # compute_means(joinmin_data, print_caption, has_cost)
    compute_means(data, dbms, has_cost, "Greatest Per Group Microbenchmark, all data")
    compute_means(data[data['T2'] < 300000], dbms, has_cost, "Greatest Per Group Microbenchmark, less than 5 minutes data")

    print("-------------------------------------")
    print("Algorithms over 3000s for each algorithm:")
    print("LateralAgg: ", len(lateralagg_data[lateralagg_data['T2'] >= 300000])/len(lateralagg_data)*100 , "% (", len(lateralagg_data[lateralagg_data['T2'] >= 300000]), "/", len(lateralagg_data), ")")
    print("LateralLimitTies: ", len(laterallimit_data[laterallimit_data['T2'] >= 300000])/len(laterallimit_data)*100 , "%", "(", len(laterallimit_data[laterallimit_data['T2'] >= 300000]), "/", len(laterallimit_data), ")")
    print("LateralDistinctLimitTies: ", len(lateraldistinctlimit_data[lateraldistinctlimit_data['T2'] >= 300000])/len(lateraldistinctlimit_data)*100 , "%", "(", len(lateraldistinctlimit_data[lateraldistinctlimit_data['T2'] >= 300000]), "/", len(lateraldistinctlimit_data), ")" )
    print("JoinMin: ", len(joinmin_data[joinmin_data['T2'] >= 300000])/len(joinmin_data)*100 , "%", "(", len(joinmin_data[joinmin_data['T2'] >= 300000]), "/", len(joinmin_data), ")" )

    # plt.rcParams['font.size'] = 14
    def all_parameters():
        attrcount = 11
        boxplot_dict = plt.boxplot([np.log10(data['T1'] / data['T2']),
                                    np.log10(lateralagg_data['T1'] / lateralagg_data['T2']),
                                    np.log10(laterallimit_data['T1'] / laterallimit_data['T2']),
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
                                   labels=['ALL', 'LateralAgg', 'LateralLimitTies', 'LateralDistinctLimitTies', 'JoinMin',
                                           'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING',
                                           'NO INDEX', 'SOME INDEX']
                                   )

        # Modify the fliers marker style
        flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
        for flier in boxplot_dict['fliers']:
            flier.set(**flier_marker_style)

        # Define the colors for the desired boxes
        colors = ['orange', 'orange', 'orange', 'orange', 'blue', 'blue', 'green', 'green', 'brown', 'brown', 'magenta', 'magenta']

        # Loop through the desired boxes and modify their color
        for i in range(1, attrcount):
            box = boxplot_dict['boxes'][i]
            box.set(color=colors[i - 1])

        plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1

        return boxplot_dict

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


    def times(input_data, local_print_caption):
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
        plt.title(local_print_caption)

        # plt.yscale('log')  # show the y-axis in log scale
        # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        # if we use np.log10, we need to use the following lines
        plt.yticks(np.arange(0, 6), 10.0 ** (np.arange(0, 6)))
        # plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1


        plt.subplots_adjust(left=0.2, right=0.97, top=0.94, bottom=0.1)  # Adjust the values as per your requirements
        plt.savefig(print_caption + '_times.pdf', format='pdf')

        # Show the plot
        plt.show()


    ###################################################################
    # Create the box plot
    return all_parameters()
    ###################################################################
    # Create the next box plot for T1/T2 based on PB value
    # bvalues(joinmin_data, print_caption + ' JoinMin')
    # bvalues(lateraldistinctlimit_data, print_caption + ' LateralDistinctLimit')

    ###################################################################
    # times(data, print_caption + ' Query Processing Times')


    fig, axs = plt.subplots(1, 4, figsize=(22, 7), sharex=True)

titlesize = 25
xlabelsize = 17

# First subplot
boxplot_dict1 = dbms_results('MSSql2', 'DBMS1', True, "ROW", axs[0])
axs[0].set_title('DBMS1', fontsize=titlesize)
axs[0].tick_params(axis='x', rotation=80)
axs[0].tick_params(axis='x', labelsize=xlabelsize)
axs[0].set_ylabel(r'$T_{lin}\,/\,T_{sj}$', fontsize=20)
axs[0].set_yticks(np.arange(-4, 4), [r'$10^{' + str(i) +  r'}$' for i in range(-4, 4)], fontsize=15)


# Second subplot
boxplot_dict2 = dbms_results('Postgres2', 'PostgreSql', True, "ROW", axs[1])
axs[1].set_title('PostgreSQL', fontsize=titlesize)
axs[1].tick_params(axis='x', rotation=80)
axs[1].tick_params(axis='x', labelsize=xlabelsize)
axs[1].set_yticks(np.arange(-4, 4) , [])

# Third subplot
boxplot_dict3 = dbms_results('Oracle', 'DBMS2', False, "ROW", axs[2])
axs[2].set_title('DBMS2', fontsize=titlesize)
axs[2].tick_params(axis='x', rotation=80)
axs[2].tick_params(axis='x', labelsize=xlabelsize)
axs[2].set_yticks(np.arange(-4, 4), [])

# Fourth subplot
boxplot_dict4 = dbms_results('Hyper', 'Hyper', False, "COLUMN", axs[3])
axs[3].set_title('Hyper', fontsize=titlesize)
axs[3].tick_params(axis='x', rotation=80)
axs[3].tick_params(axis='x', labelsize=xlabelsize)
axs[3].set_yticks(np.arange(-4, 4), [])

plt.tight_layout()
plt.savefig('rank_algorithms.pdf', format='pdf')
plt.show()

# dbms_results('MSSql2', 'DBMS1', True, "ROW")
# dbms_results('Postgres2', 'PostgreSql', True, "ROW")
#
# # dbms_results('MSSql', 'DBMS1', False, "ROW")
# # dbms_results('Postgres', 'PostgreSql', False, "ROW")
# dbms_results('Oracle', 'DBMS2', False, "ROW")
# dbms_results('Hyper', 'Hyper', False, "COLUMN")
