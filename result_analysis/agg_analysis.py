## This script is used to visualize the results of the aggregate microbenchmark
## Only row storage is considered
## The results are visualized in a boxplot for each DBMS.
## The boxplots show the runtime ratio Tsj/Tln for each DBMS.


import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np

from result_analysis.manipulation import read_data, compute_means


def dbms_results(dbms, has_cost, storage, plt):
    global data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, count_data, min_data, PB_data, PB_OB_data, IC_data, Not_IC_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X
    # Read the CSV file into a DataFrame, reading each row as a string
    with open('agg_' + dbms + '.txt', 'r') as file:
        lines = file.readlines()
    columns = ['Fun', 'Constructs', 'Sel']
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
    count_data = data[data['Fun'] == 'count']
    min_data = data[data['Fun'] == 'min']
    PB_data = data[data['Constructs'] == 'PB']
    PB_OB_data = data[data['Constructs'] == 'PB_OB']
    NoIndex_data = data[data['IDX'].str.startswith(' ')]
    SomeIndex_data = data[~data['IDX'].str.startswith(' ')]

    # IC_data = data[data['IDX'].str.startswith('I(C)')]
    # Not_IC_data = data[~data['IDX'].str.startswith('I(C)')]
    # IB_data = data[data['IDX'].str.contains('I\(B\)')]
    # Not_IB_data = data[~data['IDX'].str.contains('I\(B\)')]
    # IA_data = data[data['IDX'].str.contains('I\(A\)')]
    # Not_IA_data = data[~data['IDX'].str.contains('I\(A\)')]
    # IBA_data = data[data['IDX'].str.contains('I\(BA\)')]
    # IAB_data = data[data['IDX'].str.contains('I\(AB\)')]

    compute_means(data, dbms, has_cost, "Agg Microbenchmark, all data")
    compute_means(data[data['T2'] < 300000], dbms, has_cost, "Agg Microbenchmark, less than 5 minutes data")

    # plt.rcParams['font.size'] = 14

    attrcount = 11
    boxplot_dict = plt.boxplot([np.log10(data['T1'] / data['T2']),
                                np.log10(parallel_on['T1'] / parallel_on['T2']),
                                np.log10(parallel_off['T1'] / parallel_off['T2']),
                                np.log10(padding_on['T1'] / padding_on['T2']),
                                np.log10(padding_off['T1'] / padding_off['T2']),
                                np.log10(count_data['T1'] / count_data['T2']),
                                np.log10(min_data['T1'] / min_data['T2']),
                                np.log10(PB_data['T1'] / PB_data['T2']),
                                np.log10(PB_OB_data['T1'] / PB_OB_data['T2']),
                                np.log10(NoIndex_data['T1'] / NoIndex_data['T2']),
                                np.log10(SomeIndex_data['T1'] / SomeIndex_data['T2'])
                                ],

                               showfliers=True,
                               positions=[i for i in range(attrcount)],
                               labels=['ALL', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', 'COUNT',
                                       'MIN', 'PB', 'PB_OB', 'NO INDEX', 'SOME INDEX']
                               )

    # Modify the fliers marker style
    flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
    for flier in boxplot_dict['fliers']:
        flier.set(**flier_marker_style)

    # Define the colors for the desired boxes
    colors = ['red', 'red', 'green', 'green', 'orange', 'orange', 'cyan', 'cyan', 'magenta', 'magenta']

    # Loop through the desired boxes and modify their color
    for i in range(1, attrcount):
        box = boxplot_dict['boxes'][i]
        box.set(color=colors[i - 1])

    # plt.xticks(rotation=80)
    # plt.ylabel(r'$T_{lin}\,/\,T_{sj}$')
    # plt.title(print_caption)
    # plt.yticks(np.arange(-3, 5), 10.0 ** np.arange(-3, 5))

    plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1

    return boxplot_dict


fig, axs = plt.subplots(1, 4, figsize=(22, 7), sharex=True)

titlesize = 25
xlabelsize = 17

# First subplot
boxplot_dict1 = dbms_results('MSSql2', True, "ROW", axs[0])
axs[0].set_title('DBMS1', fontsize=titlesize)
axs[0].tick_params(axis='x', rotation=80)
axs[0].tick_params(axis='x', labelsize=xlabelsize)
axs[0].set_ylabel(r'$T_{lin}\,/\,T_{sj}$', fontsize=20)
axs[0].set_yticks(np.arange(-4, 5), [r'$10^{' + str(i) +  r'}$' for i in range(-4, 5)], fontsize=15)


# Second subplot
boxplot_dict2 = dbms_results('Postgres2', True, "ROW", axs[1])
axs[1].set_title('PostgreSQL', fontsize=titlesize)
axs[1].tick_params(axis='x', rotation=80)
axs[1].tick_params(axis='x', labelsize=xlabelsize)
axs[1].set_yticks(np.arange(-4, 5) , [])

# Third subplot
boxplot_dict3 = dbms_results('Oracle', False, "ROW", axs[2])
axs[2].set_title('DBMS2', fontsize=titlesize)
axs[2].tick_params(axis='x', rotation=80)
axs[2].tick_params(axis='x', labelsize=xlabelsize)
axs[2].set_yticks(np.arange(-4, 5), [])

# Fourth subplot
boxplot_dict4 = dbms_results('Hyper', False, "COLUMN", axs[3])
axs[3].set_title('Hyper', fontsize=titlesize)
axs[3].tick_params(axis='x', rotation=80)
axs[3].tick_params(axis='x', labelsize=xlabelsize)
axs[3].set_yticks(np.arange(-4, 5), [])

plt.tight_layout()
plt.savefig('agg_analysis.pdf', format='pdf')
plt.show()


# dbms_results('MSSql2', True, True, 'DBMS1')
#
# # dbms_results('MSSql', True, False, 'DBMS1')
# dbms_results('Postgres', False, False, 'PostgreSql')
# dbms_results('Oracle', False, False, 'DBMS2')
# dbms_results('Hyper', False, False, 'DBMS2')