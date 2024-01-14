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


    # Filter out results of column storage
    data = data[data['Storage'] == storage]

    boxplot_dict = plt.boxplot([np.log10(data['T1']),
                 np.log10(data['T2'])
                 ],
                showfliers=True,
                positions=[1, 2],
                labels=[r'Linear', r'Self-join']
                )

    # Modify the fliers marker style
    flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
    for flier in boxplot_dict['fliers']:
        flier.set(**flier_marker_style)

    # plt.ylabel(r'$T$ [ms]')
    # plt.title(local_print_caption)

    # plt.yscale('log')  # show the y-axis in log scale
    # plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
    # if we use np.log10, we need to use the following lines
    # plt.yticks(np.arange(0, 6), 10.0 ** (np.arange(0, 6)))
    # plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1


    # plt.subplots_adjust(left=0.2, right=0.97, top=0.94, bottom=0.1)  # Adjust the values as per your requirements
    # plt.savefig(print_caption + '_times.pdf', format='pdf')

    # Show the plot
    return boxplot_dict



#
fig, axs = plt.subplots(1, 4, figsize=(10, 5.5), sharex=True)
#
titlesize = 25
xlabelsize = 16

# First subplot
boxplot_dict1 = dbms_results('MSSql2', 'DBMS1', True, "ROW", axs[0])
axs[0].set_title('DBMS1', fontsize=titlesize)
axs[0].tick_params(axis='x', labelsize=xlabelsize)
axs[0].set_ylabel(r'$T$ [ms]', fontsize=20)
axs[0].set_yticks(np.arange(0, 6), [r'$10^{' + str(i) +  r'}$' for i in range(0, 6)], fontsize=17)


# Second subplot
boxplot_dict2 = dbms_results('Postgres2', 'PostgreSql', True, "ROW", axs[1])
axs[1].set_title('PostgreSQL', fontsize=titlesize)
axs[1].tick_params(axis='x', labelsize=xlabelsize)
axs[1].set_yticks(np.arange(0, 6) , [])

# Third subplot
boxplot_dict3 = dbms_results('Oracle', 'DBMS2', False, "ROW", axs[2])
axs[2].set_title('DBMS2', fontsize=titlesize)
axs[2].tick_params(axis='x', labelsize=xlabelsize)
axs[2].set_yticks(np.arange(0, 6), [])

# Fourth subplot
boxplot_dict4 = dbms_results('Hyper', 'Hyper', False, "COLUMN", axs[3])
axs[3].set_title('Hyper', fontsize=titlesize)
axs[3].tick_params(axis='x', labelsize=xlabelsize)
axs[3].set_yticks(np.arange(0, 6), [])

plt.tight_layout()
plt.savefig('rank_times.pdf', format='pdf')
plt.show()



# dbms_results('MSSql2', 'DBMS1', True, "ROW")
# dbms_results('Postgres2', 'PostgreSql', True, "ROW")
#
# # dbms_results('MSSql', 'DBMS1', False, "ROW")
# # dbms_results('Postgres', 'PostgreSql', False, "ROW")
# dbms_results('Oracle', 'DBMS2', False, "ROW")
# dbms_results('Hyper', 'Hyper', False, "COLUMN")
