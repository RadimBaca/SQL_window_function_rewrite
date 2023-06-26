import pandas as pd
import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np

dbms = 'MSSql'
has_column = True

# dbms = 'Postgres'
# has_column = False

# Read the CSV file into a DataFrame, reading each row as a string
with open('rank_' + dbms +'.txt', 'r') as file:
    lines = file.readlines()

# Define the regular expression pattern for valid rows
pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'



# Filter out rows that do not match the pattern
valid_lines = [line.strip().split(',') for line in lines if re.match(pattern, line.strip())]
wrong_lines = [line for line in valid_lines if not len(line) == 11]

# Create a DataFrame from the filtered lines
data = pd.DataFrame(valid_lines, columns=['T1','T2','PB','RS','Storage','IDX','Padding','Par','Cmp'])

# Convert numeric columns to the appropriate data types
numeric_cols = ['T1', 'T2', 'PB', 'RS']
data[numeric_cols] = data[numeric_cols].astype(int)




###########################################################################
# Filter rows based on conditions and compute statistics
column_data = data[data['Storage'] == 'COLUMN']
row_data = data[data['Storage'] == 'ROW']
parallel_on = data[data['Par'] == 'parallel_ON']
parallel_off = data[data['Par'] == 'parallel_OFF']
padding_on = data[data['Padding'] == 'padding_ON']
padding_off = data[data['Padding'] == 'padding_OFF']
equal1_data = data[data['Cmp'] == '=1']
equalN_data = data[data['Cmp'] == '=N']
lessN_data = data[data['Cmp'] == '<N']
IB_data = data[data['IDX'].str.contains('I\(B')]
Not_IB_data = data[~data['IDX'].str.contains('I\(B')]
IA_data = data[data['IDX'].str.contains('I\(A')]
Not_IA_data = data[~data['IDX'].str.contains('I\(A')]
IBA_data = data[data['IDX'].str.contains('I\(BA\)')]
IAB_data = data[data['IDX'].str.contains('I\(AB\)')]


# Number of rows where T1/T2 is larger than 10 for COLUMN storage
column_gt_10 = len(column_data[(column_data['T1'] / column_data['T2']) > 10])
row_gt_10 = len(row_data[(row_data['T1'] / row_data['T2']) > 10])

# Compute the geometric mean of T1/T2
geometric_mean = stats.gmean(data['T1'] / data['T2'].replace(0, 1))

print("Geometric Mean of T1/T2:", geometric_mean)


def all_parameters():
    if has_column:
        boxplot_dict = plt.boxplot([data['T1'] / data['T2'],
                     column_data['T1'] / column_data['T2'],
                     row_data['T1'] / row_data['T2'],
                     parallel_on['T1'] / parallel_on['T2'],
                     parallel_off['T1'] / parallel_off['T2'],
                     padding_on['T1'] / padding_on['T2'],
                     padding_off['T1'] / padding_off['T2'],
                     equal1_data['T1'] / equal1_data['T2'],
                     equalN_data['T1'] / equalN_data['T2'],
                     lessN_data['T1'] / lessN_data['T2'],
                                    IBA_data['T1'] / IBA_data['T2'],
                                    IAB_data['T1'] / IAB_data['T2'],
                                    IB_data['T1'] / IB_data['T2'],
                     Not_IB_data['T1'] / Not_IB_data['T2'],
                     IA_data['T1'] / IA_data['T2'],
                     Not_IA_data['T1'] / Not_IA_data['T2']
                     ],

                    showfliers=False,
                    positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,15,16],
                    labels=['ALL', 'COLUMN', 'ROW', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', '= 1',
                            '= N', '< N', 'I(BA)', 'I(AB)', 'I(B)', '~I(B)', 'I(A)', '~I(A)']
                    )
        # Define the colors for the desired boxes
        colors = ['red', 'red', 'blue', 'blue','green', 'green', 'orange', 'orange', 'orange', 'brown', 'brown', 'cyan', 'cyan', 'magenta', 'magenta']

        # Loop through the desired boxes and modify their color
        for i in range(1, 16):
            box = boxplot_dict['boxes'][i]
            box.set(color=colors[i - 1])

    else:
        plt.boxplot([data['T1'] / data['T2'],
                     parallel_on['T1'] / parallel_on['T2'],
                     parallel_off['T1'] / parallel_off['T2'],
                     padding_on['T1'] / padding_on['T2'],
                     padding_off['T1'] / padding_off['T2'],
                     equal1_data['T1'] / equal1_data['T2'],
                     equalN_data['T1'] / equalN_data['T2'],
                     lessN_data['T1'] / lessN_data['T2'],
                                    IBA_data['T1'] / IBA_data['T2'],
                                    IAB_data['T1'] / IAB_data['T2'],
                     IB_data['T1'] / IB_data['T2'],
                     Not_IB_data['T1'] / Not_IB_data['T2'],
                     IA_data['T1'] / IA_data['T2'],
                     Not_IA_data['T1'] / Not_IA_data['T2']
                     ],

                    showfliers=False,
                    positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13,14],
                    labels=['ALL', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', '= 1',
                            '= N', '< N', 'I(BA)', 'I(AB)', 'I(B)', '~I(B)', 'I(A)', '~I(A)']
                    )
        # Define the colors for the desired boxes
        colors = ['blue', 'blue','green', 'green', 'orange', 'orange', 'orange', 'brown', 'brown', 'cyan', 'cyan', 'magenta', 'magenta']

        # Loop through the desired boxes and modify their color
        for i in range(1, 14):
            box = boxplot_dict['boxes'][i]
            box.set(color=colors[i - 1])

    plt.xticks(rotation=45)
    plt.ylabel('T1/T2')
    plt.title(dbms)
    plt.yscale('log')  # show the y-axis in log scale
    plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1

    plt.subplots_adjust(left=0.11, right=0.97, top=0.94, bottom=0.2)  # Adjust the values as per your requirements
    plt.savefig(dbms + '_rank_all.pdf', format='pdf')

    # Show the plot
    plt.show()



def bvalues():
    global i
    pb_data = []
    pb_values = [10, 30, 100, 300, 1000, 3000, 10000, 30000]
    for i in range(len(pb_values)):
        pb_data.append(row_data[row_data['PB'] == pb_values[i]])
    # Create the box plots for bp_data
    plt.boxplot([pb_data[0]['T1'] / pb_data[0]['T2'],
                 pb_data[1]['T1'] / pb_data[1]['T2'],
                 pb_data[2]['T1'] / pb_data[2]['T2'],
                 pb_data[3]['T1'] / pb_data[3]['T2'],
                 pb_data[4]['T1'] / pb_data[4]['T2'],
                 pb_data[5]['T1'] / pb_data[5]['T2'],
                 pb_data[6]['T1'] / pb_data[6]['T2'],
                 pb_data[7]['T1'] / pb_data[7]['T2']
                 ],
                showfliers=False,
                positions=[1, 2, 3, 4, 5, 6, 7, 8],
                labels=pb_values
                )
    plt.xticks(rotation=45)
    plt.ylabel('T1/T2')
    plt.title(dbms)
    plt.yscale('log')  # show the y-axis in log scale
    plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
    # Show the plot
    plt.show()

def bvalues_Bindex():
    global i
    pb_data = []
    pb_values = [10, 30, 100, 300, 1000, 3000, 10000, 30000]
    for i in range(len(pb_values)):
        pb_data.append(row_data[(row_data['PB'] == pb_values[i]) & (row_data['IDX'].str.contains('I\(B'))])
    # Create the box plots for bp_data
    plt.boxplot([pb_data[0]['T1'] / pb_data[0]['T2'],
                 pb_data[1]['T1'] / pb_data[1]['T2'],
                 pb_data[2]['T1'] / pb_data[2]['T2'],
                 pb_data[3]['T1'] / pb_data[3]['T2'],
                 pb_data[4]['T1'] / pb_data[4]['T2'],
                 pb_data[5]['T1'] / pb_data[5]['T2'],
                 pb_data[6]['T1'] / pb_data[6]['T2'],
                 pb_data[7]['T1'] / pb_data[7]['T2']
                 ],
                showfliers=False,
                positions=[1, 2, 3, 4, 5, 6, 7, 8],
                labels=pb_values
                )
    plt.xticks(rotation=45)
    plt.ylabel('T1/T2')
    plt.title(dbms)
    plt.yscale('log')  # show the y-axis in log scale
    plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
    # Show the plot
    plt.show()


###################################################################
# Create the box plot
all_parameters()

###################################################################
# Create the next box plot for T1/T2 based on PB value
bvalues()

###################################################################
# Create the next box plot for T1/T2 based on PB value
bvalues_Bindex()

###################################################################
# Create the next box plot for T1/T2 based on Sel value
# sel()

###################################################################
# filtered_data = data[(data['Storage'] == 'ROW') &
#                     (data['IDX'].str.contains('I\(B')) &
#                     ((data['Sel'] != '<16') & (data['Sel'] != '<32')) &
#                      (((data['PB'] > 300) & (data['Par'] == 'parallel_OFF')) |
#                      ((data['PB'] > 3000) & (data['Par'] == 'parallel_ON')))
#                         ]
# print filtered_data size
# print('Data size: ' + str(len(data)))
# print('Filtered data size: ' + str(len(filtered_data)))
# print('data_IB_ROW data size: ' + str(len(data_IB_ROW)))
#
# selected_rows = filtered_data[(filtered_data['T1']/filtered_data['T2'] < 0.9)]
#
# # Print the selected rows
# for i in range(len(selected_rows)):
#     row_values = selected_rows.iloc[i].tolist()
#     row_string = ', '.join(map(str, row_values))
#     print(row_string)
#

##################################################################
data_IB_ROW = data
if dbms == 'MSSql':
    data_IB_ROW = data[(data['Storage'] == 'ROW') &
                        (data['PB'] < 3000) ]
if dbms == 'Postgres':
    data_IB_ROW = data[(data['Storage'] == 'ROW') &
                        (data['PB'] < 1000) &
                        (data['IDX'].str.contains('I\(B')) ]

print('Data size: ' + str(len(data)))
print('data_IB_ROW data size: ' + str(len(data_IB_ROW)))

# Compute the geometric mean of T1/T2
geometric_mean = stats.gmean(data_IB_ROW['T1'] / data_IB_ROW['T2'].replace(0, 1))
print("Geometric Mean of T1/T2:", geometric_mean)

data_IB_ROW_equal1 = data_IB_ROW[data_IB_ROW['Cmp'] == '=1']
data_IB_ROW_equalN = data_IB_ROW[data_IB_ROW['Cmp'] == '=N']
data_IB_ROW_parallelon = data_IB_ROW[data_IB_ROW['Par'] == 'parallel_ON']
data_IB_ROW_paralleloff = data_IB_ROW[data_IB_ROW['Par'] == 'parallel_OFF']
data_IB_ROW_paddingon = data_IB_ROW[data_IB_ROW['Padding'] == 'padding_ON']
data_IB_ROW_paddingoff = data_IB_ROW[data_IB_ROW['Padding'] == 'padding_OFF']


# Create the box plot
plt.boxplot([data_IB_ROW['T1'] / data_IB_ROW['T2'],
             data_IB_ROW_equal1['T1'] / data_IB_ROW_equal1['T2'],
             data_IB_ROW_equalN['T1'] / data_IB_ROW_equalN['T2'],
             data_IB_ROW_parallelon['T1'] / data_IB_ROW_parallelon['T2'],
             data_IB_ROW_paralleloff['T1'] / data_IB_ROW_paralleloff['T2'],
             data_IB_ROW_paddingon['T1'] / data_IB_ROW_paddingon['T2'],
             data_IB_ROW_paddingoff['T1'] / data_IB_ROW_paddingoff['T2']
             ],
            showfliers=False,
            positions=[1, 2, 3, 4, 5, 6, 7],
            labels=['all', '=1', '=N', 'parallel_ON', 'parallel_OFF', 'padding_ON', 'padding_OFF'])

plt.xticks(rotation=45)
plt.xlabel('T1/T2')
plt.title(dbms)
plt.yscale('log') # show the y-axis in log scale
plt.axhline(y=1, color='r', linestyle='-') # add horizontal line at value 1

plt.subplots_adjust(left=0.11, right=0.97, top=0.94, bottom=0.2)  # Adjust the values as per your requirements
plt.savefig(dbms + '_rank_X.pdf', format='pdf')


# Show the plot
plt.show()
