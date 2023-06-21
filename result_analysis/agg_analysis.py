import pandas as pd
import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np

dbms = 'MSSql'
has_column = True

# dbms = 'Postgres'
# has_column = False
#
# dbms = 'Oracle'
# has_column = False
#
# dbms = 'MySQL'
# has_column = False

# Read the CSV file into a DataFrame, reading each row as a string
with open('agg_' + dbms +'.txt', 'r') as file:
    lines = file.readlines()

# Define the regular expression pattern for valid rows
pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'



# Filter out rows that do not match the pattern
valid_lines = [line.strip().split(',') for line in lines if re.match(pattern, line.strip())]
wrong_lines = [line for line in valid_lines if not len(line) == 11]

# Create a DataFrame from the filtered lines
data = pd.DataFrame(valid_lines, columns=['T1','T2','PB','RS','Storage','IDX','Padding','Par','Fun','Constructs','Sel'])

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
count_data = data[data['Fun'] == 'count']
min_data = data[data['Fun'] == 'min']
PB_data = data[data['Constructs'] == 'PB']
PB_OB_data = data[data['Constructs'] == 'PB_OB']
IC_data = data[data['IDX'].str.startswith('I(C)')]
Not_IC_data = data[~data['IDX'].str.startswith('I(C)')]
IB_data = data[data['IDX'].str.contains('I\(B')]
Not_IB_data = data[~data['IDX'].str.contains('I\(B')]
IA_data = data[data['IDX'].str.contains('I\(A')]
Not_IA_data = data[~data['IDX'].str.contains('I\(A')]


# Number of rows where T1/T2 is larger than 10 for COLUMN storage
column_gt_10 = len(column_data[(column_data['T1'] / column_data['T2']) > 10])
row_gt_10 = len(row_data[(row_data['T1'] / row_data['T2']) > 10])

# Compute the geometric mean of T1/T2
geometric_mean = stats.gmean(data['T1'] / data['T2'].replace(0, 1))

print("Geometric Mean of T1/T2:", geometric_mean)


def all_parameters():
    if has_column:
        plt.boxplot([data['T1'] / data['T2'],
                     column_data['T1'] / column_data['T2'],
                     row_data['T1'] / row_data['T2'],
                     parallel_on['T1'] / parallel_on['T2'],
                     parallel_off['T1'] / parallel_off['T2'],
                     padding_on['T1'] / padding_on['T2'],
                     padding_off['T1'] / padding_off['T2'],
                     count_data['T1'] / count_data['T2'],
                     min_data['T1'] / min_data['T2'],
                     PB_data['T1'] / PB_data['T2'],
                     PB_OB_data['T1'] / PB_OB_data['T2'],
                     IB_data['T1'] / IB_data['T2'],
                     Not_IB_data['T1'] / Not_IB_data['T2'],
                     IC_data['T1'] / IC_data['T2'],
                     Not_IC_data['T1'] / Not_IC_data['T2'],
                     IA_data['T1'] / IA_data['T2'],
                     Not_IA_data['T1'] / Not_IA_data['T2']
                     ],

                    showfliers=False,
                    positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
                    labels=['ALL', 'COLUMN', 'ROW', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', 'COUNT',
                            'MIN', 'PB', 'PB_OB', 'I(B)', '~I(B)', 'I(C)', '~I(C)', 'I(A)', '~I(A)']
                    )
    else:
        plt.boxplot([data['T1'] / data['T2'],
                     parallel_on['T1'] / parallel_on['T2'],
                     parallel_off['T1'] / parallel_off['T2'],
                     padding_on['T1'] / padding_on['T2'],
                     padding_off['T1'] / padding_off['T2'],
                     count_data['T1'] / count_data['T2'],
                     min_data['T1'] / min_data['T2'],
                     PB_data['T1'] / PB_data['T2'],
                     PB_OB_data['T1'] / PB_OB_data['T2'],
                     IB_data['T1'] / IB_data['T2'],
                     Not_IB_data['T1'] / Not_IB_data['T2'],
                     IC_data['T1'] / IC_data['T2'],
                     Not_IC_data['T1'] / Not_IC_data['T2'],
                     IA_data['T1'] / IA_data['T2'],
                     Not_IA_data['T1'] / Not_IA_data['T2']
                     ],

                    showfliers=False,
                    # positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                    # labels=['ALL', 'COLUMN', 'ROW', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', 'COUNT', 'MIN', 'PB', 'PB_OB', 'I(A)', '~I(A)']
                    positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    labels=['ALL', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', 'COUNT', 'MIN', 'PB',
                            'PB_OB',
                            'I(B)', '~I(B)', 'I(C)', '~I(C)', 'I(A)', '~I(A)']
                    )
    plt.xticks(rotation=45)
    plt.ylabel('T1/T2')
    plt.title(dbms)
    plt.yscale('log')  # show the y-axis in log scale
    plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
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

def sel():
    global i
    sel_data = []
    sel_values = ['<1', '<2', '<4', '<8', '<16', '<32']
    for i in range(len(sel_values)):
        sel_data.append(row_data[row_data['Sel'] == sel_values[i]])
    # Create the box plots for bp_data
    plt.boxplot([sel_data[0]['T1'] / sel_data[0]['T2'],
                 sel_data[1]['T1'] / sel_data[1]['T2'],
                 sel_data[2]['T1'] / sel_data[2]['T2'],
                 sel_data[3]['T1'] / sel_data[3]['T2'],
                 sel_data[4]['T1'] / sel_data[4]['T2'],
                 sel_data[5]['T1'] / sel_data[5]['T2']
                 ],
                showfliers=False,
                positions=[1, 2, 3, 4, 5, 6],
                labels=sel_values
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
# bvalues_Bindex()

###################################################################
# Create the next box plot for T1/T2 based on Sel value
sel()

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

###################################################################
data_IB_ROW = data
if dbms == 'MSSql':
    data_IB_ROW = data[(data['Storage'] == 'ROW') &
                        (data['PB'] > 100) &
                        (data['Sel'] != '<32') &
                        (data['IDX'].str.contains('I\(B')) ]
if dbms == 'Postgres':
    data_IB_ROW = data[(data['Storage'] == 'ROW') &
                        (data['PB'] > 100) &
                        (data['Sel'] != '<32') &
                        (data['Sel'] != '<16') &
                        (data['Sel'] != '<8') &
                        (data['IDX'].str.contains('I\(B')) ]

if dbms == 'MySQL':
    data_IB_ROW = data[(data['Storage'] == 'ROW') &
                        (data['PB'] > 100) &
                        (data['Sel'] != '<32') &
                        (data['Sel'] != '<16') &
                        (data['Sel'] != '<8') &
                        (data['IDX'].str.contains('I\(B')) ]

print('Data size: ' + str(len(data)))
print('data_IB_ROW data size: ' + str(len(data_IB_ROW)))

# Compute the geometric mean of T1/T2
geometric_mean = stats.gmean(data_IB_ROW['T1'] / data_IB_ROW['T2'].replace(0, 1))
print("Geometric Mean of T1/T2:", geometric_mean)

data_IB_ROW_count = data_IB_ROW[data_IB_ROW['Fun'] == 'count']
data_IB_ROW_min = data_IB_ROW[data_IB_ROW['Fun'] == 'min']
data_IB_ROW_parallelon = data_IB_ROW[data_IB_ROW['Par'] == 'parallel_ON']
data_IB_ROW_paralleloff = data_IB_ROW[data_IB_ROW['Par'] == 'parallel_OFF']
data_IB_ROW_paddingon = data_IB_ROW[data_IB_ROW['Padding'] == 'padding_ON']
data_IB_ROW_paddingoff = data_IB_ROW[data_IB_ROW['Padding'] == 'padding_OFF']


# Create the box plot
plt.boxplot([data_IB_ROW['T1'] / data_IB_ROW['T2'],
             data_IB_ROW_count['T1'] / data_IB_ROW_count['T2'],
            data_IB_ROW_min['T1'] / data_IB_ROW_min['T2'],
             data_IB_ROW_parallelon['T1'] / data_IB_ROW_parallelon['T2'],
            data_IB_ROW_paralleloff['T1'] / data_IB_ROW_paralleloff['T2'],
             data_IB_ROW_paddingon['T1'] / data_IB_ROW_paddingon['T2'],
            data_IB_ROW_paddingoff['T1'] / data_IB_ROW_paddingoff['T2']
             ],
            showfliers=False,
            positions=[1, 2, 3, 4, 5, 6, 7],
            labels=['all', 'count', 'min', 'parallel_ON', 'parallel_OFF', 'padding_ON', 'padding_OFF'])

plt.xticks(rotation=45)
plt.xlabel('T1/T2')
plt.title(dbms)
plt.yscale('log') # show the y-axis in log scale


plt.axhline(y=1, color='r', linestyle='-') # add horizontal line at value 1

# Show the plot
plt.show()
