import pandas as pd
import re
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np

def dbms_results(dbms, has_column, print_caption):
    global file, lines, pattern, line, data, column_data, row_data, parallel_on, parallel_off, padding_on, padding_off, count_data, min_data, PB_data, PB_OB_data, IC_data, Not_IC_data, IB_data, Not_IB_data, IA_data, Not_IA_data, IBA_data, IAB_data, X
    # Read the CSV file into a DataFrame, reading each row as a string
    with open('agg_' + dbms + '.txt', 'r') as file:
        lines = file.readlines()
    # Define the regular expression pattern for valid rows
    pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'
    # Filter out rows that do not match the pattern
    valid_lines = [line.strip().split(',') for line in lines if re.match(pattern, line.strip())]
    wrong_lines = [line for line in valid_lines if not len(line) == 11]
    # Create a DataFrame from the filtered lines
    data = pd.DataFrame(valid_lines,
                        columns=['T1', 'T2', 'PB', 'RS', 'Storage', 'IDX', 'Padding', 'Par', 'Alg', 'Fun', 'Constructs',
                                 'Sel'])
    # Convert numeric columns to the appropriate data types
    numeric_cols = ['T1', 'T2', 'PB', 'RS']
    data[numeric_cols] = data[numeric_cols].astype(int)

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
    count_data = data[data['Fun'] == 'count']
    min_data = data[data['Fun'] == 'min']
    PB_data = data[data['Constructs'] == 'PB']
    PB_OB_data = data[data['Constructs'] == 'PB_OB']
    IC_data = data[data['IDX'].str.startswith('I(C)')]
    Not_IC_data = data[~data['IDX'].str.startswith('I(C)')]
    IB_data = data[data['IDX'].str.contains('I\(B\)')]
    Not_IB_data = data[~data['IDX'].str.contains('I\(B\)')]
    IA_data = data[data['IDX'].str.contains('I\(A\)')]
    Not_IA_data = data[~data['IDX'].str.contains('I\(A\)')]
    IBA_data = data[data['IDX'].str.contains('I\(BA\)')]
    IAB_data = data[data['IDX'].str.contains('I\(AB\)')]

    # compute average value of T1
    print("----------------------------")
    print("DBMS: " + dbms)
    print("Average WF strategy time: ", data['T1'].mean() / 1000.0)
    print("Average Self-join strategy time: ", data['T2'].mean() / 1000.0)
    print("Number of tests: ", len(data))

    # Compute the geometric mean of T1/T2
    geometric_mean = stats.gmean(data['T1'] / data['T2'].replace(0, 1))
    print("Geometric Mean of T1/T2:", geometric_mean)

    plt.rcParams['font.size'] = 14
    def all_parameters():
        if has_column:
            boxplot_dict = plt.boxplot([np.log10(data['T1'] / data['T2']),
                                        np.log10(column_data['T1'] / column_data['T2']),
                                        np.log10(row_data['T1'] / row_data['T2']),
                                        np.log10(parallel_on['T1'] / parallel_on['T2']),
                                        np.log10(parallel_off['T1'] / parallel_off['T2']),
                                        np.log10(padding_on['T1'] / padding_on['T2']),
                                        np.log10(padding_off['T1'] / padding_off['T2']),
                                        np.log10(count_data['T1'] / count_data['T2']),
                                        np.log10(min_data['T1'] / min_data['T2']),
                                        np.log10(PB_data['T1'] / PB_data['T2']),
                                        np.log10(PB_OB_data['T1'] / PB_OB_data['T2']),
                                        np.log10(IBA_data['T1'] / IBA_data['T2']),
                                        np.log10(IAB_data['T1'] / IAB_data['T2']),
                                        np.log10(IB_data['T1'] / IB_data['T2']),
                                        np.log10(Not_IB_data['T1'] / Not_IB_data['T2']),
                                        np.log10(IC_data['T1'] / IC_data['T2']),
                                        np.log10(Not_IC_data['T1'] / Not_IC_data['T2']),
                                        np.log10(IA_data['T1'] / IA_data['T2']),
                                        np.log10(Not_IA_data['T1'] / Not_IA_data['T2'])
                                        ],

                                       showfliers=True,
                                       positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
                                       labels=['ALL', 'COLUMN', 'ROW', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING',
                                               'NO PADDING', 'COUNT',
                                               'MIN', 'PB', 'PB_OB', 'I(BA)', 'I(AB)', 'I(B)', '~I(B)', 'I(C)', '~I(C)',
                                               'I(A)', '~I(A)']
                                       )

            # Modify the fliers marker style
            flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
            for flier in boxplot_dict['fliers']:
                flier.set(**flier_marker_style)

            # Define the colors for the desired boxes
            colors = ['red', 'red', 'blue', 'blue', 'green', 'green', 'orange', 'orange', 'cyan', 'cyan', 'magenta',
                      'magenta', 'brown', 'brown', 'purple', 'purple']

            # Loop through the desired boxes and modify their color
            for i in range(1, 17):
                box = boxplot_dict['boxes'][i]
                box.set(color=colors[i - 1])
        else:
            boxplot_dict = plt.boxplot([np.log10(data['T1'] / data['T2']),
                                        np.log10(parallel_on['T1'] / parallel_on['T2']),
                                        np.log10(parallel_off['T1'] / parallel_off['T2']),
                                        np.log10(padding_on['T1'] / padding_on['T2']),
                                        np.log10(padding_off['T1'] / padding_off['T2']),
                                        np.log10(count_data['T1'] / count_data['T2']),
                                        np.log10(min_data['T1'] / min_data['T2']),
                                        np.log10(PB_data['T1'] / PB_data['T2']),
                                        np.log10(PB_OB_data['T1'] / PB_OB_data['T2']),
                                        np.log10(IBA_data['T1'] / IBA_data['T2']),
                                        np.log10(IAB_data['T1'] / IAB_data['T2']),
                                        np.log10(IB_data['T1'] / IB_data['T2']),
                                        np.log10(Not_IB_data['T1'] / Not_IB_data['T2']),
                                        np.log10(IA_data['T1'] / IA_data['T2']),
                                        np.log10(Not_IA_data['T1'] / Not_IA_data['T2']),
                                        np.log10(IC_data['T1'] / IC_data['T2']),
                                        np.log10(Not_IC_data['T1'] / Not_IC_data['T2'])
                                        ],

                                       showfliers=True,
                                       positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
                                       labels=['ALL', 'PARALLELIZED', 'SINGLE THREAD', 'PADDING', 'NO PADDING', 'COUNT',
                                               'MIN', 'PB',
                                               'PB_OB',
                                               'I(BA)', 'I(AB)', 'I(B)', '~I(B)', 'I(A)', '~I(A)', 'I(C)', '~I(C)']
                                       )

            # Modify the fliers marker style
            flier_marker_style = dict(marker='+', markerfacecolor='red', markersize=3, linestyle='none')
            for flier in boxplot_dict['fliers']:
                flier.set(**flier_marker_style)

            # Define the colors for the desired boxes
            colors = ['red', 'red', 'green', 'green', 'orange', 'orange', 'cyan', 'cyan', 'magenta', 'magenta',
                      'brown', 'brown', 'purple', 'purple', 'blue', 'blue']

            # Loop through the desired boxes and modify their color
            for i in range(1, 17):
                box = boxplot_dict['boxes'][i]
                box.set(color=colors[i - 1])

        plt.xticks(rotation=80)
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$')
        plt.title(print_caption)
        # plt.yscale('log')  # show the y-axis in log scale
        plt.yticks(np.arange(-3, 5), 10.0 ** np.arange(-3, 5))
        plt.axhline(y=0, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.2, right=0.97, top=0.94, bottom=0.35)  # Adjust the values as per your requirements
        plt.savefig(dbms + '_agg_all.pdf', format='pdf')
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
        plt.title(dbms + " BDistinct for ROW")
        plt.yscale('log')  # show the y-axis in log scale
        plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.11, right=0.97, top=0.94, bottom=0.13)  # Adjust the values as per your requirements
        plt.savefig(dbms + '_boxplot_bdistinct_row.pdf', format='pdf')
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
        plt.title(dbms + " BDistinct for ROW + I(B)")
        plt.yscale('log')  # show the y-axis in log scale
        plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.11, right=0.97, top=0.94, bottom=0.13)  # Adjust the values as per your requirements
        plt.savefig(dbms + '_boxplot_bdistinct_row_IB.pdf', format='pdf')

        # Show the plot
        plt.show()

    def sel():
        global i
        sel_data = []
        sel_values = ['1', '2', '4', '8', '16', '32']
        for i in range(len(sel_values)):
            sel_data.append(row_data[row_data['Sel'] == "<" + sel_values[i]])
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
        plt.ylabel(r'$T_{lin}\,/\,T_{sj}$')
        plt.xlabel('SEL')
        plt.title(print_caption + " Selectivity")
        plt.yscale('log')  # show the y-axis in log scale
        plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.15, right=0.97, top=0.94, bottom=0.15)  # Adjust the values as per your requirements
        plt.savefig(dbms + '_boxplot_sel.pdf', format='pdf')
        # Show the plot
        plt.show()

    def sel_Ib():
        global i
        sel_data = []
        sel_values = ['<1', '<2', '<4', '<8', '<16', '<32']
        for i in range(len(sel_values)):
            sel_data.append(IB_data[(IB_data['Sel'] == sel_values[i]) &
                                    (IB_data['PB'] > 100) &
                                    (IB_data['Storage'] == 'ROW')])
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
        plt.title(dbms + " Selectivity for ROW + I(B) with PB > 100")
        plt.yscale('log')  # show the y-axis in log scale
        plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1

        plt.subplots_adjust(left=0.11, right=0.97, top=0.94, bottom=0.1)  # Adjust the values as per your requirements
        plt.savefig(dbms + '_boxplot_sel_IB_gt100.pdf', format='pdf')

        # Show the plot
        plt.show()

    def X():
        global geometric_mean
        data_IB_ROW = data
        if dbms == 'MSSql':
            data_IB_ROW = data[
                (
                        (data['IDX'].str.contains('I\(BA')) &
                        (data['PB'] > 100) &
                        ((data['Fun'] == 'min') | (data['Padding'] == 'Padding_ON'))
                )
                |
                (
                        (data['IDX'].str.contains('I\(BA')) &
                        (data['PB'] > 30) &
                        (data['Storage'] == 'ROW')
                )
                |
                (
                        (data['Storage'] == 'ROW') &
                        (data['PB'] > 100) &
                        (data['Sel'] != '<32') &
                        (data['IDX'].str.contains('I\(B\)')) &
                        ((data['Par'] == 'parallel_OFF') |
                         (data['Padding'] == 'padding_ON'))
                )
                |
                (
                        (data['IDX'].str.contains('I\(B\)')) &
                        (data['Constructs'] == 'PB') &
                        (data['PB'] > 30) &
                        (data['Storage'] == 'ROW') &
                        ((data['Par'] == 'parallel_OFF') |
                         (data['Padding'] == 'padding_ON'))
                )
                ]
        if dbms == 'Postgres':
            data_IB_ROW = data[(data['Storage'] == 'ROW') &
                               (data['PB'] > 100) &
                               (data['Sel'] != '<32') &
                               (data['Sel'] != '<16') &
                               (data['Sel'] != '<8') &
                               (data['IDX'].str.contains('I\(B'))]
        if dbms == 'MySQL':
            data_IB_ROW = data[(data['Storage'] == 'ROW') &
                               (data['PB'] > 100) &
                               (data['Sel'] != '<32') &
                               (data['Sel'] != '<16') &
                               (data['Sel'] != '<8') &
                               (data['IDX'].str.contains('I\(B'))]
        if dbms == 'Oracle':
            # data_IB_ROW = data[(data['Storage'] == 'ROW') &
            #                     (data['PB'] >= 100) &
            #                     (data['IDX'].str.contains('I\(BA')) ]
            data_IB_ROW = data[
                (
                        (data['Storage'] == 'ROW') &
                        (data['PB'] >= 3000) &
                        (data['Sel'] != '<32') &
                        (data['IDX'].str.contains('I\(B\)')) &
                        ((data['Fun'] == 'min') | (data['Par'] == 'parallel_OFF') |
                         (data['Padding'] == 'padding_ON'))
                ) |
                (
                        (data['Storage'] == 'ROW') &
                        (data['PB'] > 100) &
                        (data['IDX'].str.contains('I\(BA'))
                )
                ]
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
        data_IB_ROW_IA = data_IB_ROW[data_IB_ROW['IDX'].str.contains('I\(A')]
        data_IB_ROW_notIA = data_IB_ROW[~data_IB_ROW['IDX'].str.contains('I\(A')]
        data_IB_ROW_IC = data_IB_ROW[data_IB_ROW['IDX'].str.contains('I\(C')]
        data_IB_ROW_notIC = data_IB_ROW[~data_IB_ROW['IDX'].str.contains('I\(C')]
        # Create the box plot
        plt.boxplot([data_IB_ROW['T1'] / data_IB_ROW['T2'],
                     data_IB_ROW_count['T1'] / data_IB_ROW_count['T2'],
                     data_IB_ROW_min['T1'] / data_IB_ROW_min['T2'],
                     data_IB_ROW_parallelon['T1'] / data_IB_ROW_parallelon['T2'],
                     data_IB_ROW_paralleloff['T1'] / data_IB_ROW_paralleloff['T2'],
                     data_IB_ROW_paddingon['T1'] / data_IB_ROW_paddingon['T2'],
                     data_IB_ROW_paddingoff['T1'] / data_IB_ROW_paddingoff['T2'],
                     data_IB_ROW_IA['T1'] / data_IB_ROW_IA['T2'],
                     data_IB_ROW_notIA['T1'] / data_IB_ROW_notIA['T2'],
                     data_IB_ROW_IC['T1'] / data_IB_ROW_IC['T2'],
                     data_IB_ROW_notIC['T1'] / data_IB_ROW_notIC['T2']
                     ],
                    showfliers=False,
                    positions=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                    labels=['all', 'count', 'min', 'parallel_ON', 'parallel_OFF', 'padding_ON', 'padding_OFF', 'I(A)',
                            '~I(A)', 'I(C)', '~I(C)'])
        plt.xticks(rotation=45)
        plt.ylabel('T1/T2')
        plt.title(dbms + ' - X subset')
        plt.yscale('log')  # show the y-axis in log scale
        plt.axhline(y=1, color='r', linestyle='-')  # add horizontal line at value 1
        plt.subplots_adjust(left=0.11, right=0.97, top=0.94, bottom=0.2)  # Adjust the values as per your requirements
        plt.savefig(dbms + '_boxplot_parameters_X.pdf', format='pdf')
        # Show the plot
        plt.show()

    ###################################################################
    # Create the box plot
    all_parameters()
    ###################################################################
    # Create the next box plot for T1/T2 based on PB value
    # bvalues()
    ###################################################################
    # Create the next box plot for T1/T2 based on PB value
    # bvalues_Bindex()
    ###################################################################
    # Create the next box plot for T1/T2 based on Sel value
    sel()
    ###################################################################
    # Create the next box plot for T1/T2 based on Sel value with I(B) index
    # sel_Ib()
    ###################################################################
    # filtered_data = data[(data['Storage'] == 'ROW') &
    #                     (data['IDX'].str.contains('I\(B')) &
    #                     ((data['Sel'] != '<16') & (data['Sel'] != '<32')) &
    #                      (((data['PB'] > 300) & (data['Par'] == 'parallel_OFF')) |
    #                      ((data['PB'] > 3000) & (data['Par'] == 'parallel_ON')))
    #                         ]
    # # print filtered_data size
    # print('Data size: ' + str(len(data)))
    # print('Filtered data size: ' + str(len(filtered_data)))
    # # print('data_IB_ROW data size: ' + str(len(data_IB_ROW)))
    #
    # selected_rows = filtered_data[(filtered_data['T1']/filtered_data['T2'] < 0.9)]
    #
    # # Print the selected rows
    # for i in range(len(selected_rows)):
    #     row_values = selected_rows.iloc[i].tolist()
    #     row_string = ', '.join(map(str, row_values))
    #     print(row_string)
    ###################################################################
    # X()


dbms_results('MSSql', True, 'DBMS1')
dbms_results('Postgres', False, 'PostgreSql')
dbms_results('Oracle', False, 'DBMS2')
# dbms_results('MySQL', False)
