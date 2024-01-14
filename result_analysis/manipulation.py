import re
import pandas as pd
import scipy.stats as stats

def read_data(has_cost, lines, columns):
    if has_cost:
        all_columns = ['T1', 'T2', 'COST_T1', 'COST_T2', 'PB', 'RS', 'Storage', 'IDX', 'Padding', 'Par',
                                    'Alg'] + columns
        valid_rows_pattern = r'^\d+,\d+,\d+\.\d+,[\dEe.+-]+,\d+,\d+,(?:ROW|COLUMN),.*$'
        # Filter out rows that do not match the pattern
        valid_lines = [line.strip().split(',') for line in lines if re.match(valid_rows_pattern, line.strip())]
        data = pd.DataFrame(valid_lines,
                            columns=all_columns)
        # Convert numeric columns to the appropriate data types
        numeric_cols = ['T1', 'T2', 'COST_T1', 'COST_T2', 'PB', 'RS']
        data[numeric_cols] = data[numeric_cols].astype(float)
        # If column data['T2'] is larger than 300000, then set it to 300000
        data.loc[data['T2'] > 300000, 'T2'] = 300000
        return data
    else:
        all_columns = ['T1', 'T2', 'PB', 'RS', 'Storage', 'IDX', 'Padding', 'Par', 'Alg'] + columns
        valid_rows_pattern = r'^\d+,\d+,\d+,\d+,(?:ROW|COLUMN),.*$'
        # Filter out rows that do not match the pattern
        valid_lines = [line.strip().split(',') for line in lines if re.match(valid_rows_pattern, line.strip())]
        # wrong_lines = [line for line in valid_lines if not len(line) == 11]

        data = pd.DataFrame(valid_lines, columns=all_columns)
        # Convert numeric columns to the appropriate data types
        numeric_cols = ['T1', 'T2', 'PB', 'RS']
        data[numeric_cols] = data[numeric_cols].astype(int)
        # If column data['T2'] is larger than 300000, then set it to 300000
        data.loc[data['T2'] > 300000, 'T2'] = 300000
        return data


def compute_means(data, dbms, has_cost, intro_text):
    print("----------------------------")
    print("DBMS: " + dbms)
    print(intro_text)
    print("Mean linear strategy time: ", data['T1'].mean())
    print("Mean self-join strategy time: ", data['T2'].mean())
    print("Number of tests: ", len(data))
    # Compute the geometric mean of T1/T2
    geometric_mean = stats.gmean(data['T1'] / data['T2'].replace(0, 1))
    print("Geometric Mean of T1/T2:", geometric_mean)
    print("Number of rows reaching the 300s limit:", len(data[data['T2'] >= 300000]) / len(data) * 100, "% (",
          len(data[data['T2'] >= 300000]), "/", len(data), ")")
    if has_cost:
        better_time_data = data[['T1', 'T2']].min(axis=1)
        cost_time_data = data.apply(lambda row: row['T1'] if row['COST_T1'] < row['COST_T2'] else row['T2'], axis=1)
        print("Mean time of optimal stategy:", better_time_data.mean())
        print("Mean time of cost-based strategy:", cost_time_data.mean())
        # print("Avg of best times:", data['BETTER_TIME'].sum() / len(data))
        # print("Avg of cost times:", data['COST_TIME'].sum() / len(data))

        missmatch_cost1 = data[(data['T1'] > data['T2']) & (data['COST_T1'] < data['COST_T2'])]
        missmatch_cost2 = data[(data['T2'] > data['T1']) & (data['COST_T2'] < data['COST_T1'])]
        print("T_lin false positives:", len(missmatch_cost1) / len(data) * 100, "% (",
              len(missmatch_cost1), "/", len(data), ")")
        print("T_sj false positives:", len(missmatch_cost2) / len(data) * 100, "% (",
              len(missmatch_cost2), "/", len(data), ")")

        significant_missmatch_cost1 = data[(data['T1'] > data['T2'] * 2) & (data['COST_T1'] < data['COST_T2'])]
        significant_missmatch_cost2 = data[(data['T2'] > data['T1'] * 2) & (data['COST_T2'] < data['COST_T1'])]
        print("T_lin signifincat false positives:", len(significant_missmatch_cost1) / len(data) * 100, "% (",
              len(significant_missmatch_cost1), "/", len(data), ")")
        print("T_sj significant false positives:", len(significant_missmatch_cost2) / len(data) * 100, "% (",
              len(significant_missmatch_cost2), "/", len(data), ")")

        # The penultimate row, and the last row indicates how often a false selection of a linear strategy, and a self-join strategy occurs, respectively.
