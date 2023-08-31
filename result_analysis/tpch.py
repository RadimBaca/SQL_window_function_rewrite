## This script is used to plot the results of selected queris of the TPC-H benchmark

import matplotlib.pyplot as plt

# Data
DBMS = ['PostgreSQL', 'DBMS1', 'DBMS2']


LinQ2 = [11.3, 7.68, 113.2]
SJQ2 = [4.5, 0.22,0.56]

LinQ15 = [12.57, 0.08, 0.28]
SJQ15 = [24.5, 0.2, 0.54]

LinQ17 = [143.2, 114, 2.7]
SJQ17 = [1.27, 0.22, 0.93]




def compare_times(SJtimes, Lintimes, title):
    global ax, x
    # Determine the number of DBMS
    num_dbms = len(DBMS)
    # Set the width of the bars
    bar_width = 0.35


    plt.rcParams['font.size'] = 14

    # Create an array for the x-axis positions of the bars
    x_positions = range(num_dbms)
    # Create the figure and axes
    fig, ax = plt.subplots()
    # Plot the Lin values
    rects1 = ax.bar(x_positions, Lintimes, bar_width, label=r'$T_{lin}$', log=True, color='blue')
    # Plot the SJ values next to the Lin values
    rects2 = ax.bar([x + bar_width for x in x_positions], SJtimes, bar_width, label=r'$T_{sj}$', log=True, color='green')
    # Set the x-axis ticks and labels
    ax.set_xticks([x + bar_width / 2 for x in x_positions])
    ax.set_xticklabels(DBMS)
    # Set the axis labels and title
    # ax.set_xlabel('DBMS')
    ax.set_ylabel(r'$T$ [ms]')
    ax.set_title(title)
    # Add a legend
    ax.legend()
    # Show the plot
    plt.tight_layout()
    # Save the plot
    plt.savefig(title + '.pdf', format='pdf')
    plt.show()



compare_times(SJQ2, LinQ2, "Q2")

compare_times(SJQ15, LinQ15, "Q15")

compare_times(SJQ17, LinQ17, "Q17")