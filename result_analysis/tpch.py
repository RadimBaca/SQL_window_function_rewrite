import matplotlib.pyplot as plt
import numpy as np

# Data
DBMS = ['DBMS1', 'PostgreSQL', 'DBMS2', 'Hyper']

LinQ2 = [7.68, 11.3, 113.2, 0.25]
SJQ2 = [0.22, 4.5, 0.56, 0.3]

LinQ15 = [0.08, 12.57, 0.28, 0.19]
SJQ15 = [0.2, 24.5, 0.54, 0.16]

LinQ17 = [114, 143.2, 2.7, 3]
SJQ17 = [0.22, 1.27, 0.93, 0.03]

def compare_times(SJtimes, Lintimes, titles):
    num_dbms = len(DBMS)
    bar_width = 0.35

    plt.rcParams['font.size'] = 14
    x_positions = np.arange(len(DBMS))

    fig, axs = plt.subplots(1, 3, figsize=(15, 4))  # Create a figure with 3 subplots

    # Iterate over each subplot and plot the data
    for i, ax in enumerate(axs):
        # Plot the Lin values
        rects1 = ax.bar(x_positions - bar_width / 2, Lintimes[i], bar_width, label=r'$T_{lin}$', log=True, color='blue')
        # Plot the SJ values next to the Lin values
        rects2 = ax.bar(x_positions + bar_width / 2, SJtimes[i], bar_width, label=r'$T_{sj}$', log=True, color='green')
        # Set the x-axis ticks and labels
        ax.set_xticks(x_positions)
        ax.set_xticklabels(DBMS)
        ax.set_yticks(np.arange(0.01, 1000))
        ax.set_yscale('log')
        # Set the axis labels and title
        if i == 0:
            ax.set_ylabel(r'$T$ [ms]')
        ax.set_title(titles[i])
        ax.legend()

    plt.tight_layout()
    # Save the plot
    plt.savefig('tpch.pdf', format='pdf')
    plt.show()

compare_times([SJQ2, SJQ15, SJQ17], [LinQ2, LinQ15, LinQ17], ['Q2', 'Q15', 'Q17'])
