import argparse
import re
import matplotlib.pyplot as plt

# Read data from text file
# data_file = "rank_postgre_maxdop1.txt"  # Update with the actual path to your data file
# Create command-line argument parser
parser = argparse.ArgumentParser(description='Create x,y graphs from data file')
parser.add_argument('data_files', nargs='+', type=str, help='Path to the data file')
args = parser.parse_args()

# Initialize variables

all_series_data = []
legend = []
# Read the data files
for data_file in args.data_files:
    series = []
    series_data = []
    with open(data_file, "r") as file:
        for line in file:
            line = line.strip()
            if line.startswith("legend:"):
                legend.append(line.split(":")[1].strip())
            if re.match(r'^\d+,\d+,\d+$', line):
                values = line.split(",")
                groups = int(values[0])
                sql1 = int(values[1])
                sql2 = int(values[2])
                series_data.append((groups, sql1, sql2))
            else:
                # if series_data is not empty, add it to the series
                if series_data:
                    series.append(series_data)
                    series_data = []
    if series_data:
        series.append(series_data)
    all_series_data.append(series)

# if len(legend) != len(all_series_data[0]):
#     print("Error: Number of legends does not match number of series")
#     exit(1)

# Calculate the number of rows and columns in the grid
num_plots = len(all_series_data[0])
num_rows = (num_plots + 2) // 3  # Round up to the nearest integer
num_cols = min(num_plots, 3) + 1

# Create the grid of plots
# fig, axes = plt.subplots(num_rows, num_cols)
fig, axes = plt.subplots(num_rows, num_cols, width_ratios=[1, 4, 4, 4], figsize=(15, 20))

# Flatten the axes array if it's not a 2D grid
if num_plots == 1:
    axes = [axes]

titles = ['=1', '=N', '<=N']
labels = ['No index', 'I(A)', 'I(B)', 'I(A), I(B)', 'I(A, B)', 'I(B,A)']

# Plotting the data
axes_without_leftmost_column = axes[:, 1:].flatten()
for idx, (ax) in enumerate(axes_without_leftmost_column):
    # iterate over all series and legends
    for series, label in zip(all_series_data, legend):
    # for series, label in (all_series_data, legend):
        series_data = series[idx]
        x = [d[0] for d in series_data]
        y = [d[1] / d[2] for d in series_data]
        # Plot on the current axis
        ax.plot(x, y, marker='o', label=label)

    ax.set_xscale('log')
    ax.set_yscale('log')
    if idx % 3 == 0:
        ax.set_ylabel('Relative speedup')
    if idx >= num_plots - 3:
        ax.set_xlabel('Number of groups')
    if idx < 3:
        ax.set_title(titles[idx])
    ax.axhline(y=1, color="red", linestyle="--")
    ax.set_ylim(0.1, 10)

# Add labels on the left side of the grid
for i, label in enumerate(labels):
    axes[i, 0].axis('off')  # Turn off the leftmost subplot
    axes[i, 0].text(0, 0.5, label, transform=axes[i, 0].transAxes, va='center', ha='center')

# Create a legend outside the grid

handles, labels = axes[0, 1].get_legend_handles_labels()
fig.legend(handles, labels, loc='upper right')

# Adjust the layout to provide space for the legend
fig.tight_layout(rect=(0, 0, 0.95, 1))

# Adjust spacing between subplots
# plt.tight_layout()
# plt.subplots_adjust(hspace=1)

# Display the plots
plt.show()

