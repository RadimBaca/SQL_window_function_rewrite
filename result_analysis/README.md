# Result analysis (python)

The output produced by the microbenchmark can be analyzed using python scripts in the `result_analysis` directory. 
The directory currently contains result text files for the microbenchmarks used in the paper. 
For example to get result graphs for the aggrewgate window function microbenchmark run the following command:

```shell
python agg_analysis.py
```
If you want to analyse your own results then you need to create a text file with the same name as it is currently in the directory.
