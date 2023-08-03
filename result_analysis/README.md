# Result analysis (python)

The output produced by the microbenchmark can be analyzed using python scripts in the `result_analysis` directory. 
The directory currently contains result text filestoring the microbenchmarks results run as a part of the article. 
For example to get result graphs for the aggregate window function microbenchmark run the following command:

```shell
python agg_analysis.py
```
If you want to analyse your own results then you need to create a text file with the same name as it is currently in the directory.
All the parameters of the analysis are hardcoded in the python files.
There are no parameter options.
