# Result analysis (python)

The output produced by the microbenchmark can be analyzed using Python scripts in the `result_analysis` directory. 
The required Python libraries are listed in `requirements.txt`.
The directory currently contains `*.txt` files with our microbenchmarks results. These results were presented in the article. 
For example, to get result graphs for the aggregate window function microbenchmark run the following command:

```shell
pip install -r requirements.txt
python agg_analysis.py
```
The input file names are hardcoded in the Python files.
If you want to analyse your own results then you need to rewrite it there.
