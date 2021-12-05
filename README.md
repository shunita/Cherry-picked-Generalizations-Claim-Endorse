# Cherry-picked-Generalizations

Args:
python SE.py arg1:dataset name arg2:out putfile name  arg3:algorithm(Naive, Cube, Optimized, Counter, Alternative)
Other parameters like which dataset, which experiment group are written in the SE.py file.
The Cube algorithm requires to store the datasets to Postgres database first and then run the algorithm.

How to run the code Example:
python SE.py datasets/so.csv result/so_result.txt Optimized
