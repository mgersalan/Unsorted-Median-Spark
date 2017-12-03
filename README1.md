# Unsorted-Median-Spark

Spark implementation of some basic opeations such as finding minimum,maximum,mean and variance of a dataset.

Secondly, implementation of quick selection algorithm (kth   smallest) to find median of a large dataset without sorting it.

-Zepplin notebook is used with Python 3.7.

-Sample dataset is uploaded here but the program is tested with larger data sets as well.

#Part1

Some basic operations to calculate the minimum value, maximum value , mean and variance of the dataset. Before proceeding to the calculations,
to increase efficiency I first defined a function called ' cache_RDD ' where I started by creating an RDD from the input text file and 
transform it to a new RDD by mapping each line of the text file to float type. Then, I cached that RDD which will be used in the other computations.
I defined a function for each of the calculations where the input parameter passed as the cached RDD.
For each calculation, pre-built methods(actions) of RDD used to calculate the maximum,minimum,mean and the variance values.

#Part2

Calculating median of a large data set without sorting it.
