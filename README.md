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


Function Explanations:

## Part 1 (Basic calculations for large dataset)

Before proceeding to the calculations, to increase efficiency I first defined a function called 'cache_RDD' where I started by creating an RDD from the text file and transform it to a new RDD by mapping each line of the text file to float type. Then, I cached that RDD which will be used in the other computations.


I defined a function for each of the calculations where the input parameter passed as the cached RDD.
For each calculation, pre-built methods(actions) of RDD used to calculate the maximum,minimum,mean and the variance values.

The   values   found   are   below   :
The   average   of   data   set   is   50.0073930038 
The   minimum   of   data   set   is   2.307e-05 
The   maximum   of   data   set   is   99.9999985 
The   variance   of   data   set   is   833.02497623

## Part 2 (Finding median without sorting the big data set)

2 functions have defined. First one is,find_median and the other one is find_kth_element.
Quick Selection algorithm has been used.

Find_Median function is called first, this function first checks if the total number of elements in data set is odd or even. After determining that, iterative  find_kth_element  function is called by passing the initial cached RDD which contains all the values in the dataset and parameter k which represents the kth smallest value. In our case it(k) means the median. Since median is the value in the middle I defined k as (round(N/2) + 1 ) when the total number of elements in the RDD is odd. Otherwise, if the total number of elements in the RDD is even, I calculated 2 values in the middle by calling  find_kth_element twice with different values of parameter 'k'  passed to the function. After getting two middle values, I take the average of them to get the actual median of the even data set. So, when we are dealing with even dataset I defined k1 as (round(N/2) + 1 ) and k2 as (round(N/2) ) and called  find_kth_element for each of them. I get the 2 values in the middle then take the average of them to find our median value without doing sorting.

find_kth_element  function first called from inside the  find_median  function taking the whole RDD and k(definition of k is defined above) as the parameters. The function then starts with defining a pivot value randomly. We pick a random value from the RDD values by using takeSample() built-in function of RDD. When we randomly select this value and assign it as the pivot then we split the RDD into new 2 RDDs based on greater values than pivot and lesser values than pivot. Greater values are put into the rdd_greater and smaller values are put into the rdd_smaller by using filter feature of RDD. After that we have 2 RDDs. In order for the pivot value to be equal to median value, k should be equal to size of smaller RDD + 1. For example if we have 5 values and we split it based on smaller and greater values compared to pivot value, when k(index of median value) is equal to number of values smaller than (array[k] + 1) this means pivot number is in the middle and it's the median. We follow the same logic. If k is lesser than the number of values smaller than pivot(rdd_smaller) that means our pivot is not the median and the median must be among the smaller subset because the value count of smaller rdd subset is more than the greater subset. So, if that is the case we call the function again by passing the RDD parameter as the smaller rdd.

Otherwise if k is greater than the number of values in  smaller_rdd this means that greater RDD has more elements than smaller RDD and we need to search for the median inside the greater RDD(rdd_greater) so we call the  find_kth_element function again by passing the  greater RDD as the first argument this time. For the second argument k , we have a different case. Since k is not equal to (N/2 + 1) anymore based on the fact that smaller RDD has less elements than greater RDD. We set [k - (rdd_smaller.count() + 1] as the k parameter because this means there is at least [k - rdd_smaller.count() + 1] values before the median and we need to take them into account when finding the kth smallest value in each iteration. After these iterative processes when we find the value which satisfies our kth smallest condition(based on how we defined k) the function returns the value of the median value which is equal to our pivot. When I run my code to calculate the median I get the result below:
The median is 50.001105695
