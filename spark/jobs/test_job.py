from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
rdd = sc.parallelize(range(1000))
count = rdd.count()
print("Number of elements:", count)
sc.stop()