from pyspark import SparkContext

if __name__ == "__main__":
    # Initialize Spark context
    sc = SparkContext("local", "WordCountApp")

    # Example input data
    data = [
        "hello spark",
        "hello world",
        "spark makes big data simple",
        "hello hello spark"
    ]

    # Create RDD
    rdd = sc.parallelize(data)

    # Split each line into words
    words = rdd.flatMap(lambda line: line.split(" "))

    # Count occurrences of each word
    word_counts = words.map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

    # Collect results back to the driver
    output = word_counts.collect()

    # Print results
    for word, count in output:
        print(f"{word}: {count}")

    # Stop Spark context
    sc.stop()
