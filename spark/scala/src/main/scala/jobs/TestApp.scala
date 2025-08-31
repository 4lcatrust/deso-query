package jobs
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Simple App").getOrCreate()
    val cnt = spark.sparkContext.parallelize(0 until 1000).count()
    println(s"Number of elements: $cnt")
    spark.stop()
  }
}
