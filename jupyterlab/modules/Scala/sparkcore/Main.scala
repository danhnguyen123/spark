package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import Rdd_by_parallelize.rdd_by_parallelize
import Rdd_by_external_dataset.rdd_by_external_dataset
import Print_element.print_element
import Persist_cache.persist_cache
import Passing_function.passing_function
import Transformations.transformation
import Actions.action
import Accumulator.accumulator
import Broadcast.broadcast

object Main {
  def main(args: Array[String]) {
    // Create Spark context
    val conf = new SparkConf()
      .setAppName("Spark score")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    rdd_by_parallelize(sc)
//    rdd_by_external_dataset(sc)
//    print_element(sc)
//    persist_cache(sc)
//    passing_function(sc)
//    transformation(sc)
//    action(sc)
//    accumulator(sc)
//    broadcast(sc)
  }
}
