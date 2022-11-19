package sparkcore

import org.apache.spark.SparkContext

object Transformations {
  def transformation(sc: SparkContext): Unit = {
    val data = Array.range(0, 10)
    val distData = sc.parallelize(data, 2)

    // map
    val transform_value = distData.map(s => s + 5)


//    // filter
//    val transform_value = distData.filter(x => x > 2)


//    // flatMap
//    val data = Array("hello world", "apache spark", "my heart")
//    val distData = sc.parallelize(data)
//    val transform_value = distData.flatMap(s => s.split(" "))


//    // mapPartitions
//    def foo(iterator: Iterator[Int]): Iterator[Any] = {
//      val r = new scala.util.Random
//      val random_val = r.nextInt(5)
//      val do_something_res = iterator.map(x => {
//        println("Partition "+ random_val + ": "+x)
//        x
//      })
//      val res = for (x <- do_something_res) yield x
//      res
//    }
//    val transform_value = distData.mapPartitions(foo).collect()


//    // mapPartitionsWithIndex
//    def foo(idx: Int, iterator: Iterator[Int]): Iterator[Any] = {
//      val do_something_res = iterator.map(x => {
//        println("Partition "+ idx + ": "+x)
//        x
//      })
//      val res = for (x <- do_something_res) yield x
//      res
//    }
//    val transform_value = distData.mapPartitionsWithIndex(foo).collect()


//    // sample
//    val transform_value = distData.sample(false, 0.5, 3)


//    // union
//    val data2 = Array.range(10, 20)
//    val distData2 = sc.parallelize(data2)
//    val transform_value = distData.union(distData2)


//    // intersection
//    val data2 = Array.range(5, 15)
//    val distData2 = sc.parallelize(data2)
//    val transform_value = distData.intersection(distData2)


//    // distinct
//    val data2 = Array(1, 1, 2, 4, 5, 6, 7, 7, 7)
//    val distData2 = sc.parallelize(data2)
//    val transform_value = distData2.distinct()


//    // groupByKey
//    val data2 = Array(1, 1, 2, 4, 5, 6, 7, 7, 7)
//    val distData2 = sc.parallelize(data2).map(x => (x, 1))
//    val transform_value = distData2.groupByKey()


//    // reduceByKey & sortByKey
//    val data2 = Array(1, 1, 2, 4, 5, 6, 7, 7, 7)
//    val distData2 = sc.parallelize(data2).map(x => (x, 1))
//    val transform_value = distData2.reduceByKey((x, y) => x+y).sortByKey().collect()
////    val transform_value = distData2.reduceByKey(_+_)


//    // repartition
//    println("old number partition: " + distData.getNumPartitions)
//    val repartition_rdd = distData.repartition(4)
//    println("new number partition: " + repartition_rdd.getNumPartitions)


    transform_value.foreach(x => println("===== value: " + x))

  }
}
