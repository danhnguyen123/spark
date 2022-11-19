package sparkcore

import org.apache.spark.SparkContext

object Passing_function {
  val foo_1 = (x:Int) => x * 2

  def foo_2(x: Int): Int = x * 3

  def passing_function(sc: SparkContext): Unit = {
    val data = Array.range(0, 5)
    val distData = sc.parallelize(data)

//        val map_rdd = distData.map(foo_1)
//        val map_rdd = distData.map(foo_2)
//    val map_rdd = distData.map(x => x * 4)
    val map_rdd = distData.map(x => {
      val y = 5
      x * y
    })
    map_rdd.foreach(x => println("===== value: " + x))
  }
}
