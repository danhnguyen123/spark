package sparkcore

import org.apache.spark.SparkContext

object Actions {
  def action(sc: SparkContext): Unit = {
    val data = Array.range(0, 10)
    val distData = sc.parallelize(data, 2)

    // reduce
    val action_val = distData.reduce((x, y) => x + y)


//    // collect
//    val action_val = distData.collect()
//    action_val.foreach(x => println("===== value: " + x))


//    // count
//    val action_val = distData.count()


//    // first
//    val action_val = distData.first()


//    // take
//    val action_val = distData.take(5)
//    action_val.foreach(x => println("===== value: " + x))


//    // takeSample
//    val action_val = distData.takeSample(false, 5, 3)
//    action_val.foreach(x => println("===== value: " + x))


//    // saveAsTextFile
//    val action_val = distData.saveAsTextFile("./data/textfile")


//    // countByKey & countByValue
//    val data2 = Array(1, 1, 2, 4, 5, 6, 7, 7, 7)
//    val distData1 = sc.parallelize(data2)
//    val distData2 =distData1.map(x => (x, 1))
//    val action_val = distData2.countByKey()
////    val action_val = distData1.countByValue()


//    // foreach
//    distData.foreach(x => println("===== value: " + x))

    println("===== action_val: " + action_val)

  }
}
