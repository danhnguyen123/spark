package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Persist_cache {
  def persist_cache(sc: SparkContext): Unit = {
    val lines = sc.textFile("./data/names.txt")
    //    val lineLengths = lines.map(x => x.length())

    // storage level: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY_SER, MEMORY_ONLY_2
    val lineLengths = lines.map(x => x.length()).persist(StorageLevel.MEMORY_ONLY_SER )

    ////    a shorthand for using the default storage level, which is StorageLevel.MEMORY_ONLY
    //    val lineLengths = lines.map(x => x.length()).cache()

    val totalLength = lineLengths.reduce((x, y) => x + y)
    val maxLength = lineLengths.reduce((x, y) => x max y)
    println("===== totalLength: " + totalLength)
    println("===== maxLength: " + maxLength)
  }
}
