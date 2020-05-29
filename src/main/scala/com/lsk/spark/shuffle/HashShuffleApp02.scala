package com.lsk.spark.shuffle

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark-core_2.11
 * spark 1.6.0
 * ls -lR|grep "^-"|wc -l
 * hash shuffle 6 M * N M:MapTask N"ReduceTask (Partition)
 * hash shuffle consolidateFiles 6
 * sort shuffle 4 ??? k * 2 (data file handle + index file handle)
 * k:core nums，如果一个executor上有k个core，那么executor同时可运行k个task
 * tungsten-sort shuffle 4 序列化
 * ctrl + shift + f
 *
 * http://sharkdtu.com/posts/spark-shuffle.html
 */
object HashShuffleApp02 {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.shuffle.manager","hash")
      .set("spark.shuffle.consolidateFiles","true")
    
    val sc = new SparkContext(sparkConf)
    
    val lines = sc.parallelize(Seq(
      "1","2","3","4","5","6","7","8","1","2"
    ),2)
    
    val rdd = lines.flatMap(_.split(",")).map((_,1))
  
    import org.apache.spark.SparkContext._
  
//    rdd.combineByKey()
  
    val reduceRDD = rdd.reduceByKey(_ + _, 3)
  
    reduceRDD.collect()
    
    Thread.sleep(2000000)
  }
  
}
