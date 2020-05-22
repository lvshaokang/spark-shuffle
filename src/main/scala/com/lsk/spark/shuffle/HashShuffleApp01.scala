package com.lsk.spark.shuffle

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark-core_2.11
 * spark 1.6.0
 * ls -lR|grep "^-"|wc -l
 *
 * ctrl + shift + f
 */
object HashShuffleApp01 {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.shuffle.manager","hash")
    
    val sc = new SparkContext(sparkConf)
    
    val lines = sc.parallelize(Seq(
      ("1,2,3,4,5"),
      ("1,2")
    ),2)
    
    val rdd = lines.flatMap(_.split(",")).map((_,1))
    
    rdd.reduce((x,y) => (x._1,x._2 + y._2))
  
    val groupByRDD = rdd.groupBy((x:(String,Int)) => {
      x._1
    }, 3)
  
    groupByRDD.foreach(println)
  
    groupByRDD.map(_._2).map(x => {
      val strings = x.toList.map(_._1).take(1)(0)
      val ints = x.toList.map(_._2).sum
      (strings,ints)
    }).collect()
    
    Thread.sleep(2000000)
  }
  
}
