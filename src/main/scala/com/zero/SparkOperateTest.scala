package com.zero

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianjia1 on 16/04/07.
  */
object SparkOperateTest {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Straming")
    val sparkContext = new SparkContext(sparkConf)

    def main(args: Array[String]): Unit = {
        //        joinOperate
        //        cogroupOperate
        //        leftOuterJoinOperate
        //        fullOuterJoinOperate
        //        cartesianOperate
        filterOperate

        TimeUnit.MINUTES.sleep(5)
    }


    def filterOperate: Unit = {
        val a = "flume spark hadoop zookeeper"
        val b = "flume spark"
        val c = "hadoop zookeeper"
        val d = "flume zookeeper spark"
        val parallelCollectionRDD = sparkContext.parallelize(Seq(a, b, c, d))
        println(parallelCollectionRDD.count()) //RDD有四个元素
        println(parallelCollectionRDD.toDebugString)

        val mapPartitionsRDD1: RDD[String] = parallelCollectionRDD.filter(s => s.contains("spark"))
        println(mapPartitionsRDD1.toDebugString)

        val mapPartitionsRDD2 = mapPartitionsRDD1.flatMap(s => s.split(" "))
        println(mapPartitionsRDD2.toDebugString)

        mapPartitionsRDD2.foreach(println)
        //        flume
        //        spark
        //        hadoop
        //        zookeeper
        //        flume
        //        spark
        //        flume
        //        zookeeper
        //        spark

        mapPartitionsRDD2.foreachPartition(s => println(s.mkString("{", ",", "}")))
        //       {flume,spark,hadoop,zookeeper,flume,spark,flume,zookeeper,spark}

        mapPartitionsRDD2.map(s => (s, 1)).reduceByKey(_+_).collect().foreach(println)
    }


    def cartesianOperate: Unit = {
        val a = sparkContext.parallelize(Array(("123", 4.0), ("456", 9.0), ("789", 9.0)))
        val b = sparkContext.parallelize(Array(("123", 8.0), ("789", 10)))

        val c = a.cartesian(b)
        c.foreach(println)
        //笛卡尔积
        //        ((123,4.0),(123,8.0))
        //        ((123,4.0),(789,10))
        //        ((456,9.0),(123,8.0))
        //        ((456,9.0),(789,10))
        //        ((789,9.0),(123,8.0))
        //        ((789,9.0),(789,10))
    }

    def fullOuterJoinOperate: Unit = {
        val a = sparkContext.parallelize(Array(("123", 4.0), ("456", 9.0), ("789", 9.0)))
        val b = sparkContext.parallelize(Array(("123", 8.0), ("789", 10)))

        val c = a.fullOuterJoin(b)
        c.foreach(println)
        //        (456,(Some(9.0),None))
        //        (123,(Some(4.0),Some(8.0)))
        //        (789,(Some(9.0),Some(10)))
    }

    def leftOuterJoinOperate: Unit = {
        val a = sparkContext.parallelize(Array(("123", 4.0), ("456", 9.0), ("789", 9.0)))
        val b = sparkContext.parallelize(Array(("123", 8.0), ("789", 10)))

        val c = a.leftOuterJoin(b)
        c.foreach(println)
        //        (456,(9.0,None))
        //        (123,(4.0,Some(8.0)))
        //        (789,(9.0,Some(10)))
    }

    def cogroupOperate: Unit = {
        //将多个RDD中同一个Key对应的Value组合到一起。
        val a = sparkContext.parallelize(Array(("123", 4.0), ("456", 9.0), ("789", 9.0)))
        val b = sparkContext.parallelize(Array(("123", 8.0), ("789", 10)))

        val c = a.cogroup(b)
        c.foreach(println)
        //        (456,(CompactBuffer(9.0),CompactBuffer()))
        //        (123,(CompactBuffer(4.0),CompactBuffer(8.0)))
        //        (789,(CompactBuffer(9.0),CompactBuffer(10)))
    }

    def joinOperate: Unit = {
        //只取相同的key，value组合
        val a = sparkContext.parallelize(Array(("123", 4.0), ("456", 9.0), ("789", 9.0)))
        val b = sparkContext.parallelize(Array(("123", 8.0), ("789", 10)))

        val c = a.join(b)
        c.foreach(println)
        //        (123,(4.0,8.0))
        //        (789,(9.0,10))

    }

    def getParallelCollectionRDD = {
        val a = "flume spark hadoop zookeeper"
        val b = "flume spark"
        val c = "hadoop zookeeper"
        val d = "flume zookeeper"
        sparkContext.parallelize(Seq(a, b, c, d))
    }
}
