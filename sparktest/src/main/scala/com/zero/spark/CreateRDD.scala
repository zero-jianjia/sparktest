package com.zero.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Inuyasha on 16.04.03.
  */
object CreateRDD {
    def main(args: Array[String]) {
        createdByScalaCollection
//        createdByHDFS
    }

    def createdByScalaCollection: Unit ={
        val conf = new SparkConf().setAppName("CreateRDD").setMaster("local")
        val sparkContext = new SparkContext(conf)

        val collection = sparkContext.parallelize(1 to 100)//根据集合创建了ParallelCollectionRDD
        println(collection.count())
        
    }

    def createdByHDFS: Unit ={
        val conf = new SparkConf().setAppName("CreateRDD").setMaster("local")
        val sparkContext = new SparkContext(conf)

        val hdfsRDD = sparkContext.textFile("")//创建了HadoopRDD
        println(hdfsRDD.count())
    }

    def createdByOtherRDD: Unit ={
        val conf = new SparkConf().setAppName("CreateRDD").setMaster("local")
        val sparkContext = new SparkContext(conf)

        val otherRDD = sparkContext.parallelize(1 to 100)//根据集合创建了ParallelCollectionRDD

        val  newRDD = otherRDD.map(_ * 2)
    }
}
