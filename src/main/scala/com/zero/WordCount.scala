package com.zero

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Inuyasha on 16.06.22.
  */
object WordCount {
    def main(args: Array[String]) {
        /**
          * 创建spark的配置对象SparkConf，设置Spark程序运行时的配置信息。
          */
        val conf = new SparkConf()      //创建SparkConf对象，这是全局唯一的。
        conf.setAppName("Word Count")    //设置应用程序的名称，在程序运行的监控界面可以看到名称
        conf.setMaster("local[2]")   //此时程序在本地运行，2个线程，一个监听，一个处理数据
        
        /**
          * 根据SparkConf创建SparkContext对象。
          * SparkContext是Spark程序所有功能的唯一入口，
          * SparkContext是整个Spark应用程序中最为重要的一个对象。
          * SparkContext核心作用：
          *     初始化应用程序运行时所需要的核心组件，包括DAGScheduler,TaskScheduler,Scheduler Backend,
          *     同时还会负责Spark程序往Master注册程序等。
          */
        val sc = new SparkContext(conf)  //通过创建SparkContext对象，通过传入SparkConf实例来定制SPark地的具体参数和配置信息。
       
        /**
          * 据具体的数据来源（/HBase/Local FS/DB/S3等）通过SparkContext创建RDD。
          * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴。
          */
        val lines = sc.textFile("files\\test.txt", 2)
        
        //对初始RDD进行Transformation级别的处理。
        //对每一行的字符串进行单词拆分，map每次循环一行，将每一行的小集合通过flat合并成一个大集合
        val words = lines.flatMap { line => line.split(",") }
        //在单词拆分的基础上对每个单词实例 进行计数为1，也就是word => (word,1)
        val pairs = words.map { word => (word, 1) }
        //在每个单词实例计数为1的基础上，统计每个单词在文件中出现的总次数。
        val wordCounts = pairs.reduceByKey(_ + _)
    
        //对初始RDD进行Action级别的处理。
        wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
        
        sc.stop()    //释放资源
    }
}
