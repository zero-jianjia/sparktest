package com.zero

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path

/**
  * Created by Inuyasha on 16.06.22.
  */
object WordCount {
    def main(args: Array[String]) {
        /**
          * 第一步：创建spark的配置对象SparkConf，设置Spark程序运行时的配置信息
          * 例如通过setMaster来设置程序要链接的Spark集群的Master的URL，如果设置
          * 为local，则代表Spark程序在本地运行
          */
        val conf = new SparkConf()      //创建SparkConf对象。因为是全局唯一的，所以使用new，不用工厂方法模式。
        conf.setAppName("word count")    //设置应用程序的名称，在程序运行的监控界面可以看到名称
        conf.setMaster("local[2]")  //此时程序在本地运行，不需要安装spark集群。
        /**
          * 第二步：创建SparkContext对象，
          * SparkContext是Spark程序所有功能的唯一入口，无论是采用scala/java/Python/R等都必须有一个SParkContext，而且默认都只有一个。
          * SparkContext核心作用：初始化应用程序运行时所需要的核心组件，包括DAGScheduler,TaskScheduler,Scheduler Backend,
          * 同时还会负责Spark程序往Master注册程序等。SparkContext是整个Spark应用程序中最为重要的一个对象，
          *
          */
        val sc = new SparkContext(conf)    //通过创建SparkContext对象，通过传入SparkConf实例来定制SPark地的具体参数和配置信息。
        /**
          * 第三步：根据具体的数据来源（/HBase/Local FS/DB/S3等）通过SparkContext创建RDD，
          * RDD创建有三种基本方式：1.根据外部数据来源（如HDFS），2.根据Scala集合，3.由其他RDD操作产生
          * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴，
          */
        val lines = sc.textFile("files\\test.log", 2)
        
        /**
          * 第4步：对初始RDD进行Transformation级别的处理。例如map/filter等高阶函数等的编程
          * 来进行具体的数据计算。第4.1步：将每一行的字符串拆分成单个的单词。
          */
        val words = lines.flatMap { line => line.split(",") }   //对每一行的字符串进行单词拆分，map每次循环一行，将每一行的小集合通过flat合并成一个大集合
        /**
          * 第4.2步，在单词拆分的基础上对每个单词实例 进行计数为1，也就是word => (word,1)
          */
        val pairs = words.map { word => (word, 1) }
        println(pairs.toDebugString)
        
        /**
          * 第4.3步，在每个单词实例计数为1的基础上，统计每个单词在文件中出现的总次数。
          */
        val wordCounts = pairs.reduceByKey(_ + _)  //对相同的Key,进行Value的累计（包括Local和Reduce级别同时 Reduce）
        wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
        sc.stop()    //把上下文去掉，释放资源
    }
}
