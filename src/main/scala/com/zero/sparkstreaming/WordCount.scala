package com.zero.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Inuyasha on 16.09.12.
  */
object WordCount {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        
        //设置batchDuration时间间隔来控制Job生成的频率并且创建Spark Streaming执行的入口
        val ssc = new StreamingContext(conf, Seconds(5)) //5秒间隔
        val lines = ssc.socketTextStream(
            "127.0.0.1",
            6666,
            StorageLevel.MEMORY_AND_DISK_SER) // 服务器地址，端口，序列化方案
        val words = lines.flatMap(_.split(","))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()
        
        
        /**
          * 在StreamingContext调用start方法的内部其实是会启动JobScheduler的Start方法，进行消息循环，在JobScheduler
          * 的start内部会构造JobGenerator和ReceiverTacker，并且调用JobGenerator和ReceiverTacker的start方法：
          * 1，JobGenerator启动后会不断的根据batchDuration生成一个个的Job
          * 2，ReceiverTracker启动后首先在Spark Cluster中启动Receiver（其实是在Executor中先启动ReceiverSupervisor），在Receiver收到
          * 数据后会通过ReceiverSupervisor存储到Executor并且把数据的Metadata信息发送给Driver中的ReceiverTracker，在ReceiverTracker
          * 内部会通过ReceivedBlockTracker来管理接收到的元数据信息
          * 每个BatchInterval会产生一个具体的Job，其实这里的Job不是Spark Core中所指的Job，它只是基于DStreamGraph而生成的RDD
          * 的DAG而已，从Java角度讲，相当于Runnable接口实例，此时要想运行Job需要提交给JobScheduler，在JobScheduler中通过线程池的方式找到一个
          * 单独的线程来提交Job到集群运行（其实是在线程中基于RDD的Action触发真正的作业的运行），为什么使用线程池呢？
          * 1，作业不断生成，所以为了提升效率，我们需要线程池；这和在Executor中通过线程池执行Task有异曲同工之妙；
          * 2，有可能设置了Job的FAIR公平调度的方式，这个时候也需要多线程的支持；
          *
          */
        //真正的调度开始
        ssc.start()
        ssc.awaitTermination()
    }
}

