package com.zero.sparkstreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Inuyasha on 16.09.12.
  */
object WordCount2 {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        //使用广播变量定义黑名单，使用Broadcast广播黑名单到每个Executor中
        val broadcastSet = sc.broadcast(Set("hadoop", "spark"))
        //全局计数器，用于通知在线过滤了多少各黑名单
        val accumulator = sc.accumulator(0, "OnlineBlacklistCounter")
        
        //设置batchDuration时间间隔来控制Job生成的频率并且创建Spark Streaming执行的入口
        val ssc = new StreamingContext(sc, Seconds(5)) //5秒间隔
        val lines = ssc.socketTextStream(
                "127.0.0.1",
                6666,
                StorageLevel.MEMORY_AND_DISK_SER) // 服务器地址，端口，序列化方案
        val words = lines.flatMap(_.split(","))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
            .filter(e => {
                if (broadcastSet.value.contains(e._1)) {
                    accumulator.add(e._2)
                    false
                } else {
                    true
                }
            })
        wordCounts.print()
        System.out.println("******" + accumulator.value)
        //真正的调度开始
        ssc.start()
        ssc.awaitTermination()
    }
}

