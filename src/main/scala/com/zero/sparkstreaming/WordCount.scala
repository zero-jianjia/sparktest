package com.zero.sparkstreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

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
        val wordCounts = words.map(x => (x, 1))
            .reduceByKey(_ + _)
        wordCounts.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
}

