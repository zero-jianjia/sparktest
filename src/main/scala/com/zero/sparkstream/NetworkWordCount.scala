package com.zero.sparkstream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Inuyasha on 16.09.12.
  */
object NetworkWordCount {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5)) //5秒间隔  
        val lines = ssc.socketTextStream(
                "127.0.0.1",
                6666,
                StorageLevel.MEMORY_AND_DISK_SER) // 服务器地址，端口，序列化方案
        println(lines)
        val words = lines.flatMap(_.split(","))
        val wordCounts = words.map(x => (x, 1))
            .reduceByKey(_ + _)
//            .countByValueAndWindow()/
        
        wordCounts.print()
        ssc.start()
        ssc.awaitTermination()
    }
}

