package com.zero.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Inuyasha on 16.09.25.
  */
object WindowsTest {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Windows").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val lines = ssc.socketTextStream(
            "127.0.0.1",
            6666,
            StorageLevel.MEMORY_AND_DISK_SER) // 服务器地址，端口，序列化方案
        val words = lines.flatMap(_.split(","))
        val wordCounts = words.map(x => (x, 1))
//            .window(Seconds(15),Seconds(3)).reduceByKey(_ + _)
            .reduceByKeyAndWindow(_ + _, _ - _, Seconds(3))
        wordCounts.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
}
