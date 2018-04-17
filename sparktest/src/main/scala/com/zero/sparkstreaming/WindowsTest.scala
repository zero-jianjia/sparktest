package com.zero.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Inuyasha on 16.09.25.
  */
object WindowsTest {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("WindowsTest").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(1))//(conf: SparkConf, batchDuration: Duration)
        
        val lines = ssc.socketTextStream(
            "10.210.228.92",
            6666,
            StorageLevel.MEMORY_AND_DISK_SER) // 服务器地址，端口，序列化方案
        val words = lines.flatMap(_.split(","))
        
        //windows操作  
        val wordCounts = words.map(x => (x, 1))
            .reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(3), Seconds(5))
        // 第二个参数是 窗口时间间隔，是 监听间隔batchDuration 的倍数
        // 第三个参数是 滑动时间间隔，也必须是 监听间隔batchDuration 的倍数
        //那么这里的作用是，每隔3秒钟，对前5秒的数据，进行一次处理，这里的处理就是 word count。  
        wordCounts.print()
        
        //val wordCounts = words.map(x => (x , 1))
        //  .reduceByKeyAndWindow(_+_, _-_,Seconds(args(2).toInt), Seconds(args(3).toInt))  
        //这个是优化方法， 即加上上一次的结果，减去上一次存在又不在这一次的数据块的部分。
                
        //val sortedWordCount = wordCounts.map{case(char, count) => (count, char)}.transform{_.sortByKey(false)}
        // .map{case(char, count) => (count, char)}
        //这个地方用transform，而不直接使用sortByKey，是因为sortByKey只能对单个RDD进行操作，而wordCounts是一连串的RDD，所以这里需要用transform来对这一连串的RDD
        // 进行sortByKey操作。
        //sortedWordCount.print()
                
                
        ssc.start()
        ssc.awaitTermination()
    }
}
