package com.zero

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Inuyasha on 16.04.16.
  */
object FileWordCount {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]") //这里指在本地运行，2个线程，一个监听，一个处理数据  
        // Create the context  
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        //        val lines = ssc.textFileStream("C:\\Users\\Inuyasha\\git\\sparktest\\files")
        val lines = ssc.fileStream("C:\\Users\\Inuyasha\\git\\sparktest\\files")
        
        
        lines.count().print()
//        val words = lines.flatMap(_.split(" "))
//        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//        wordCounts.print()
//        val stateDstream = wordCounts.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
//            val currentCount = values.foldLeft(0)(_ + _)
//            val previousCount = state.getOrElse(0)
//            Some(currentCount + previousCount)
//        })
//        stateDstream.print()
        ssc.start()
        ssc.awaitTermination();
    }
}
