package com.zero

import java.util.concurrent.TimeUnit

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianjia1 on 16/04/07.
  */
object Test {
    private val pvLogLength: Int = 14
    private val pvLineitemIdLoc: Int = 6
    private val pvAccountIdLoc: Int = 10
    private val pvGroupIdLoc: Int = 12
    private val clkLogLength: Int = 15
    private val clkLineitemIdLoc: Int = 4
    private val clkAccountIdLoc: Int = 5
    private val clkGroupIdLoc: Int = 6
    private val billLogLength: Int = 10
    private val billLineitemIdLoc: Int = 1
    private val billAccountIdLoc: Int = 3
    private val billGroupIdLoc: Int = 2
    private val billCostLoc: Int = 4
    private val billCostTypeLoc: Int = 5
    private val billTimeLoc: Int = 6

    def main(args: Array[String]) {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Straming")
        val sparkContext = new SparkContext(sparkConf)
//        val streamContext = new StreamingContext(sparkContext, Durations.seconds(5))

        val pvPairDStream = sparkContext.parallelize(getPVSeq)
        val clickPairDStream = sparkContext.parallelize(getClickSeq)
        val billPairDStream = sparkContext.parallelize(getBillSeq)

//        val pvDStream = pvPairDStream.filter(_.split("\t").length > 0)
//        val clickDStream = clickPairDStream.filter(_.split("\t").length > 0)
//        val billDStream = billPairDStream.filter(_.split("\t").length > 0)

//        val pvItems = pvDStream.flatMap(x => {
//            val items: Array[String] = x.split("\t")
//            val pvTime: String = items(0).substring(0, 13)
//            val lineitemId: String = items(pvLineitemIdLoc)
//            val accountId: String = items(pvAccountIdLoc)
//            val groupId: String = items(pvGroupIdLoc)
//            Array(pvTime + "#" + lineitemId + "#" + accountId + "#" + groupId)
//        })
        //        pvItems.foreach(println)
        //        println()

//        val clickItems = clickDStream.flatMap(x => {
//            val items: Array[String] = x.split("\t")
//            val clickTime: String = items(0).substring(0, 13)
//            val lineitemId: String = items(clkLineitemIdLoc)
//            val accountId: String = items(clkAccountIdLoc)
//            val groupId: String = items(clkGroupIdLoc)
//            Array(clickTime + "#" + lineitemId + "#" + accountId + "#" + groupId)
//        })
//        //        clickItems.foreach(println)
//        //        println()
//
//        val billItems = billDStream.flatMap(x => {
//            val items: Array[String] = x.split("\t")
//            val billTime: String = items(billTimeLoc).substring(0, 13)
//            val lineitemId: String = items(billLineitemIdLoc)
//            val accountId: String = items(billAccountIdLoc)
//            val groupId: String = items(billGroupIdLoc)
//            val cost: String = items(billCostLoc)
//            Array(billTime + "#" + lineitemId + "#" + accountId + "#" + groupId + "---" + cost)
//        })
//        //        billItems.foreach(println)
//        //        println()
//
//        val pvItemCounts = pvItems.map(x => (x, 1)).reduceByKey(_ + _)
//        //        pvItemCounts.foreach(println)
//        //        println()
//
//
//
//        val clickItemCounts = clickItems.map(x => (x, 1)).reduceByKey(_ + _)
//        //        clickItemCounts.foreach(println)
//        //        println()
//
//        val billItemCounts = billItems.map(x => (x.split("---")(0), x.split("---")(1).toLong)).reduceByKey(_ + _)
//        //        billItemCounts.foreach(println)
//        //        println()
//        val fullOutJoin = pvItemCounts.fullOuterJoin(clickItemCounts).fullOuterJoin(billItemCounts)
//        fullOutJoin.foreach(println)
//        println()
//
//        // (Option[(Option[Int], Option[Int])], Option[Long])
//        fullOutJoin.mapValues(v1 => {
//            println(v1 + "-------------")
//            var first = 0
//            var second = 0
//            var third = 0L
//
//            v1._1 match {
//                case Some(a) => {
//                    a._1 match {
//                        case Some(b) => first = b
//                        case None =>
//                    }
//                    a._2 match {
//                        case Some(b) => second = b
//                        case None =>
//                    }
//                }
//                case None =>
//            }
//            v1._2 match {
//                case Some(a) => third = a
//                case None =>
//            }
//            (first, second, third)
//        }).foreach(println)
////        streamContext.start()
//        TimeUnit.MINUTES.sleep(5)
    }

    def getPVSeq = {
//        Seq(a1, a2, a3)
    }

    def getClickSeq = {
//        Seq(a1, a2, a3)
    }

    def getBillSeq = {
//        Seq(a1, a2, a3)
    }
}
