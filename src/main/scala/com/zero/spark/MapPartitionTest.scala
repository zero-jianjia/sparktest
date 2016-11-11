package com.zero.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianjia1 on 16/11/10.
  */
object MapPartitionTest {
    val conf = new SparkConf().setAppName("MapPartitionTest").setMaster("local")
    val sc = new SparkContext(conf)

    def main(args: Array[String]) {

    }

    def f1: Unit = {
        var rdd1 = sc.makeRDD(1 to 5, 2)
        var rdd3 = rdd1.mapPartitions { x => {
            var result = List[Int]()
            var i = 0
            while (x.hasNext) {
                i += x.next()
            }
            result.::(i).iterator
        }
        }
    }
}
