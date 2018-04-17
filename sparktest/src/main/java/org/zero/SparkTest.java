package org.zero;

import org.apache.spark.SparkConf;

/**
 * Created by jianjia1 on 16/04/07.
 */
public class SparkTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkTest");
    }
}
