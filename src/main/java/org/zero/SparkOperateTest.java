package org.zero;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Inuyasha on 16.08.07.
 */
public class SparkOperateTest {
    public static void main(String[] args) {
        final SparkConf sparkConf = new SparkConf().setAppName("OperateTest").setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        mapOperate(sc);
        sc.close();
    }

    public static void joinOperate(JavaSparkContext jsc) {
        JavaRDD<Tuple2<String, Integer>> rdd0 = jsc.parallelize(Arrays.asList(new Tuple2<String, Integer>("a", 5), 
                new Tuple2<String, Integer>("b", 5)));
        JavaRDD<Tuple2<String, Integer>> rdd1 = jsc.parallelize(Arrays.asList(new Tuple2<String, Integer>("a", 3), 
                new Tuple2<String, Integer>("b", 3),new Tuple2<String, Integer>("c", 10)));
        
    }
    
    public static void cogroupOperate(JavaSparkContext jsc) {
        JavaRDD<Tuple2<String, Integer>> rdd0 = jsc.parallelize(Arrays.asList(new Tuple2<String, Integer>("a", 5),
                new Tuple2<String, Integer>("b", 5)));
        JavaRDD<Tuple2<String, Integer>> rdd1 = jsc.parallelize(Arrays.asList(new Tuple2<String, Integer>("a", 3),
                new Tuple2<String, Integer>("b", 3),new Tuple2<String, Integer>("c", 10)));

        
        
    }


    public static void mapOperate(JavaSparkContext jsc) {
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("spark", "storm"));
        JavaPairRDD<Integer, String> pairRdd = rdd.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(s.length(), s);
            }
        });
        pairRdd.collect().forEach(System.out:: println);
    }

}
