package com.river.mapreduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author riverfan
 */
public class ReduceByKeyExample {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 计算文本长度
        String filePath = "/Users/riverfan/mytest/spark/hello.txt";
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.textFile(filePath)
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((x, y) -> x + y);
        stringIntegerJavaPairRDD.collect().forEach(t-> System.out.println(t._1 + ":" + t._2));
    }
}
