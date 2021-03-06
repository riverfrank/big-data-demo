package com.river.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author riverfan
 */
public class WordCount {
    public static void main(String[] args) {
        System.out.println("hello");

        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String worldcount = "/Users/riverfan/mytest/spark/hello.txt";
        wordCount2(sc,worldcount);

    }

    public static void wordCount2(JavaSparkContext sc, String filePaht) {
        JavaPairRDD<String, Integer> rdd1 = sc.textFile(filePaht)
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .filter(t -> StringUtils.isNoneBlank(t._1) && !StringUtils.equals(t._1, ","))
                .reduceByKey((v1, v2) -> (v1 + v2));
        rdd1.collect().forEach(t -> System.out.println(t + "  " + Thread.currentThread()));

    }
}
