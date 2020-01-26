package com.river.mapreduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author riverfan
 */
public class ReturnExample {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15,16);
        JavaRDD<Integer> distData = sc.parallelize(data);
        distData.collect().forEach(t-> System.out.println(t));

    }
}
