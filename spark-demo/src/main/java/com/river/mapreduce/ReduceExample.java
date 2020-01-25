package com.river.mapreduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author riverfan
 */
public class ReduceExample {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 计算文本长度
        String filePath = "/Users/riverfan/mytest/spark/hello.txt";
        Integer reduce = sc.textFile(filePath).map(t -> t.length())
                .reduce((x, y) -> (x + y));
        System.out.println(reduce);

    }
}
