package com.river.globleValue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @author riverfan
 */
public class BroadcastExample {
    private static volatile Broadcast<List<String>> broadcastList = null;
    private static volatile List<String> broadcastList2 = Arrays.asList("river","frank","spark");

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        broadcastList = sc.broadcast(Arrays.asList("Hadoop","Mahout","Hive"));


        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15,16);
        JavaRDD<Integer> distData = sc.parallelize(data);
        distData.map(t->{
            System.out.println(Thread.currentThread().getName() + broadcastList.value());
            return t;
        }).collect().forEach(t-> {
            System.out.println(t);
        });

    }
}
