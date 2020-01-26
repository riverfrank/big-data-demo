package com.river.globleValue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * @author riverfan
 */
public class AccumulatorExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

        accum.value();
        System.out.println(accum.value());
    }
}
