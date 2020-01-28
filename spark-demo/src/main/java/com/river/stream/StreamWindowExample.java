package com.river.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author river
 * @doc  nc -lk 9999
 */
public class StreamWindowExample {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaPairDStream<String, Integer> rst = lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x-> new Tuple2<>(x,1))
                .reduceByKeyAndWindow((x,y)->x+y,Durations.seconds(30),Durations.seconds(10));
                //.window(Durations.seconds(10),Durations.seconds(5))
               // .reduceByKey((x,y)->x+y);

        System.out.println("============================>");
        int[] pp = new int[1];
        rst.foreachRDD(t->{
            System.out.println("==========>"+pp[0]++);
        });

        rst.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
