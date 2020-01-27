package com.river.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlJsonExample {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SQLJava")
                .setMaster("local[2]");

        SparkSession session = SparkSession.builder()
                .appName("SQLJava")
                .config("spark.master", "local[1]")
                .getOrCreate();


        Dataset<Row> df1 = session.read().json("/Users/riverfan/mytest/spark/sparkSql/json.txt");
//        创建临时视图
        df1.createOrReplaceTempView("customers");
        df1.show();


        df1.select("id").where("id > 1").show();

        //按照sql方式查询
        Dataset<Row> df2 = session.sql("select * from customers where age > 25 ");
        df2.show();
        System.out.println("=================");

    }
}
