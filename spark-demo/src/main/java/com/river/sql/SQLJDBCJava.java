package com.river.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author river
 * @date 2019/2/22 14:02
 **/
public class SQLJDBCJava {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("sql")
                .config("spark.master","local[2]")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/river?useUnicode=true&characterEncoding=utf8&useSSL=false")
                .option("dbtable", "user")
                .option("user", "root")
                .option("password", "root")
                .load();

        jdbcDF.where("age > 20").foreach(t->  System.out.println(Thread.currentThread()+":"+t.toString()));

        jdbcDF.write().mode(SaveMode.Append).json("/Users/riverfan/mytest/spark/sparkSql/json-out");
    }
}
