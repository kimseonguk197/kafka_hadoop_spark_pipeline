package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex05ReadStreamingOutput {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ReadStreamingOutput")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 스트리밍으로 저장된 HDFS 경로
        String streamOutputPath = "hdfs://namenode:8020/user/hadoop/streaming_output/";

        // 1. 데이터 읽기 (Parquet 포맷 지정)
        // 스트리밍으로 계속 쌓인 모든 데이터를 한꺼번에 조회
        Dataset<Row> df = spark.read().parquet(streamOutputPath);

        System.out.println(">>> 스트리밍으로 적재된 데이터 조회:");
        df.show();

        // 2. 추가적인 분석 (예: 데이터 개수 확인)
        System.out.println(">>> 총 적재된 데이터 건수: " + df.count());

        spark.stop();
    }
}