package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class Ex04JsonStreaming {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("StreamingBasics")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        });

        // 1. 스트리밍 데이터 읽기
        Dataset<Row> streamingDf = spark.readStream()
                .schema(schema)
                .json("/test_data/streaming_input/");

        // 2. 가공 로직
        Dataset<Row> processedDf = streamingDf.filter(col("age").geq(19));

        // 3. 실시간 HDFS 저장
        // path: 실제 결과 데이터(정제된 Parquet 파일)가 쌓이는 곳
        // checkpointLocation: 데이터가 아니라 이 스트리밍 쿼리의 진행 상태가 기록되는 곳
        StreamingQuery query = processedDf.writeStream()
                .outputMode("append")
                .format("parquet") // 저장 포맷 지정 (json, parquet 등)
                .option("path", "hdfs://namenode:8020/user/hadoop/streaming_output/") // 저장할 HDFS 경로
                .option("checkpointLocation", "hdfs://namenode:8020/user/hadoop/checkpoints/ex04/") // 현재까지 처리한 데이터의 위치(Offset)와 연산 상태를 기록
                .start();

        query.awaitTermination();
    }
}