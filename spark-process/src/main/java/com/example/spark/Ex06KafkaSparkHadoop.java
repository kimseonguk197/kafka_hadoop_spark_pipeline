package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.VoidFunction2;

import static org.apache.spark.sql.functions.*;


// 실행명령어. 다른 파일과는 다르게 아래 코드에서는 kafka의존성이 필요하여 포함시켜 실행
// docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --class com.example.spark.Ex06KafkaSparkHadoop --master spark://spark-master:7077 /spark-process/build/libs/spark-examples.jar
public class Ex06KafkaSparkHadoop {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaMemberStreamingProcessor")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // kafka-consumer-producer/producer의 MemberDto가 직렬화하는 JSON 구조 (name, email, age, createdAt)
        StructType memberSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("createdAt", DataTypes.StringType, true)
        });

        System.out.println(">>> Ex06KafkaSparkHadoop 시작: member-topic 구독 대기 중...");

        // 1. member-topic 구독
        // consumer group id의 경우 spark-kafka-source-<UUID>-... 형태로 자동 생성
        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host.docker.internal:29092")
                .option("subscribe", "member-topic")
                .load();

        // 2. Kafka record의 value(byte[])를 문자열로 바꾼 뒤 JSON 파싱
        Dataset<Row> memberDf = kafkaDf
                .selectExpr("CAST(value AS STRING) AS json_value")
                .select(from_json(col("json_value"), memberSchema).as("data"))
                .select("data.*");

        // 3. 데이터 정제
        Dataset<Row> refinedDf = memberDf
                .withColumn("emailDomain", split(col("email"), "@").getItem(1))
                .withColumn("isAdult", when(col("age").geq(19), true).otherwise(false));

        // 4. HDFS(Parquet)에 저장
        // foreachBatch를 쓰면 배치마다 콜백이 호출되므로, 저장 전에 건수/내용을 로그로 찍을 수 있음
        String hdfsRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/member_stream/";
        StreamingQuery query = refinedDf.writeStream()
                .outputMode("append")
                .option("checkpointLocation", "hdfs://namenode:8020/user/hadoop/checkpoints/ex06/")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> {
                    long count = batchDf.count();
                    System.out.println(">>> [batch " + batchId + "] 수신 메시지 건수: " + count);
                    if (count > 0) {
                        batchDf.show(false);
                        batchDf.write().mode("append").parquet(hdfsRefinedPath);
                        System.out.println(">>> [batch " + batchId + "] HDFS 저장 완료: " + hdfsRefinedPath);
                    }
                })
                .start();

        query.awaitTermination();
    }
}