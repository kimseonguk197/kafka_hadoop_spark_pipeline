package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.VoidFunction2;

import static org.apache.spark.sql.functions.*;


// 테스트 코드 실행
// spark-submit --class com.example.spark.Ex06KafkaSparkAWS --master yarn --deploy-mode cluster --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4 s3://your-s3-bucket-name/jars/spark-examples.jar
public class Ex07KafkaSparkAWS {
    public static void main(String[] args) throws Exception {
        // 1. SparkSession 생성 (EMR 환경에서는 master 설정을 코드 내에 박아두지 않고 외부에서 인자로 받거나 기본값 사용)
        SparkSession spark = SparkSession.builder()
                .appName("KafkaMemberStreamingProcessorAWS")
                .getOrCreate();
        
        spark.sparkContext().setLogLevel("WARN");

        // 2. Member 스키마 정의
        StructType memberSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("createdAt", DataTypes.StringType, true)
        });

        System.out.println(">>> Ex06KafkaSparkAWS 시작: MSK member-topic 구독 대기 중...");

        // 3. AWS MSK 구독 설정
        // TODO: 본인의 MSK 브로커 엔드포인트 주소들로 변경하세요 (예: b-1.xxx, b-2.xxx)
        String mskBootstrapServers = "c-10001.cjmsk.lhd0qr.c4.kafka.ap-northeast-2.amazonaws.com:9092,c-10002.cjmsk.lhd0qr.c4.kafka.ap-northeast-2.amazonaws.com:9092";

        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", mskBootstrapServers)
                .option("subscribe", "member-topic")
                .option("startingOffsets", "latest") // 또는 earliest
                .load();

        // 4. Kafka record 값 파싱
        Dataset<Row> memberDf = kafkaDf
                .selectExpr("CAST(value AS STRING) AS json_value")
                .select(from_json(col("json_value"), memberSchema).as("data"))
                .select("data.*");

        // 5. 데이터 정제
        Dataset<Row> refinedDf = memberDf
                .withColumn("emailDomain", split(col("email"), "@").getItem(1))
                .withColumn("isAdult", when(col("age").geq(19), true).otherwise(false));

        // 6. AWS S3(Parquet)에 저장 설정
        // TODO: 본인의 S3 버킷 이름으로 변경하세요
        String s3RefinedPath = "s3a://my-kafka-spark-bucket/refined_data/member_stream/";
        String s3CheckpointPath = "s3a://my-kafka-spark-bucket/checkpoints/ex06/";

        StreamingQuery query = refinedDf.writeStream()
                .outputMode("append")
                .option("checkpointLocation", s3CheckpointPath)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> {
                    long count = batchDf.count();
                    System.out.println(">>> [batch " + batchId + "] 수신 메시지 건수: " + count);
                    if (count > 0) {
                        batchDf.show(false);
                        // S3에 Parquet 포맷으로 저장
                        batchDf.write().mode("append").parquet(s3RefinedPath);
                        System.out.println(">>> [batch " + batchId + "] S3 저장 완료: " + s3RefinedPath);
                    }
                })
                .start();

        query.awaitTermination();
    }
}