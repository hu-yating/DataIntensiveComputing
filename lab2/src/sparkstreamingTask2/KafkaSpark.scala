package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{Trigger, GroupState, GroupStateTimeout}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, split}

object KafkaSpark {
  def main(args: Array[String]) {
    
  
    val spark:SparkSession = SparkSession.builder.appName("avg_spark").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val inputTable = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "avg").load()
    

    import spark.implicits._

    // val k : ((row: Row)) => String = {case (k, _) => k}
    val temp_table = inputTable.selectExpr("CAST(value AS STRING)").as[(String)]
        .select(split(col("value"),",").getItem(0).as("key"),
                split(col("value"),",").getItem(1).as("val")).drop("value")
        .groupByKey(x => x.getString(0))


def mappingFunc(key: String, values: Iterator[Row], state: GroupState[(Double, Int)]):(String, Double) = {
        // val (sum, cnt) = state.getOption.getOrElse((0.0, 0))
        var (sum, cnt) = state.getOption.getOrElse((0.0, 0))
        // val sum = state.get._1
        // val cnt = state.get._2
    
        // val newSum = value.map(_(0)) + sum
        // val newSum = state.getOption.getOrElse(0.0)
        
        values.foreach{value => 
            // println(value)
            cnt = cnt + 1
            sum = value.getString(1).toDouble + sum
        }
        
        // val newCnt = state.getOption.map(_._2 + 1).getOrElse(0)
        state.update((sum, cnt))
        (key, sum/cnt)
    }

    val resultTable = temp_table.mapGroupsWithState(mappingFunc _)

    resultTable.writeStream.format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .start()
      .awaitTermination()
  }
}
