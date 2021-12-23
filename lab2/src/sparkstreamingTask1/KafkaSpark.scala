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


object KafkaSpark {
  def main(args: Array[String]) {
    val kafkaParams = Map(
    "metadata.broker.list" -> "localhost:9092",
    "zookeeper.connect" -> "localhost:2181",
    "group.id" -> "kafka-spark-streaming",
    "zookeeper.connection.timeout.ms" -> "1000")

    // make a connection to Kafka and read (key, value) pairs from it
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("readKeyValuePairFromKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
   
    ssc.checkpoint(".")
    
    val topic = Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder](
      ssc,
      kafkaParams,
      topic
    )

    val kvpair_str = messages.map(_._2).map(_.split(","))
    val kvpair_float = kvpair_str.map(x => (x(0), x(1).toDouble))
    
    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double)]): (String, Double) = {
      val value_new = value.getOrElse(0.0);
      var (count_state, sum_state) = state.getOption.getOrElse((0, 0.0))

      count_state   = count_state + 1;
      sum_state     = sum_state   + value_new;

      val avg_state = sum_state/count_state;

      state.update((count_state, sum_state))
      (key,avg_state)
    }

    val stateDstream = kvpair_float.mapWithState(StateSpec.function(mappingFunc _))

    
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
