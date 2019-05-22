package com.yxBuild

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Kafka实时消费的工具类
  *
  */
object MyKafkaUtil {
  /* 读取配置文件内容 */
  private val properties: Properties = PropertiesUtil.load("config.properties")

  /* 获取Kafka集群连接配置 */
  private val broker_list: String = properties.getProperty("kafka.broker.list")

  /* 配置Kafka消费者参数 */
  val kafkaParam = Map(
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "gMall_consumer_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  /**
    * 获取Kafka实时实例对象
    *
    * @param topic 主题名称
    * @param ssc StreamingContext实例对象
    * @return
    */
  def getKafkaStream(topic: String,ssc:StreamingContext): InputDStream[ConsumerRecord[String,String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](
                    ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam)
                  )
    dStream
  }

}
