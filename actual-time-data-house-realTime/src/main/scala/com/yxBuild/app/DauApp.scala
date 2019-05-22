package com.yxBuild.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.yxBuild.{DateUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import com.yxBuild.bean.StartUp
import yxBuild.constant.GmallConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * 计算日活的设备数
  *
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    // 1、获取SparkConf实例对象
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    // 2、获取streamingContext实例对象
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 3、获取Kafka消费流的启动日志消息
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, streamingContext)

    // 4、遍历DStream
    val startUpLogDStream: DStream[StartUp] = inputDStream.map(record => {
      // 4.1、获取一条启动日志消息
      val startUpStr: String = record.value()
      // 4.2、将日志消息转换为StartUp样例类
      val startUp: StartUp = JSON.parseObject(startUpStr, classOf[StartUp])
      // 4.3、转化时间
      val startUpTime: String = DateUtil.toDateStrByTimestamp(startUp.ts)
      // 4.4、切割时间格式
      val dateAndTimeArray: Array[String] = startUpTime.split(" ")
      // 4.5、获取日期、时、分,并且进行赋值
      startUp.date = dateAndTimeArray(0) // 日期
      startUp.hour = dateAndTimeArray(1).split(":")(0) // 时
      startUp.minute = dateAndTimeArray(1).split(":")(1) // 分
      startUp
    })

    // 5、将消息保存到Redis的Set类型中,因为Set具有去重作用
    val jedisClient: Jedis = RedisUtil.getJedisClient
    jedisClient.select(5)

    // 6、过滤已经今日已经登录过的设备(判断是否在Redis已经保存,如果保存不需要在RDD返回)
    val filteredStartUpLogDStream: DStream[StartUp] = startUpLogDStream.transform(rdd => {
      // 6.1、定义Key值
      val todayDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key: String = "dau:" + todayDate
      // 6.1、获取Redis中的该key所有值
      val dauSet: util.Set[String] = jedisClient.smembers(key)
      // 6.2 由于transform是在Driver端执行,而filter在Exector端执行,所以需要将dauSet作为广播变量
      val dauSetBC: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(dauSet)
      // 6.3、过滤掉已经保存的设备标识
      val filterRDD: RDD[StartUp] = rdd.filter(startUpLog => {
        val tmpDauSet: util.Set[String] = dauSetBC.value
        !tmpDauSet.contains(startUpLog.mid)
      })
      filterRDD
    })
    jedisClient.close()

    // 7、根据Mid进行分组,定制格式
    val groupByMidDStream: DStream[(String, Iterable[StartUp])] = filteredStartUpLogDStream.map(startUpLog => (startUpLog.mid,startUpLog)).groupByKey()

    // 8、每组取一个
    val distinctStartUpDStream: DStream[StartUp] = groupByMidDStream.flatMap {
      case (mId, startupItr) => {
        startupItr.take(1)
      }
    }

    // 9、记录今天登录的用户,将数据保存到Redis
    distinctStartUpDStream.foreachRDD(rdd => {
      rdd.foreachPartition(startUpLogItr => {
        val jedis: Jedis = RedisUtil.getJedisClient
        jedis.select(5)
        val startUpList: List[StartUp] = startUpLogItr.toList
        // 9.1 将新Mid进行保存到Redis
        for (startUpLog <- startUpList ) {
          val key="dau:"+ startUpLog.date
          jedis.sadd(key,startUpLog.mid)
        }
        jedis.close()
        // 9.2 将详细数据保存到ElasticSearch
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_DAU,GmallConstant.ES_DEFAULT_TYPE,startUpList)
      })
    })

    // 10、启动
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
