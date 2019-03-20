package com.isea.imall.dw.realtime.app


import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.isea.imall.dw.common.constant.ImallConstant
import com.isea.imall.dw.common.util.MyEsUtil
import com.isea.imall.dw.realtime.bean.StartupLog
import com.isea.imall.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartUpApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("imall-realtime-startup")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ImallConstant.KAFKA_TOPIC_STARTUP, ssc)

    /*    recordDStream.map(_.value()).foreachRDD( rdd =>
          println(rdd.collect().mkString("\n"))
        )*/

    // 转成Javabean 结构化数据
    val startupLogDStream: DStream[StartupLog] = recordDStream.map(_.value()).map { jsonString =>
      val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
      val logDateTime: String = dateFormat.format(new Date(startupLog.ts))
      startupLog.logDate = logDateTime.split(" ")(0)
      startupLog.logHourMinute = logDateTime.split(" ")(1)
      startupLog.logHour = logDateTime.split(" ")(1).split(":")(0)
      startupLog
    }

    // 过滤今天登陆过的用户，利用广播变量，将今天redis中的登录用户广播到Executor中
    val filterStarupLogDStream: DStream[StartupLog] = startupLogDStream.transform { rdd =>

      println("过滤前：" + rdd.count())

      // Driver端
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val today: String = dateFormat.format(new Date())

      val jedisClient: Jedis = RedisUtil.getJedisClient
      val dauSet: util.Set[String] = jedisClient.smembers("dau:" + today)
      val dauSetBC: Broadcast[util.Set[String]] = sc.broadcast(dauSet)

      // Executor端
      val filteredRDD: RDD[StartupLog] = rdd.filter { startupLog =>
        if (dauSetBC.value != null && dauSetBC.value.contains(startupLog.mid)){
          false
        } else{
          true
        }
      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD
    }

    /*
       使用的数据类型：每天都会有一个key，但是对应着多个value
       使用set，key："dau"+date daily active user value:mid
     */
    // 把今日访问的用户保存到redis中

    filterStarupLogDStream.foreachRDD(rdd =>
      //driver
      rdd.foreachPartition{startupLogItr =>
        //executor
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val listBuffer = new ListBuffer[Any]
        for (startupLog <- startupLogItr) {
          val dauKey: String = "dau:" + startupLog.logDate
          jedisClient.sadd(dauKey,startupLog.mid)
          listBuffer += startupLog
        }
        jedisClient.close()

        // 将数据存储到ES中
        MyEsUtil.insertBuilk(ImallConstant.ES_INDEX_DAU,listBuffer.toList)
      }
    )

    // 目标：保存到redis中，
    ssc.start()
    ssc.awaitTermination()
  }
}
