package com.isea.imall.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.isea.imall.dw.common.constant.ImallConstant
import com.isea.imall.dw.common.util.MyEsUtil
import com.isea.imall.dw.realtime.bean.OrderInfo
import com.isea.imall.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object NewOrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("imall-realtime-new-order").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ImallConstant.KAFKA_TOPIC_ORDER, ssc)

    val orderInfoDStream: DStream[OrderInfo] = recordDStream.map(_.value()).map { jsonString =>

      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val dateArray: Array[String] = orderInfo.createTime.split(" ")

      orderInfo.createDate = dateArray(0)

      val hourMS: Array[String] = dateArray(1).split(":")

      orderInfo.createHour = hourMS(0)
      orderInfo.createHourMinute = hourMS(0) + ":" + hourMS(1)
      orderInfo
    }

    orderInfoDStream.foreachRDD { rdd =>
      // 将数据保存到ES
      rdd.foreachPartition { orderInfoItr =>
        MyEsUtil.insertBuilk(ImallConstant.ES_INDEX_NEW_ORDER, orderInfoItr.toList)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}