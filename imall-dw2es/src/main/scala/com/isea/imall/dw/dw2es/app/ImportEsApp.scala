package com.isea.imall.dw.dw2es.app

import com.isea.imall.dw.dw2es.bean.SaleDetailDayCount
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession

object ImportEsApp {
  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("imall-dw2es")

    val sparkSession: SparkSession = new spark.sql.SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate()

    // 读取宽表，该表在Hive中的DWS层
    sparkSession.sql("use imall_one")
    import sparkSession.implicits._
    val saleRDD: RDD[SaleDetailDayCount] = sparkSession.sql("select user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level,cast( sku_price as double) sku_price,sku_name,sku_tm_id, sku_category3_id,sku_category2_id,sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount,dt from dws_sale_detail_daycount where dt='2019-04-01'").as[SaleDetailDayCount].rdd

    // 把rddd存入到Es中


  }



















}
