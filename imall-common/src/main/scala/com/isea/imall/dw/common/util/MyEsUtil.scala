package com.isea.imall.dw.common.util

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop101"
  private val ES_HTTP_PORT = 9200
  private var factory : JestClientFactory = null

  def getClient:JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    * @param client
    * @return
    */
  def close(client: JestClient) ={
    if (!Objects.isNull(client)) try
      client.hashCode()
    catch{
      case e :Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */

  private def build() ={
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + // 和哪台机器的那个端口建立连接
    ES_HTTP_PORT).multiThreaded(true).maxTotalConnection(20) // 连接总数
      .connTimeout(10000).readTimeout(10000).build()) // 连接建立的延迟，和读取数据的延迟
  }

  def insertBuilk(indexName : String,docLit: List[Any])  ={
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder

    bulkBuilder.defaultIndex(indexName).defaultType("_doc")

    println(docLit.mkString("\n"))

    for (doc <- docLit) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }

    val result: BulkResult = jest.execute(bulkBuilder.build())
    println("插入到ES中的数据个数：" + result.getItems.size())
    close(jest)
  }


  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
   val doc : String = "{\n  \"name\":\"isea\",\n  \"age\":22\n}"
    val index: Index = new Index.Builder(doc).index("test").`type`("_doc").build()
    jest.execute(index)
  }
}
