package com.yxBuild

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import yxBuild.constant.GmallConstant

object MyEsUtil {

  /* ElasticSearch连接主机名 */
  private val elasticSearchHost = PropertiesUtil.load("config.properties").getProperty("elasticSearch.host")

  /* ElasticSearch连接主机名 */
  private val elasticSearchPort = PropertiesUtil.load("config.properties").getProperty("elasticSearch.port")

  /* 获取jestClient工厂客户端 */
  private var factory: JestClientFactory = null;

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    *
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    *
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticSearchHost + ":" + elasticSearchPort).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }

  /**
    * 批量添加数据(如果Index和Type不存在,则自动创建)
    *
    * @param indexName Index名称
    * @param typeName Type名称
    * @param list 数据集合
    */
  def insertBulk(indexName: String,typeName:String, list: List[Any]): Unit = {
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder
    bulkBuilder.defaultIndex(indexName).defaultType(typeName)
    for (doc <- list) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
    close(jest)
    println("保存ES: " + items.size() + "条")
  }

}
