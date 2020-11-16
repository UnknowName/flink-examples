package cn.unknowname.sources

import scala.concurrent.duration.Duration
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.search
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.ElasticProperties
import com.sksamuel.elastic4s.requests.searches.{SearchIterator, SearchType}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext


// 无并发的Source类,示例这里后续传入的类型是Long类型
class ElasticsearchSource extends SourceFunction[Long] {
  var total = 0L
  var isRunning = true

  // 所有产生数据的动作都在run方法中实现
  override def run(sourceContext: SourceContext[Long]): Unit = {
    while (isRunning) {
      sourceContext.collect(this.total)
      this.total += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    println("取消当前任务中....")
    this.isRunning = false
    println("取消成功")
  }
}


// 支持并发的Source类，Long是类型，也可以自定义的类型
class ParallelElasticsearchSource extends ParallelSourceFunction[Long] {
  var total = 0L
  var isRunning = true

  // 所有产生数据的动作都在run方法中实现
  override def run(sourceContext: SourceContext[Long]): Unit = {
    while (isRunning) {
      sourceContext.collect(this.total)
      this.total += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    println("取消当前任务中....")
    this.isRunning = false
    println("取消成功")
  }
}


class GoodsOrder(name: String, val requestTime: Long, val total: Long, val shopId: String) {

}


// 富函数，后续传入的类型是MyType
class RichElasticSearchSource(val hosts: String, index: String) extends RichSourceFunction[GoodsOrder] {
  // 单个 hosts = "http://192.168.0.1:9200"
  // 集群 hosts = "http://192.168.0.1:9200,192.168.0.2:9200"
  private val es = ElasticProperties(hosts)
  private var esClient: ElasticClient = _
  private var count = 0 // 已处理的记数，用于从ES返回的记录偏移量
  private val number = 100 // 每次处理的数量

  override def open(config: Configuration): Unit = {
    println("获取外部连接相关代码，一定要放这里！！放在初始化方法就会报错，任务启动时执行一次")
    // 即使地址有误，连接也不会发生异常，会在取数据时产生异常
    this.esClient = ElasticClient(JavaClient(this.es))
  }

  override def run(sourceContext: SourceContext[GoodsOrder]): Unit = {
    // 定义一个隐式变量，用于从ES获取数据定义的超时
    implicit val timeout: Duration = Duration(5000, "millis")
    while (this.esClient != null) {
      val iterator = SearchIterator.hits(
        this.esClient,
        search(this.index).searchType(SearchType.DEFAULT).matchAllQuery.keepAlive("1m").size(this.number)
      )
      // 不能使用foreach，这样会导致将索引中的所有数据全部取出后再执行相关代码，通过偏移量，每次取100条
      val hits = iterator.slice(this.count, this.count + this.number)
      if (hits.nonEmpty) {
        for (hit <- hits) {
          val dict = hit.sourceAsMap
          val shopId = dict.getOrElse("mercId", "default").toString
          val requestTime = dict.getOrElse("requestTime", "0").toString.toLong
          val total = dict.getOrElse("goodsAmt", "0").toString.toLong
          val name = dict.getOrElse("goodsName", "default").toString
          // val good = new GoodsOrder(name, requestTime, total,shopId)
          sourceContext.collect(new GoodsOrder(name, requestTime, total,shopId))
        }
        this.count += this.number
      } else {
        println("ES索引数据已处理完毕，任务正常结束")
        this.cancel()
      }
    }
  }

  override def close(): Unit = {
    println("调用关闭方法...")
    if (this.esClient != null) {
      this.esClient.close()
    }
    this.esClient = null
  }

  override def cancel(): Unit = {
    // 直接调用类本身的close方法
    println("调用取消方法....")
    this.close()
  }
}
