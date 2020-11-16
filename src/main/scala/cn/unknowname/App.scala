package cn.unknowname

import scala.concurrent.duration.Duration

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.requests.searches.{SearchType, SearchHit, SearchIterator}


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


case class wordCount(word: String, count: Long)

object App {
  def main(args: Array[String]): Unit = {
    // Step1: 获取执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    // Step2: 连接数据源，这里用Socket。返回的就是DataStream类型
    val stream = streamEnv.socketTextStream("128.0.255.10", 9999, maxRetry=1)
    // Step3: 现在可以对数据进行各种操作了，如过滤、聚合、统计等
    val words = stream.flatMap(line => line.split("\\s"))
      .map(word => wordCount(word, 1))
      .keyBy(word => word.word)
      // 每隔2秒计算60秒内的滑动窗口的数据
      .timeWindow(Time.seconds(60), Time.seconds(2))
      .sum("count")
    words.print().setParallelism(1)
    // Step4： 转换后的结果输出至Sink
    // Step5: 执行
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 基于事件时间计算
    streamEnv.execute()
  }
}

class MyType{}

object TestES {
  def main(args: Array[String]): Unit = {
    val client = ElasticClient(JavaClient(ElasticProperties("http://128.0.255.186:9200")))
    // 佚代器取回所有数据
    implicit val timeout: Duration = Duration(10000, "millis")
    val iterator = SearchIterator.hits(
      client,
      search("goods_202003").searchType(SearchType.DEFAULT).matchAllQuery.keepAlive("1m").size(1)
    )
    val f = (n: SearchHit) => {
      // println("receive data")
      println(n.sourceAsString)
    }
    // iterator.foreach(f)
    val datas = iterator.take(2)
    val datas2 = iterator.take(2)
    datas.foreach(println)
    datas2.foreach(println)
  }

  def fetch(es: ElasticClient, index: String, size: Int = 1): Unit = {
    // scroll滑动窗口查询,第一次先取一条记录
    val results = es.execute(
      search(index).searchType(SearchType.DEFAULT).scroll("3m").size(size)
    ).await
    // 判断下状态码是不是正常
    if (results.isSuccess && results.status == 200) {
      var scrollId = results.result.scrollId
      println(results.result.getClass)
      // 随便再把取出的数据也处理下
      for ( data <- results.result.hits.hits) {
        println(data.sourceAsString)
      }
    } else {
      // 异常状态码返回并退出
      println("error happen...")
    }

  }
}

// 保存结果的样例，后续Flink执行相关的操作都是针对它
case class Total(shopID: String, total: Long){}

object TestSource {
  def main(args: Array[String]): Unit = {
    this.elasticExample()
  }

  // 这里不够严谨，只是演示。实际对于历史数据，应该使用DataSet API
  def elasticExample(): Unit = {
    import cn.unknowname.sources.{RichElasticSearchSource, ElasticsearchSource}
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = streamEnv.addSource(new RichElasticSearchSource(hosts="http://128.0.255.186", index="goods_202003"))
    // stream.assignTimestampsAndWatermarks()
    stream.map(good => Total(good.shopId, good.total))
      .keyBy(total => total.shopID)
      // window要在sum方法前执行,因为ElasticsearchSource每次处理100，因此这里是每100个数据统计一次
      .countWindow(1)
      .sum("total")
      .setParallelism(1)
      .print()
    streamEnv.execute()
  }
}
