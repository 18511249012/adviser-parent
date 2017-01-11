package com.hzcard.adviser
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayOps
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty
import org.elasticsearch.spark.rdd._
import com.fasterxml.jackson.databind.ObjectMapper
import scala.util.Sorting
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkEnv
import org.elasticsearch.hadoop.rest.RestService
import org.apache.spark.scheduler.DAGScheduler

object EventFlowFirstStage {
  @transient lazy val log = LogFactory.getLog(EventFlowFirstStage.getClass)
  implicit def eventEntityToOrdered(p: EventEntity) = new OrderedEventEntity(p)
  def main(args: Array[String]): Unit = {

    if (args.length == 0)
      throw new IllegalArgumentException("Not enough arguments: missing args name.")

    val conf = new SparkConf().setAppName("queryEs")
    for (i <- 0 to args.length - 1) {

      args(i) match {
        case x if (x.startsWith("--es.") || x.startsWith("--spark.")) => conf.set(x.drop(2), args(i + 1))
        case _ =>
      }

    }
    if (conf.get("es.nodes") == null || conf.get("es.nodes").trim().length() == 0)
      throw new IllegalArgumentException("Not enough arguments es.nodes: must be setting.")

    //    conf.set("es.nodes", "192.168.102.165:9200")
    //    conf.set("es.net.http.auth.user", "elastic");
    //    conf.set("es.net.http.auth.pass", "elastic");
    //  conf.set("es.nodes", "172.16.16.4:9200")
    //  conf.set("spark.cores.max", "5")
    //  conf.set("spark.executor.memory", "5g")

    //    conf.set("es.nodes", "172.16.48.4:9200")      //pre

    conf.set("es.index.read.missing.as.empty", "yes")
    conf.set("es.index.auto.create", "true")
    conf.set("spark.rpc.message.maxSize", "512")
    val sc = new SparkContext(conf)
    val myVectorAcc = new EventAccumulatorV2
    sc.register(myVectorAcc, "myVectorAcc")
    while (true) {
      //    sc.broadcast(myVectorAcc)
      val dealedRecodRdd = sc.esRDD("feiginclient/dealedrecod", "?q=business:eventFirstStage");
      lazy val businessId = {
        if (dealedRecodRdd.isEmpty())
          None
        else
          Some(dealedRecodRdd.collect().take(1)(0)._1)
      }
      lazy val recordTime = {
        dealedRecodRdd.collect().take(1)(0)._2
      }
      val queryString = {
        if (!dealedRecodRdd.isEmpty() && !recordTime.get("recordtime").isEmpty) {
          try {
            "{\"query\":{\"range\" : {\"timestamp\" : {\"gt\" : \"" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(recordTime.get("recordtime").get) + "\",\"format\":\"yyyy-MM-dd HH:mm:ss.SSS\",\"time_zone\": \"+08:00\"}}},\"sort\": [{\"timestamp\":\"asc\"}]}"
          } catch {
            case x: Throwable => {
              log.error(x)
              "{\"query\": {\"match_all\": {}},\"sort\": [{\"timestamp\":\"asc\"}]}"
            }
          }
        } else
          "{\"query\": {\"match_all\": {}},\"sort\": [{\"timestamp\":\"asc\"}]}"
      }
      val rdd = sc.esRDD("feiginclient/new", Map("es.query" -> queryString)).map(_._2)

      val eventRdd = rdd.map(x => {
        val startTime = x.get("timestamp").get.asInstanceOf[java.util.Date] //时间
        val requestResponseKey = x.get("requestResponseKey").get.asInstanceOf[String] //请求标识

        val clientAndProfileId = {
          val threadName = x.get("threadName").get.asInstanceOf[String]
          try {
            if (threadName.indexOf(":") > 0) {
              val threadNameSplits = threadName.split("-").drop(2).dropRight(1).fold("") { (x, y) => x + "-" + y }.drop(1)
              val clientAndProfile = threadNameSplits.split(":")
              (clientAndProfile(0), clientAndProfile.drop(1).fold("") { (x, y) => x + y })
            } else
              (threadName, "")
          } catch {
            case x: Throwable => {
              println(threadName)
              log.error(x)
              (threadName, "")
            }
          }
        }
        val logContent = { //日志体
          x.get("logContent").getOrElse(null) match {
            case None => null
            case _    => x.get("logContent").get.asInstanceOf[String]
          }
        }
        try {
          if (logContent != null) { //日志体解析
            if (logContent.startsWith("--->") && !logContent.startsWith("---> END") && !logContent.startsWith("---> RETRYING")) RequestStart(requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, logContent.split(" ")(1), logContent.split(" ")(2))
            else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-id") || logContent.startsWith("X-Event-Id"))) EventID(requestResponseKey, logContent.split(":")(1).trim())
            else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-type") || logContent.startsWith("X-Event-Type"))) EventType(requestResponseKey, logContent.split(":")(1).trim())
            else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-code") || logContent.startsWith("X-Event-Code"))) EventCode(requestResponseKey, logContent.split(":")(1).trim())
            else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-platform") || logContent.startsWith("X-Event-Platform"))) EventTypePlatform(requestResponseKey, logContent.split(":")(1).trim())
            else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-sequence") || logContent.startsWith("X-Event-Sequence"))) EventRequestSequence(requestResponseKey, logContent.split(":")(1).trim())
            else if (logContent.contains("{") || logContent.contains("<xml>")) EventHttpBody(requestResponseKey, startTime, logContent) //以json开始的标识{作为判断是否是事件体
            else if (logContent.startsWith("--->") && logContent.startsWith("---> END")) RequestEnd(requestResponseKey, startTime)
            else if (logContent.startsWith("<---") && !logContent.startsWith("<--- END")) {
              val httpResponseSplits = logContent.split(" ")
              ResponseStart(requestResponseKey, httpResponseSplits.takeRight(1)(0).substring(1, httpResponseSplits.takeRight(1)(0).indexOf("m")).toInt, httpResponseSplits(2), startTime)
            } else if (logContent.startsWith("<---") && logContent.startsWith("<--- END"))
              ResponseEnd(requestResponseKey, startTime)
            else
              NoVialbleLog(requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, logContent)
          } else
            NoVialbleLog(requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, "")
        } catch {
          case x: Throwable => {
            log.error("解析报文体错误！！！！" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startTime), x)
            println(logContent);
            if (logContent != null)
              if (requestResponseKey != null)
                NoVialbleLog(requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, logContent)
              else {
                println("没有requestResponseKey 值:" + logContent)
                NoVialbleLog("null", clientAndProfileId._1, clientAndProfileId._2, startTime, logContent)
              }
            else if (requestResponseKey != null)
              NoVialbleLog(requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, "")
            else
              NoVialbleLog("null", clientAndProfileId._1, clientAndProfileId._2, startTime, "")
          }
        }
      }).foreach({ x => myVectorAcc.add(x) })
      //    }).foreach { x => myVectorAcc.add(x) }
      val eventEnds = myVectorAcc.value.values().
        toArray(new Array[EventEntity](myVectorAcc.value.values().size())).filter { x => x.startTime != null }.filter { x => x.endTime != null }.sorted
      if (eventEnds.size > 0) {
        EsSpark.saveToEs(sc.makeRDD(eventEnds.toSeq), "eventflow/firststage")
        EsSpark.saveToEs(sc.makeRDD(Seq(BusinessRecoder(businessId.getOrElse(UUID.randomUUID().toString()), eventEnds.takeRight(1)(0).startTime, "eventFirstStage"))), "feiginclient/dealedrecod", Map("es.mapping.id" -> "id"))
      }
      myVectorAcc.reset()
      TimeUnit.SECONDS.sleep(10)
    }
  }

  sealed abstract class EventComponent(requesResponseKey: String) extends Serializable {
    def getRequesResponseKey = requesResponseKey
  }

  case class RequestStart(requesResponseKey: String, client: String, profileAndId: String, startTime: java.util.Date, method: String, url: String) extends EventComponent(requesResponseKey) //请求开始

  case class EventID(requesResponseKey: String, id: String) extends EventComponent(requesResponseKey) //事件ID

  case class EventCode(requesResponseKey: String, code: String) extends EventComponent(requesResponseKey) //事件类型

  case class EventType(requesResponseKey: String, eventType: String) extends EventComponent(requesResponseKey) //事件类型

  case class EventTypePlatform(requesResponseKey: String, platForm: String) extends EventComponent(requesResponseKey) //事件平台

  case class EventRequestSequence(requesResponseKey: String, sequence: String) extends EventComponent(requesResponseKey) //事件请求序列

  case class EventHttpBody(requesResponseKey: String, time: java.util.Date, body: String) extends EventComponent(requesResponseKey) //请求体、返回体Json

  case class RequestEnd(requesResponseKey: String, requestEndTime: java.util.Date) extends EventComponent(requesResponseKey) //请求结束

  case class ResponseStart(requesResponseKey: String, durition: Int, httpStatus: String, startTime: java.util.Date) extends EventComponent(requesResponseKey) //请求返回持续时间、状态

  case class ResponseEnd(requesResponseKey: String, endTime: java.util.Date) extends EventComponent(requesResponseKey) //请求返回

  case class NoVialbleLog(requesResponseKey: String, client: String, profileAndId: String, startTime: java.util.Date, notIgnoreLog: String) extends EventComponent(requesResponseKey)

  class OrderedEventEntity(val even: EventEntity) extends Ordered[EventEntity] {
    def compare(that: EventEntity) = even.startTime.compareTo(that.startTime)
  }

  case class BusinessRecoder(id: String, recordtime: Date, business: String)

}