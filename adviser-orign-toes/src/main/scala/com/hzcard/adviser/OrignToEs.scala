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
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.`package`.SQLContextFunctions
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayOps
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import org.apache.commons.logging.LogFactory
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

object OrignToEs {
  @transient lazy val log = LogFactory.getLog(OrignToEs.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      throw new IllegalArgumentException("Not enough arguments: missing args name.")
    val conf = new SparkConf().setAppName("OrignToEs")
    conf.set("es.index.auto.create", "true")
    conf.set("es.index.read.missing.as.empty", "yes")
    val kafkaParams = scala.collection.mutable.Map[String, Object](
      "bootstrap.servers" -> "192.168.102.164:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
      
    for (i <- 0 to args.length - 1) {  //设置参数
      args(i) match {
        case x if(x.startsWith("--es.") || x.startsWith("--spark.")) => conf.set(x.drop(2), args(i + 1))
        case x if(x.startsWith("--kafka.")) => kafkaParams += (x.drop("--kafka.".length())-> args(i + 1))
        case _                          =>{}
      }
    }
    if (conf.get("es.nodes") == null || conf.get("es.nodes").trim().length() == 0)
      throw new IllegalArgumentException("Not enough arguments es.nodes: must be setting.")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topics = Array("lognewtopic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)) //.window(Seconds(30),Seconds(20)).map(_.value())
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        processRdd(rdd)
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
  def processRdd(rdd: RDD[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]): Unit = {
    val line = rdd.map(record => (record.key, record.value)).map(_._2)
    val logObjectRdd = line.map { x => x.split(" ") } //将日志每一行拆成数组
      .map { x =>
        {
          val y = x.toBuffer
          if (y.length > 7) {
            for (i <- 7 until y.length)
              y(6) = y(6) + " " + y(i) //把日志体再合并在一起
          }
          y(1) = y(0) + " " + y(1) //将时间合并
          y.remove(0)
          y.toArray
        }
      }.filter { x => x(5).trim().length() > 0 }
      .filter { x => !x(5).startsWith("Accept:") || !x(5).startsWith("Content-Type:") || !x(5).startsWith("content-type:") || !x(5).startsWith("server:") }
      .map { x => new LogObject(getTimestamp(x(0)), x(1), x(2), x(5)) }
    EsSpark.saveToEs(logObjectRdd, "feiginclient/new")
  }

  def getTimestamp(x: Any): java.sql.Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
    if (x.toString() == "")
      return null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      return t
    }
  }

  @SerialVersionUID(-3656453505503987988L) case class LogObject(val timestamp: Timestamp, val threadName: String, val requestResponseKey: String, val logContent: String)
}