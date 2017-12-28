package com.hzcard.adviser

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import redis.clients.jedis.{Jedis, JedisShardInfo}

import collection.JavaConverters._
import scala.collection.mutable

object OrignToEs {
  @transient lazy val log = LogFactory.getLog(OrignToEs.getClass)

  val mapper = new ObjectMapper()

  import com.fasterxml.jackson.module.scala.DefaultScalaModule

  implicit def eventEntityToOrdered(p: EventEntity) = new OrderedEventEntity(p)

  mapper.registerModule(DefaultScalaModule)

  val REDIS_HOST = "host"

  val REDIS_PORT = "port"

  val REDIS_PASSWORD = "password"

  val REDIS_DATABASE = "dataBase"
  val REDIS_CONNECTIONTIMEOUT = "connTimeOut"
  val REDIS_SOTIMEOUT = "soTimeOut"

  val NO_COMPLETE_CACHE_KEY = "NO_COMPLETE_"

  val KAFKA_OFFSET_CACHE_KEY = "LOG_KAFKA_OFFSET"

  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      throw new IllegalArgumentException("Not enough arguments: missing args name.")
    val redisConf = scala.collection.mutable.Map[String, String](
      REDIS_HOST -> "192.168.102.206",
      REDIS_PORT -> "6379",
      REDIS_CONNECTIONTIMEOUT -> "2000",
      REDIS_SOTIMEOUT -> "2000")

    val conf = new SparkConf().setAppName("OrignToEs")
    conf.set("es.index.auto.create", "true")
    conf.set("es.index.read.missing.as.empty", "yes")
    val kafkaParams = scala.collection.mutable.Map[String, Object](
      "bootstrap.servers" -> "192.168.102.164:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "exampe",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    for (i <- 0 to args.length - 1) { //设置参数
      args(i) match {
        case x if x.startsWith("--redis.") => redisConf += (x.drop("--redis.".length()) -> args(i + 1))
        case x if (x.startsWith("--es.")) => conf.set(x.drop(2), args(i + 1))
        case x if (x.startsWith("--spark.")) => conf.set(x.drop(2), args(i + 1))
        case x if (x.startsWith("--kafka.")) => kafkaParams += (x.drop("--kafka.".length()) -> args(i + 1))
        case _ => {}
      }
    }
    if (conf.get("es.nodes") == null || conf.get("es.nodes").trim().length() == 0)
      throw new IllegalArgumentException("Not enough arguments es.nodes: must be setting.")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val jedisShardInfo = new JedisShardInfo(redisConf.get(REDIS_HOST).get, redisConf.get(REDIS_PORT).get.toInt)
    if (!redisConf.get(REDIS_PASSWORD).isEmpty)
      jedisShardInfo.setPassword(redisConf.get(REDIS_PASSWORD).get)

    jedisShardInfo.setConnectionTimeout(redisConf.get(REDIS_CONNECTIONTIMEOUT).get.toInt)
    jedisShardInfo.setSoTimeout(redisConf.get(REDIS_SOTIMEOUT).get.toInt)

    //拿到offset
    val offsetRedis = jedisShardInfo.createResource()
    val offsetStr = offsetRedis.get(KAFKA_OFFSET_CACHE_KEY)

    val fromOffset = {       //读取offset
      val mutableMap = mutable.Map.empty[TopicPartition, Long]
      if (offsetStr != null) {
        val savedOffset = mapper.readValue(offsetStr, classOf[Map[TopicPartitionRedis, Long]])
        for (offset <- savedOffset)
          mutableMap.put(new TopicPartition(offset._1.topic, offset._1.partition), offset._2)
      }
      mutableMap.toMap
    }


    val ssc = new StreamingContext(conf, Seconds(1))
    val myVectorAcc = new EventAccumulatorV2
    ssc.sparkContext.register(myVectorAcc, "myVectorAcc")
    val topics = Array("lognewtopic")
    val stream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams, fromOffset)) //.window(Seconds(30),Seconds(20)).map(_.value())
    stream.foreachRDD(x => {
      val offsetRanges = x.asInstanceOf[HasOffsetRanges].offsetRanges
      //      offsetRanges.foreach(offsetRange=> offsetRange.)
      val redisOffset = offsetRanges.foldLeft(mutable.Map.empty[TopicPartitionRedis, Long])((m, o) => {
        m.put(TopicPartitionRedis(o.topic, o.partition), o.fromOffset); m
      }).toMap

      x.map(changeLinToLogObject).map(logObjectToEvent).foreach(myVectorAcc.add)
      val vector = myVectorAcc.value.values().toArray(new Array[EventEntity](myVectorAcc.value.values().size()))
      val events = vector.filter(x => x.startTime != null && x.endTime != null).sorted.toBuffer
      val notCompletEvent = vector.filter(x => x.startTime != null && x.endTime == null).sorted
      //没有startTime，但是有endTime
      val noStartEvents = vector.filter(x => x.startTime == null && x.endTime != null)
      //既没有startTime，又没有endTime
      val noStartNoEndEvents = vector.filter(x => x.startTime == null && x.endTime == null)

      val redisClient = jedisShardInfo.createResource()
      try {
        redisClient.pipelined()
        redisClient.select(redisConf.get(REDIS_DATABASE).getOrElse("0").toInt);
        //        处理noStartEvents
        val afterMergerNoStartEvents = noStartEvents.map(x => { //循环有endtime没有startTime的数据。进行合并
          val startNoEndEvent = redisClient.get(NO_COMPLETE_CACHE_KEY + x.requesResponseKey) //看看有没有之前缓存的没有endTime的事件数据
          if (startNoEndEvent != null) {
            val newEvent = mapper.readValue(startNoEndEvent, classOf[EventEntity]).mergerEventEntity(x)
            redisClient.set(NO_COMPLETE_CACHE_KEY + x.requesResponseKey, mapper.writeValueAsString(newEvent))
            Some(x)
          } else
            None
        }).flatten
        afterMergerNoStartEvents.foreach(x => { //afterMergerNoStartEvents长度存在，则丢失了start信息。只能转成不完全的event来存储到es
          events.append(x) //不完整的插入
        })


        //处理noStartNoEndEvents
        val afterNoStartNoEndEvents = noStartNoEndEvents.map(x => {
          val startNoEndEvent = redisClient.get(NO_COMPLETE_CACHE_KEY + x.requesResponseKey) //看看有没有之前缓存的没有endTime的事件数据
          if (startNoEndEvent != null) { //startNoEndEvent也没有的话，只能丢弃，说明日志丢失了
            val newEvent = mapper.readValue(startNoEndEvent, classOf[EventEntity]).mergerEventEntity(x)
            redisClient.set(NO_COMPLETE_CACHE_KEY + x.requesResponseKey, mapper.writeValueAsString(newEvent))
            Some(x)
          } else
            None
        }).flatten
        afterNoStartNoEndEvents.foreach(x => { //afterNoStartNoEndEvents长度存在，则丢失了start和endTime信息。只能转成不完全的event来存储到es
          events.append(x) //不完整的插入
        })


        //循环处理redis的key值
        val keys = redisClient.keys(NO_COMPLETE_CACHE_KEY + "*")
        keys.asScala.foreach(key => {
          val eventEntity = mapper.readValue(redisClient.get(key), classOf[EventEntity])
          if (eventEntity.endTime != null) { //合并完成的事件，
            events.append(eventEntity) //添加并删除合并完成的事件
            redisClient.del(NO_COMPLETE_CACHE_KEY + eventEntity.requesResponseKey) //删除缓存
          } else if (events.length > 0 && ((events(0).eventTime.getTime - eventEntity.startTime.getTime) / (1000 * 60)) > 15) { //缓存了太长时间--15分钟的事件
            events.append(eventEntity) //不完整的插入
            redisClient.del(NO_COMPLETE_CACHE_KEY + eventEntity.requesResponseKey)
          }

        })

        //notCompletEvent缓存到redis 里,暂时不写入es
        notCompletEvent.foreach(event => {
          redisClient.set(NO_COMPLETE_CACHE_KEY + event.requesResponseKey, mapper.writeValueAsString(event))
        })

        if (events.length > 0) {
          //按天分组，写入不同的索引中
          val dayEvents = events.groupBy(x => {
            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+08:00"))
            simpleDateFormat.format(x.eventTime)
          })

          dayEvents.foreach(dayEvent => {
            EsSpark.saveToEs(ssc.sparkContext.makeRDD(dayEvent._2.toArray.toSeq), s"log${dayEvent._1}/eventflow", Map("es.mapping.id" -> "requesResponseKey"))
          })
        }
        redisClient.set(KAFKA_OFFSET_CACHE_KEY,mapper.writeValueAsString(redisOffset))     //存储offset
//        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } catch {
        case th: Throwable =>
          log.error(th.getMessage, th)
          throw th;
      } finally {
        redisClient.close()
        myVectorAcc.reset() //reset已经完成的事件

      }
    }
    )


    //    stream.foreachRDD { rdd =>
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      if (!rdd.isEmpty()) {
    //        processRdd(rdd)
    //      }
    //      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    //    }

    ssc.start()
    ssc.awaitTermination()
  }

  def logObjectToEvent(logObject: LogObject): EventComponent = {
    val logContent = logObject.logContent.getOrElse("")
    val clientAndProfileId = {
      val threadName = logObject.threadName
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
    val startTime = logObject.timestamp
    if (logContent.length > 0) { //日志体解析
      if (logContent.startsWith("--->") && !logContent.startsWith("---> END") && !logContent.startsWith("---> RETRYING")) RequestStart(logObject.requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, logContent.split(" ")(1), logContent.split(" ")(2),logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-id") || logContent.startsWith("X-Event-Id"))) EventID(logObject.requestResponseKey, logContent.split(":")(1).trim(), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-type") || logContent.startsWith("X-Event-Type"))) EventType(logObject.requestResponseKey, logContent.split(":")(1).trim(), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-code") || logContent.startsWith("X-Event-Code"))) EventCode(logObject.requestResponseKey, logContent.split(":")(1).trim(), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-platform") || logContent.startsWith("X-Event-Platform"))) EventTypePlatform(logObject.requestResponseKey, logContent.split(":")(1).trim(), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-event-sequence") || logContent.startsWith("X-Event-Sequence"))) EventRequestSequence(logObject.requestResponseKey, logContent.split(":")(1).trim(), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.contains("{") || logContent.contains("<xml>")) EventHttpBody(logObject.requestResponseKey, startTime, logContent, startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset) //以json开始的标识{作为判断是否是事件体
      else if (logContent.startsWith("--->") && logContent.startsWith("---> END")) RequestEnd(logObject.requestResponseKey, startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.indexOf(":") > 0 && (logContent.startsWith("x-application-context") || logContent.startsWith("X-Application-Context"))) ResponseProfileAndId(logObject.requestResponseKey, logContent.split(":").drop(1).mkString(""), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else if (logContent.startsWith("<---") && !logContent.startsWith("<--- END")) {
        val httpResponseSplits = logContent.split(" ")
        ResponseStart(logObject.requestResponseKey, httpResponseSplits.takeRight(1)(0).substring(1, httpResponseSplits.takeRight(1)(0).indexOf("m")).toInt, httpResponseSplits(2), startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      } else if (logContent.startsWith("<---") && logContent.startsWith("<--- END"))
        ResponseEnd(logObject.requestResponseKey, startTime,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
      else
        NoVialbleLog(logObject.requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, logContent,logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)
    } else
      NoVialbleLog(logObject.requestResponseKey, clientAndProfileId._1, clientAndProfileId._2, startTime, "",logObject.kfTopic,logObject.kfPartition,logObject.kfOffset)

  }

  def changeLinToLogObject(line: ConsumerRecord[String,String]) = {

    val lineParttions = line.value().split(" ").toBuffer
    if (lineParttions.length > 7) {
      for (i <- 7 until lineParttions.length)
        lineParttions(6) = lineParttions(6) + " " + lineParttions(i)
    }
    lineParttions(1) = lineParttions(0) + " " + lineParttions(1)
    val newlinePar = lineParttions.tail
    if (newlinePar.length >= 6)
      new LogObject(getTimestamp(newlinePar(0)), newlinePar(1), newlinePar(2), Some(newlinePar(5)),line.topic(),line.partition(),line.offset())
    else
      new LogObject(getTimestamp(newlinePar(0)), newlinePar(1), newlinePar(2), None,line.topic(),line.partition(),line.offset())

  }


  //  def processRdd(rdd: RDD[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]): Unit = {
  //    val line = rdd.map(record => (record.key, record.value)).map(_._2)
  //    val logObjectRdd = line.map { x => x.split(" ") } //将日志每一行拆成数组
  //      .map { x =>
  //        {
  //          val y = x.toBuffer
  //          if (y.length > 7) {
  //            for (i <- 7 until y.length)
  //              y(6) = y(6) + " " + y(i) //把日志体再合并在一起
  //          }
  //          y(1) = y(0) + " " + y(1) //将时间合并
  //          y.remove(0)
  //          y.toArray
  //        }
  //      }.filter { x => x(5).trim().length() > 0 }
  //      .filter (x => !x(5).startsWith("Accept:") || !x(5).startsWith("Content-Type:") || !x(5).startsWith("content-type:") || !x(5).startsWith("server:"))
  //      .map { x => new LogObject(getTimestamp(x(0)), x(1), x(2), x(5)) }
  //
  //
  ////    EsSpark.saveToEs(logObjectRdd, "feiginclient/new")
  //  }

  def getTimestamp(x: Any): java.sql.Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", Locale.CHINA)
    format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    if (x.toString() == "")
      return null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      return t
    }
  }


  @SerialVersionUID(-3656453505503987988L) case class LogObject(val timestamp: Timestamp, val threadName: String, val requestResponseKey: String, val logContent: Option[String],val kfTopic:String,val kfPartition: Int, val kfOffset:Long) extends Serializable


  sealed abstract class EventComponent(requesResponseKey: String, eventTime: Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends Serializable {
    def getRequesResponseKey = requesResponseKey

    def getEventTime = eventTime
  }

  case class RequestStart(requesResponseKey: String, client: String, profileAndId: String, startTime: java.util.Date, method: String, url: String,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //请求开始

  case class EventID(requesResponseKey: String, id: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //事件ID

  case class EventCode(requesResponseKey: String, code: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //事件类型

  case class EventType(requesResponseKey: String, eventType: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //事件类型

  case class EventTypePlatform(requesResponseKey: String, platForm: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //事件平台

  case class EventRequestSequence(requesResponseKey: String, sequence: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //事件请求序列

  case class EventHttpBody(requesResponseKey: String, time: java.util.Date, body: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //请求体、返回体Json

  case class RequestEnd(requesResponseKey: String, requestEndTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = requestEndTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //请求结束

  case class ResponseStart(requesResponseKey: String, durition: Int, httpStatus: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //请求返回持续时间、状态

  case class ResponseEnd(requesResponseKey: String, endTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = endTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset) //请求返回

  case class NoVialbleLog(requesResponseKey: String, client: String, profileAndId: String, startTime: java.util.Date, notIgnoreLog: String,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset)

  case class ResponseProfileAndId(requesResponseKey: String, profileAndId: String, startTime: java.util.Date,kfTopic:String, kfPartition: Int, kfOffset:Long) extends EventComponent(requesResponseKey, eventTime = startTime,kfTopic = kfTopic,kfPartition = kfPartition,kfOffset = kfOffset)

  class OrderedEventEntity(val even: EventEntity) extends Ordered[EventEntity] {
    def compare(that: EventEntity) = try {
      even.startTime.compareTo(that.startTime)
    } catch {
      case x: Throwable =>
        log.error(s"compared event exception , this event is ${even.requesResponseKey}, that event is ${that.requesResponseKey}", x)
        throw x
    }
  }

  case class BusinessRecoder(id: String, recordtime: Date, business: String)

  case class TopicPartitionRedis(topic: String, partition: Int)

}