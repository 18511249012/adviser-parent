package com.hzcard.adviser

import redis.clients.jedis.Jedis
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.util.Properties
import scala.beans.BeanProperty
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import java.util.concurrent.TimeUnit
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import org.apache.spark.sql.types.TimestampType
import java.sql.Timestamp
import java.util.Date
import redis.clients.jedis.JedisShardInfo

object ExchangeImport {

  @transient lazy val log = LogFactory.getLog(ExchangeImport.getClass)

  val REDIS_HOST = "host"

  val REDIS_PORT = "port"

  val REDIS_PASSWORD = "password"

  val REDIS_DATABASE = "dataBase"
  val REDIS_CONNECTIONTIMEOUT = "connTimeOut"
  val REDIS_SOTIMEOUT = "soTimeOut"

  val JDBC_URL = "url"

  def main(args: Array[String]): Unit = {

    if (args.length == 0)
      throw new IllegalArgumentException("Not enough arguments: missing args name.")

    val redisConf = scala.collection.mutable.Map[String, String](
      REDIS_HOST -> "192.168.102.206",
      REDIS_PORT -> "6379",
      REDIS_CONNECTIONTIMEOUT -> "2000",
      REDIS_SOTIMEOUT -> "2000")
    val jdbcConf = scala.collection.mutable.Map[String, String](
      JDBC_URL -> "jdbc:mysql://192.168.102.206:3306/exchange?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&maxReconnects=100",
      "useUnicode" -> "true",
      "characterEncoding" -> "utf8",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root",
      "rewriteBatchedStatements" -> "true")
    val conf = new SparkConf().setAppName("exchangeImport")

    var parallelism: Int = 20
    for (i <- 0 to args.length - 1) {
      args(i) match {
        case x if x.startsWith("--redis.")      => redisConf += (x.drop("--redis.".length()) -> args(i + 1))
        case x if x.startsWith("--spark.")      => conf.set(x.drop(2), args(i + 1))
        case x if x.startsWith("--jdbc.")       => jdbcConf += (x.drop("--jdbc.".length()) -> args(i + 1))
        case x if x.startsWith("--parallelism") => parallelism = args(i + 1).toInt
        case _                                  =>
      }
    }
    val jedisShardInfo = new JedisShardInfo(redisConf.get(REDIS_HOST).get, redisConf.get(REDIS_PORT).get.toInt)
    if (!redisConf.get(REDIS_PASSWORD).isEmpty)
      jedisShardInfo.setPassword(redisConf.get(REDIS_PASSWORD).get)

    jedisShardInfo.setConnectionTimeout(redisConf.get(REDIS_CONNECTIONTIMEOUT).get.toInt)
    jedisShardInfo.setSoTimeout(redisConf.get(REDIS_SOTIMEOUT).get.toInt)

    val sc = SparkSession.builder().appName("exchangeImport").config(conf).getOrCreate()
    while (true) {
      //读取redis里 key的数据
      val redisClient = jedisShardInfo.createResource()
      redisClient.pipelined()
      redisClient.select(redisConf.get(REDIS_DATABASE).getOrElse("0").toInt); //new Jedis(redisConf.get(REDIS_HOST).get, redisConf.get(REDIS_PORT).get.toInt)
      val redisForRedyValue = redisClient.get("redeem_notify_start")
      import com.fasterxml.jackson.databind.ObjectMapper
      val mapper = new ObjectMapper()
      import com.fasterxml.jackson.module.scala.DefaultScalaModule
      mapper.registerModule(DefaultScalaModule)
      if (redisForRedyValue != null && redisForRedyValue.trim().length() > 0) {
        val toRedy = {
          try {
            mapper.readValue(redisForRedyValue, classOf[SaveRedeemObj])
          } catch {
            case x: Throwable => {
              log.error(x)
              redisClient.set("redeem_notify_end_", mapper.writeValueAsString(SparkResult(false, x.getMessage)))
              redisClient.del("redeem_notify_start")
              null
            }
          }
        }
        //转为rdd
        try {
          if (toRedy != null) {
            val value = redisClient.get(toRedy.redeemSaveVO.redisKey);
            if (value != null && value.trim().length() > 0) {
              val redeemCodes = mapper.readValue(value, classOf[Array[OrignRedeemCode]]).
                map { x => RedeemCode(x.code, x.pin, x.redeemed, x.used, x.code + toRedy.redeemSaveVO.productRedeemCode, toRedy.redeemSaveVO.partnerCode) }

              val toImportRdd = redeemCodes.toSeq
              log.error("===========toImportRdd " + toImportRdd.size)
              import collection.JavaConversions._
              val jdbcPro = new Properties()
              jdbcPro.putAll(jdbcConf)
              val schema = StructType(
                List(
                  StructField("id", StringType, false),
                  StructField("code", StringType, true),
                  StructField("pin", StringType, true),
                  StructField("partner_id", StringType, false),
                  StructField("partner_code", StringType, false),
                  StructField("product_redeem_code", StringType, false),
                  StructField("batch_id", StringType, false),
                  StructField("redeemed", IntegerType, true),
                  StructField("used", IntegerType, true),
                  StructField("generate_time", TimestampType, false),
                  StructField("task_redeem_id", StringType, false),
                  StructField("deleted", IntegerType, false)))
              val rowRDD = sc.sparkContext.makeRDD(toImportRdd, parallelism).
                map(p => Row(p.id, p.code, p.pin, toRedy.redeemSaveVO.partnerId, toRedy.redeemSaveVO.partnerCode,
                  toRedy.redeemSaveVO.productRedeemCode, toRedy.taskRedeem.batchId, p.redeemed.toInt, p.used.toInt,
                  new Timestamp(System.currentTimeMillis()), toRedy.taskRedeem.id, 0))

              val startTime = System.currentTimeMillis()
              sc.createDataFrame(rowRDD, schema).
                write.mode(SaveMode.Append).
                jdbc(jdbcConf.get(JDBC_URL).get, "re_sequence", jdbcPro)
              log.error("=============处理结束，处理持续时间：" + (System.currentTimeMillis() - startTime))
              redisClient.set("redeem_notify_end_" + toRedy.taskRedeem.id, mapper.writeValueAsString(SparkResult(true, "")))
            } else {
              TimeUnit.SECONDS.sleep(2)
              //使用spark sql 写mysql数据库
              redisClient.del("redeem_notify_start")
            }
          }
        } catch {
          case x: Throwable => {
            log.error(x)
            redisClient.set("redeem_notify_end_" + toRedy.taskRedeem.id, mapper.writeValueAsString(SparkResult(false, x.getMessage)))
          }
        } finally {
          if (toRedy != null && toRedy.redeemSaveVO!=null && toRedy.redeemSaveVO.redisKey!=null)
            redisClient.del(toRedy.redeemSaveVO.redisKey);
          redisClient.del("redeem_notify_start")
          redisClient.close()
        }
      } else
        redisClient.close()
    }
    sc.stop()
  }

  case class TaskRedeem(id: String, paramjson: String, enableFlag: Int, redeemType: String, startTime: Long, batchId: String)

  case class RedeemSaveVO(partnerId: String, partnerCode: String, productRedeemCode: String, beginDate: Long, endDate: Long, operateId: String, operateName: String, operationComment: String, redisFlag: String, redisKey: String, amountReorder: String)

  case class SaveRedeemObj(mes: String, taskRedeem: TaskRedeem, redeemSaveVO: RedeemSaveVO)

  case class OrignRedeemCode(code: String, pin: String, redeemed: String, used: String)

  case class RedeemCode(code: String, pin: String, redeemed: String, used: String, id: String, partnerCode: String)

  case class SparkResult(result: Boolean, errmsg: String)
}