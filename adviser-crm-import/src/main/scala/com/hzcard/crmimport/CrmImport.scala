package com.hzcard.crmimport

import java.sql.Timestamp
import java.util.{Properties, UUID}

import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object CrmImport {

  @transient lazy val log = LogFactory.getLog(CrmImport.getClass)

  val JDBC_URL = "url"

  def main(args: Array[String]): Unit = {

    if (args.length == 0)
      throw new IllegalArgumentException("Not enough arguments: missing args name.")
    val jdbcConf = scala.collection.mutable.Map[String, String](
      JDBC_URL -> "jdbc:mysql://192.168.102.206:3306/crm?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&maxReconnects=100",
      "useUnicode" -> "true",
      "characterEncoding" -> "utf8",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root",
      "rewriteBatchedStatements" -> "true")
    val conf = new SparkConf().setAppName("crmImport")
    var parallelism: Int = 20
    var csvFile: String = ""
    var tableName: String = ""
    var partnerCode: String = ""
    for (i <- 0 to args.length - 1) {
      args(i) match {
        case x if x.startsWith("--spark.") => conf.set(x.drop(2), args(i + 1))
        case x if x.startsWith("--jdbc.") => jdbcConf += (x.drop("--jdbc.".length()) -> args(i + 1))
        case x if x.startsWith("--parallelism") => parallelism = args(i + 1).toInt
        case x if x.startsWith("--csv") => csvFile = args(i + 1).toString
        case x if x.startsWith("--tableName") => tableName = args(i + 1).toString
        case x if x.startsWith("--partnerCode") => partnerCode = args(i + 1).toString
        case _ =>
      }
    }
    if (csvFile.trim == "" || tableName.trim == "")
      throw new Exception("找不到要导入的文件或没有指定导入的表:--csv   --tableName")
    if(partnerCode.trim=="")
      throw new Exception("需要指定合作伙伴编号:--partnerCode")
    val sc = SparkSession.builder().appName("crmImport").config(conf).getOrCreate()
    val csvschema = StructType(
      List(
        StructField("partner_member_card", StringType, false),
        StructField("user_name", StringType, true),
        StructField("partner_member_id", StringType, true),
        StructField("mobile_phone", StringType, false),
        StructField("partner_point", LongType, false)))
    val toImportRdd = sc.read.schema(csvschema).csv(csvFile).collect()
    //转为rdd
    try {
      import collection.JavaConversions._
      val jdbcPro = new Properties()
      jdbcPro.putAll(jdbcConf)

      val dataBaseSchema = StructType(
        List(
          StructField("id", StringType, false),
          StructField("partner_code", StringType, true),
          StructField("generation_time", TimestampType, true),
          StructField("partner_member_card", StringType, true),
          StructField("user_name", StringType, true),
          StructField("partner_member_id", StringType, true),
          StructField("mobile_phone", StringType, true),
          StructField("partner_point", LongType, false),
          StructField("point", LongType, false)
        ))

      val rowRDD = sc.sparkContext.makeRDD(toImportRdd, parallelism).map(x => Row(UUID.randomUUID().toString,
        partnerCode,
        new Timestamp(System.currentTimeMillis()),
        x.getValuesMap(Seq("partner_member_card")).get("partner_member_card").getOrElse(""),
        x.getValuesMap(Seq("user_name")).get("user_name").getOrElse(""),
        x.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse(""),
        x.getValuesMap(Seq("mobile_phone")).get("mobile_phone").getOrElse(""),
        x.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L),
        {
          val partner_point = x.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L).asInstanceOf[Long]
          if(partner_point>=10000L)
            partner_point * 10L
          else
            partner_point * 5L
        }
      ))
      val startTime = System.currentTimeMillis()
      sc.createDataFrame(rowRDD, dataBaseSchema).
        write.mode(SaveMode.Append).
        jdbc(jdbcConf.get(JDBC_URL).get, tableName, jdbcPro)
      log.error("=============处理结束，处理持续时间：" + (System.currentTimeMillis() - startTime))

    } catch {
      case x: Throwable => {
        log.error(x)
      }
    } finally {
      sc.stop()
    }


  }
}