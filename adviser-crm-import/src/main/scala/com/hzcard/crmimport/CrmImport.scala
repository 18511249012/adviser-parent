package com.hzcard.crmimport

import java.sql.Timestamp
import java.util.Properties

import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, TimestampType, _}
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
    if (partnerCode.trim == "")
      throw new Exception("需要指定合作伙伴编号:--partnerCode")
    val sc = SparkSession.builder().appName("crmImport").config(conf).getOrCreate()
    val csvschema = StructType(
      List(
        StructField("partner_member_card", StringType, false),
        StructField("user_name", StringType, true),
        StructField("partner_member_id", StringType, true),
        StructField("mobile_phone", StringType, false),
        StructField("partner_point", LongType, false),
        StructField("partner_change", StringType, false)))
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
          StructField("point", LongType, false),
          StructField("partner_change", StringType, false),
          StructField("idcard_no", StringType, true),
          StructField("unionId", StringType, true),
          StructField("convert_status",LongType, true)
        ))

      val rowRDD = sc.sparkContext.makeRDD(toImportRdd, parallelism).map(x => Row(
        x.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse(""), //使用partner_member_id作为主键
        partnerCode,
        new Timestamp(System.currentTimeMillis()),
        x.getValuesMap(Seq("partner_member_card")).get("partner_member_card").getOrElse(""),
        x.getValuesMap(Seq("user_name")).get("user_name").getOrElse(""),
        x.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse(""),
        x.getValuesMap(Seq("mobile_phone")).get("mobile_phone").getOrElse(""),
        x.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L),
        {
          val partner_point = x.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L).asInstanceOf[Long]
          if (partner_point >= 10000L)
            partner_point * 10L
          else
            partner_point * 5L
        },
        x.getValuesMap(Seq("partner_change")).get("partner_change").getOrElse(""),
        "", "",0
      ))
      val startTime = System.currentTimeMillis()
      val oldData = sc.createDataFrame(sc.sparkContext.makeRDD(sc.read.jdbc(jdbcConf.get(JDBC_URL).get, tableName, jdbcPro).limit(10000000).collect().map(x=> Row(
        x.getValuesMap(Seq("id")).get("id").getOrElse(""), //使用partner_member_id作为主键
        x.getValuesMap(Seq("partner_code")).get("partner_code").getOrElse(""),
        Timestamp.valueOf(x.getValuesMap(Seq("generation_time")).get("generation_time").get.toString),
        x.getValuesMap(Seq("partner_member_card")).get("partner_member_card").getOrElse(""),
        x.getValuesMap(Seq("user_name")).get("user_name").getOrElse(""),
        x.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse(""),
        x.getValuesMap(Seq("mobile_phone")).get("mobile_phone").getOrElse(""),
        x.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L),
        x.getValuesMap(Seq("point")).get("point").getOrElse(0L) ,
        x.getValuesMap(Seq("partner_change")).get("partner_change").getOrElse("") ,
        x.getValuesMap(Seq("idcard_no")).get("idcard_no").getOrElse(""),
        x.getValuesMap(Seq("unionId")).get("unionId").getOrElse(""),
        x.getValuesMap(Seq("convert_status")).get("convert_status").getOrElse(0L)
      )), parallelism), dataBaseSchema)
      val newData = sc.createDataFrame(rowRDD, dataBaseSchema)
//      log.error(s"oldData = ${oldData.collect().length}")
//      log.error(s"newData = ${newData.collect().length}")
//      val test = newData.union(oldData).collect()
//      test.foreach(x => log.error(s"test = ${test.length}"))

//      test2.foreach(x => x._2.foreach(y => log.error(s"test2 array = ${y.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse("")}")))
      //合并，分组，并取新的rdd
      val merged = newData.union(oldData).collect().
        groupBy(x => x.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse("")).
        flatMap(x => if (x._2.size > 1) {
          val newImportData = x._2.take(1).take(1)(0)
          val oldImportData = x._2.drop(1).take(1)(0)
          Array(Row(newImportData.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse(""),
            newImportData.getValuesMap(Seq("partner_code")).get("partner_code").getOrElse(""),
            new Timestamp(System.currentTimeMillis()),
            newImportData.getValuesMap(Seq("partner_member_card")).get("partner_member_card").getOrElse(""),
            newImportData.getValuesMap(Seq("user_name")).get("user_name").getOrElse(""),
            newImportData.getValuesMap(Seq("partner_member_id")).get("partner_member_id").getOrElse(""),
            newImportData.getValuesMap(Seq("mobile_phone")).get("mobile_phone").getOrElse(""),
            newImportData.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L),
             {
              val partner_point = newImportData.getValuesMap(Seq("partner_point")).get("partner_point").getOrElse(0L).asInstanceOf[Long]
              if (partner_point >= 10000L)
                partner_point * 10L
              else
                partner_point * 5L
            },
            newImportData.getValuesMap(Seq("partner_change")).get("partner_change").getOrElse(""),
            oldImportData.getValuesMap(Seq("idcard_no")).get("idcard_no").getOrElse(""),
            oldImportData.getValuesMap(Seq("unionId")).get("unionId").getOrElse(""),
            oldImportData.getValuesMap(Seq("convert_status")).get("convert_status").getOrElse(0L))
          )
        } else x._2).toArray
      //      oldData.collect().filter(x=>x.getValuesMap(Seq("partner_member_id")).get("partner_member_id").get)
//      val test2 = merged.foreach(x => log.error(s"=============generation_time is =${x.getValuesMap(Seq("generation_time")).get("generation_time").get}"))
//      log.error("=============merged is =======")
      sc.createDataFrame(sc.sparkContext.makeRDD(merged, parallelism), dataBaseSchema).write.mode(SaveMode.Overwrite).
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
