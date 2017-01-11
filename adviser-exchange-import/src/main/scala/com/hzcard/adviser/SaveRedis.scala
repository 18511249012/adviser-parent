package com.hzcard.adviser

import scala.beans.BeanProperty
import java.util.ArrayList
import redis.clients.jedis.Jedis
import java.util.concurrent.TimeUnit
import com.hzcard.adviser.ExchangeImport._

object SaveRedis {
  def main(args: Array[String]): Unit = {
    //    val redeemVO = new RedeemSaveVO();
    //		redeemVO.setPartnerId("e22cfccb-4f74-4b16-aca4-18d9b72005c7");	// 合作伙伴ID
    //		redeemVO.setPartnerCode("ZSY3");	// 合作伙伴Code
    //		redeemVO.setProductRedeemCode("1470819594559");	// 商品兑换编号
    //		redeemVO.setBeginDate(DateUtil.getDayAfter(new Date(), -1));	// 兑换起始时间
    //		redeemVO.setEndDate(DateUtil.getDayAfter(new Date(), 60));	// 兑换结束时间
    //		redeemVO.setOperateId("1123");	// 操作renID
    //		redeemVO.setOperateName("delong"); // 操作人名称

    val sequenceList = new ArrayList[SequenceVo]();
    for (i <- 0 to 100000) {
      val sequenceVo = new SequenceVo();
      sequenceVo.setCode("EAYI000" + i); // 兑换码
      sequenceVo.setRedeemed(0); // 兑换状态
      sequenceVo.setUsed(0); // 使用状态
      sequenceList.add(sequenceVo);

    }
    //		redeemVO.setSequenceList(sequenceList);
    val key = "12345678901234567890";
    val redisClient = new Jedis("192.168.102.206", 6379)
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    mapper.registerModule(DefaultScalaModule)
    redisClient.set(key, mapper.writeValueAsString(sequenceList));
    

//    val obj = SaveRedeemObj(, 
//          TaskRedeem("49f4cdd7d696493ea2ff63b566f17794"), RedeemSaveVO("12345678901234567890"))
//     redisClient.set("redeem_notify_start",mapper.writeValueAsString(obj))
    redisClient.close()
  }
  class SequenceVo {
    @BeanProperty var code: String = _
    @BeanProperty var redeemed: Int = _
    @BeanProperty var used: Int = _
  }
}