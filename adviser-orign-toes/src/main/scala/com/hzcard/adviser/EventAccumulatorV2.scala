package com.hzcard.adviser

import java.util.concurrent.ConcurrentHashMap

import com.hzcard.adviser.OrignToEs.EventComponent
import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConverters._

@SerialVersionUID(1L) class EventAccumulatorV2 extends AccumulatorV2[EventComponent, ConcurrentHashMap[String, EventEntity]] {

  val _synchronizedMap = new ConcurrentHashMap[String, EventEntity]

  override def reset() {
    //    var seedEvent :EventEntity=  null
    //    if(!isZero()) {
    //      for (key <- _synchronizedMap.keySet().asScala) {
    //        if (_synchronizedMap.get(key) == null)
    //          _synchronizedMap.remove(key)
    //        else if (_synchronizedMap.get(key).startTime != null && _synchronizedMap.get(key).endTime != null)
    //          seedEvent = _synchronizedMap.remove(key)
    //        else if (_synchronizedMap.get(key).eventTime != null && seedEvent!=null && ((seedEvent.eventTime.getTime - _synchronizedMap.get(key).eventTime.getTime) / (1000 * 60)) > 15) { //超过种子的15分钟，为了让已经丢失的日志清理，这里只能自定义结束时间了
    //          if (_synchronizedMap.get(key).startTime == null)
    //            _synchronizedMap.get(key).customStartEventTime(_synchronizedMap.get(key).eventTime)
    //          if (_synchronizedMap.get(key).endTime == null)
    //            _synchronizedMap.get(key).customEndEventTime(_synchronizedMap.get(key).eventTime)
    //        }
    //      }
    //    }
    _synchronizedMap.clear()
  }

  override def add(inEvn: EventComponent) {
    val nEntity = EventEntity(inEvn)
    val adderSupplier = new java.util.function.BiFunction[String, EventEntity, EventEntity]() {
      override def apply(t: String, u: EventEntity): EventEntity = {
        if (u == null)
          nEntity
        else
          u.mergerEventEntity(nEntity)

      }
    }
    _synchronizedMap.compute(nEntity.requesResponseKey, adderSupplier)
  }

  override def copy(): AccumulatorV2[EventComponent, ConcurrentHashMap[String, EventEntity]] = {
    val ventAccumulatorV2 = new EventAccumulatorV2()
    ventAccumulatorV2._synchronizedMap.putAll(_synchronizedMap)
    ventAccumulatorV2
  }

  override def isZero() = {
    _synchronizedMap.isEmpty()
  }

  override def merge(other: AccumulatorV2[EventComponent, ConcurrentHashMap[String, EventEntity]]) = other match {
    case o: EventAccumulatorV2 => {
      val ite = other.value.entrySet().iterator()
      while (ite.hasNext()) {
        val oSE = ite.next();
        val adderSupplier = new java.util.function.BiFunction[String, EventEntity, EventEntity]() {
          override def apply(t: String, u: EventEntity): EventEntity = {
            if (u == null)
              oSE.getValue
            else
              u.mergerEventEntity(oSE.getValue)

          }
        }
        _synchronizedMap.compute(oSE.getKey, adderSupplier)
      }
    }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value = {
    _synchronizedMap
  }

}