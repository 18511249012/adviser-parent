package com.hzcard.adviser

import com.hzcard.adviser.EventFlowFirstStage.RequestStart
import com.hzcard.adviser.EventFlowFirstStage.EventID
import com.hzcard.adviser.EventFlowFirstStage.EventType
import com.hzcard.adviser.EventFlowFirstStage.EventTypePlatform
import com.hzcard.adviser.EventFlowFirstStage.EventRequestSequence
import com.hzcard.adviser.EventFlowFirstStage.EventHttpBody
import com.hzcard.adviser.EventFlowFirstStage.RequestEnd
import com.hzcard.adviser.EventFlowFirstStage.ResponseStart
import com.hzcard.adviser.EventFlowFirstStage.ResponseEnd
import java.util.Date
import scala.beans.BeanProperty
import com.hzcard.adviser.EventFlowFirstStage.EventComponent
import com.hzcard.adviser.EventFlowFirstStage.EventCode
import scala.collection.mutable.ArrayBuffer
import java.io.Serializable
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonFormat
import com.hzcard.adviser.EventFlowFirstStage.NoVialbleLog
import java.util.concurrent.ConcurrentLinkedQueue

object EventEntity {
  def apply(eventCompent: EventComponent) = {
    val env = new EventEntity(eventCompent)
    env.mergerCompentToEvent(eventCompent)
  }
}

@SerialVersionUID(1L) class EventEntity private (@JsonIgnore val eventCompent: EventComponent) extends Serializable {

  def requesResponseKey: String = { eventCompent.getRequesResponseKey }

  @JsonIgnore
  private var bodyTime: Date = null

  @JsonIgnore
  private val mergedArray = new ArrayBuffer[EventComponent]

  @BeanProperty
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
  var startTime: Date = _

  @BeanProperty
  var method: String = _

  @BeanProperty
  var url: String = _

  @BeanProperty
  var eventId: String = _

  @BeanProperty
  var eventType: String = _

  @BeanProperty
  var eventCode: String = _

  @BeanProperty
  var eventPlatForm: String = _

  @BeanProperty
  var eventSequence: String = _

  @BeanProperty
  var requestBody: String = _

  @BeanProperty
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
  var requestEndTime: Date = _

  @BeanProperty
  var responeBody: String = _

  @BeanProperty
  var httpStatus: String = _

  @BeanProperty
  var durition: Int = 0

  @BeanProperty
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
  var endTime: Date = null

  @BeanProperty
  var notIgnoreLogCont: String = _

  @BeanProperty
  var client: String = _

  @BeanProperty
  var profileAndId: String = _

  @BeanProperty
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
  var responseStartTime: Date = null

  @JsonIgnore
  def nothingToDo() = {}

  @JsonIgnore
  def mergerEventEntity(other: EventEntity): EventEntity = {
    for (sComponent <- other.mergedArray) {
      this.mergerCompentToEvent(sComponent)
    }
    this
  }

  @JsonIgnore
  private def mergerCompentToEvent(inEvn: EventComponent) = {
    mergedArray += inEvn
    inEvn match {
      case x: RequestStart => {
        this.startTime = x.startTime
        this.method = x.method
        this.url = x.url
        this.client = x.client
        this.profileAndId = x.profileAndId
      }
      case x: EventID              => this.eventId = x.id
      case x: EventType            => this.eventType = x.eventType
      case x: EventCode            => this.eventCode = x.code
      case x: EventTypePlatform    => this.eventPlatForm = x.platForm
      case x: EventRequestSequence => this.eventSequence = x.sequence
      case x: EventHttpBody => {
        if (this.bodyTime == null)
          if (this.requestEndTime != null && (x.time.before(this.requestEndTime) || x.time.equals(this.requestEndTime)))
            this.requestBody = x.body
          else
            this.responeBody = x.body
        if (this.bodyTime != null)
          if (x.time.before(this.bodyTime)) {
            if (this.requestBody != null) {
              this.responeBody = this.requestBody
            }
            this.requestBody = x.body
          } else {
            if (this.responeBody != null)
              this.requestBody = this.responeBody
            this.responeBody = x.body
          }
        this.bodyTime = x.time
      }
      case x: RequestEnd => {
        this.requestEndTime = x.requestEndTime
        if (this.bodyTime != null) {
          if (this.requestBody == null) {
            if (this.bodyTime.before(this.requestEndTime) || this.bodyTime.equals(this.requestEndTime)) {
              this.requestBody = this.responeBody
              this.responeBody = null
            }
          }
          if (this.responeBody == null) {
            if (this.bodyTime.after(this.requestEndTime)) {
              this.responeBody = this.requestBody
              this.requestBody = null
            }
          }
        }
      }
      case x: ResponseStart => {
        this.durition = x.durition
        this.httpStatus = x.httpStatus
        this.responseStartTime = x.startTime
      }
      case x: ResponseEnd => {
        this.endTime = x.endTime
      }
      case x: NoVialbleLog => {
        if (this.notIgnoreLogCont != null)
          this.notIgnoreLogCont = this.notIgnoreLogCont + " ; " + x.notIgnoreLog
        else
          this.notIgnoreLogCont = x.notIgnoreLog
        this.client = x.client
        this.profileAndId = x.profileAndId
      }
      case _ =>
    }
    this
  }
}