package cn.cs.scu.scalautils

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/** Streaming
  * Created by zhangchi on 17/3/15.
  * 创建
  */
object MyKafkaUtils {

  def createStream(//参数列表
                   ssc: StreamingContext,
                   zkQuorum: String,
                   group: String,
                   topics: String,
                   numThreads: Int = 2,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
                  ): ReceiverInputDStream[(String, String)] = {

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, storageLevel)
  }

}
