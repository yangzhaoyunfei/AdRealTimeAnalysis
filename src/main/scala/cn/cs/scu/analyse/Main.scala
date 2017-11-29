package cn.cs.scu.analyse


import java.util.concurrent.{ExecutorService, Executors}

import cn.cs.scu.conf.ConfigurationManager
import cn.cs.scu.constants.Constants
import cn.cs.scu.scalautils.{InitUnits, MyKafkaUtils}
import cn.cs.scu.threads.{ClickTrendThread, ProvinceTop3AdsThread, UpdateBlackListThread}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ListBuffer

/**
  * Created by zhangchi on 17/3/16.
  */


object Main {

  var blackList: ListBuffer[String] = new ListBuffer[String]

  def main(args: Array[String]): Unit = {

    val init: (SparkContext, SQLContext, StreamingContext) = InitUnits.initSparkContext()
    val ssc: StreamingContext = init._3

    ssc.checkpoint(Constants.CHECK_POINT_DIR)

    //这一步从kafka获取输入SparkStreaming的数据
    val data = MyKafkaUtils.createStream(ssc, Constants.KAFKA_ZKQUORUM, Constants.KAFKA_GROUP, Constants.KAFKA_TOPICS)

    //获取原始数据
    val originData = RealTimeAnalyse.getOriginData(ssc, data) //原始数据应该是从SparkStreaming中输出的数据
    //获取用户点击数据
    val userClickTimes = RealTimeAnalyse.countUserClickTimes(ssc, originData)
    //过滤用户点击数据
    val filteredUserClickTimes = RealTimeAnalyse.getFilteredData(userClickTimes)
    //获取新增黑名单
    var newBlackList = RealTimeAnalyse.getBlackList(filteredUserClickTimes)
    //获取广告被点击数据
    val adClickedTimes = RealTimeAnalyse.countAdClickedTimes(ssc, originData)


    //创建更新黑名单线程
    val threadPool: ExecutorService = Executors.newFixedThreadPool(Constants.THREADS_NUM)
    try {
      threadPool.execute(new UpdateBlackListThread)
      threadPool.execute(new ClickTrendThread)
      threadPool.execute(new ProvinceTop3AdsThread)
    }
    finally {
      threadPool.shutdown()
    }

    //查询各省广告点击前三
    //    val provinceTop3Ads = RealTimeAnalyse.getTop3AD
    //    for (provinceTop3Ad <- provinceTop3Ads) {
    //      println(provinceTop3Ad.toString)
    //    }

    //    val ads = RealTimeAnalyse.getClickTrend
    //    for (ad <- ads) {
    //      println(ad.getAdId + "\t" + ad.getClickDay + "\t" + ad.getClickTime + "\t" + ad.getClickNumber)
    //    }

    filteredUserClickTimes.print()
    adClickedTimes.print()
    ssc.start()
    ssc.awaitTermination()

  }
}