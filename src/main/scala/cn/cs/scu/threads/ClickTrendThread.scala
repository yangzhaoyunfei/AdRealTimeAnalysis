package cn.cs.scu.threads

import cn.cs.scu.analyse.RealTimeAnalyse

/**
  * Created by zhangchi on 17/3/19.
  */
//获取前一小时广告各分钟点击量数组，并打印
class ClickTrendThread extends Runnable {
  override def run(): Unit = {
    while (true) {
      val ads = RealTimeAnalyse.getClickTrend
      println("_____________________________________________________________________")
      for (ad <- ads) {
        println(ad.getAdId + "\t" + ad.getClickDay + "\t" + ad.getClickTime + "\t" + ad.getClickNumber)
      }
      println("_____________________________________________________________________")
      Thread.sleep(5000)
    }
  }
}
