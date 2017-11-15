package cn.cs.scu.threads

import cn.cs.scu.analyse.RealTimeAnalyse

/**
  * Created by zhangchi on 17/3/19.
  */
class ProvinceTop3AdsThread extends Runnable{
  override def run(): Unit = {
    while (true){
      //查询各省广告点击前三的，数组
      val provinceTop3Ads = RealTimeAnalyse.getTop3AD
      println("_____________________________________________________________________")
      //打印上面的数组
      for (provinceTop3Ad <- provinceTop3Ads) {
        println(provinceTop3Ad.toString)
      }
      println("_____________________________________________________________________")
      Thread.sleep(5000)
    }
  }
}
