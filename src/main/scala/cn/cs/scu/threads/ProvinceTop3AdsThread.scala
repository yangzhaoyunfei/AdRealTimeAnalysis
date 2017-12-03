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
      println("____________________________从数据库查询并计算出各省广告点击前三表，一个前广告前三对象数组，5s一次:start_________________________________________")
      //打印上面的数组
      for (provinceTop3Ad <- provinceTop3Ads) {
        println(provinceTop3Ad.toString)
      }
      println("____________________________从数据库查询并计算出各省广告点击前三表，一个前广告前三对象数组，5s一次:end_________________________________________")
      Thread.sleep(5000)
    }
  }
}
