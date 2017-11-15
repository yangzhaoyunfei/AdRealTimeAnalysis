package cn.cs.scu.threads

import cn.cs.scu.analyse.{Main, RealTimeAnalyse}

/**
  * Created by zhangchi on 17/3/17.
  */
//从数据库中查询黑名单，并打印
class UpdateBlackListThread extends Runnable {
  override def run(): Unit = {
    while (true) {
      Main.blackList = RealTimeAnalyse.getBlackListFromDataBase
      println(Main.blackList)
      Thread.sleep(5000)
    }
  }
}
