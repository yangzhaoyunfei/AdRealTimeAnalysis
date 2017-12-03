package cn.cs.scu.scalautils

import java.text.SimpleDateFormat

/**
  * Created by zhangchi on 17/3/16.
  * //对传进来的time进行不同的格式化，并返回格式化后的字符串
  */
class DateUtils(time: Long) {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  val date: String = sdf.format(time)

  def getDate:String = {

    date

  }

}

object DateUtils{
//将传入的time(ms)格式化成“年月日”并返回
  def getDate(time: Long):String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    sdf.format(time)

  }
  //将传入的time(ms)格式化成“时分秒”并返回
  def getTime(time:Long):String = {

    val sdf = new SimpleDateFormat("HH:mm:ss")

    sdf.format(time)

  }
  //将传入的time格式化成“年月日时分”并返回
  def getMinute(time:Long):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    sdf.format(time)
  }

}