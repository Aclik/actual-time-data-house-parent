package com.yxBuild

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  /**
    *  将时间戳格式化为：年-月-日 时:分
    *
    * @param timestamp 时间戳
    * @return
    */
  def toDateStrByTimestamp(timestamp:String) : String = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date("1558426649764".toLong))
  }
}
