package com.yxBuild.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  /**
    * 读取Properties文件的数据
    *
    * @param propertiesName Properties文件名
    * @return 文件属性对象
    */
  def load(propertiesName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName) , "UTF-8"))
    prop
  }
}
