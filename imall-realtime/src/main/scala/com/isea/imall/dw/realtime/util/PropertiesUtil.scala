package com.isea.imall.dw.realtime.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertiesName : String): Properties = {
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader
    .getResourceAsStream(propertiesName),"UTF-8"))
    properties
  }
}
