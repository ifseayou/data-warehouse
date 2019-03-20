package com.isea.imall.dw.realtime.bean

// 写在构造函数中的参数，如果没有加任何的东西，就认为是val的，仍是私有的，但是可以读，
// 如果被声明成了var，那么还是私有的属性，但是可读可写
case class StartupLog(
                       mid: String,
                       appid: String,
                       area: String,
                       os: String,
                       ch: String,
                       logType: String,
                       vs: String,
                       var logDate: String,
                       var logHour: String,
                       var logHourMinute: String,
                       var ts: Long
                     ) {
}
