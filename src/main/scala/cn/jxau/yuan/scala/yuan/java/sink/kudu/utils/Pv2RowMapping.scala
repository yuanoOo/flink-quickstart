package cn.jxau.yuan.scala.yuan.java.sink.kudu.utils

import java.util.UUID

import kuduNoTuple.Utils.RowSerializable
import org.apache.flink.api.common.functions.MapFunction
import org.joda.time
import suishen.message.event.define.PVEvent

/**
  * @author zhaomingyuan
  * @date 18-9-18
  * @time 下午3:43
  */
class Pv2RowMapping extends MapFunction[PVEvent.Entity, RowSerializable] {

    override def map(event: PVEvent.Entity): RowSerializable = {
        val dateTime = new time.DateTime(event.getNginxTimeMs)
        val nginxDate = dateTime.toString("yyyyMMdd")
        val nginxHour = dateTime.toString("HH")
        val rowSerializable = new RowSerializable(59)
        rowSerializable.setField(0, nginxDate)
        rowSerializable.setField(1, event.getEventId)
        rowSerializable.setField(2, nginxHour)
        rowSerializable.setField(3, event.getNginxTimeMs)
        rowSerializable.setField(4, event.getAppKey)
        rowSerializable.setField(5, "device_id" + UUID.randomUUID())
        rowSerializable.setField(6, event.getPublish)
        rowSerializable.setField(7, event.getImei)
        rowSerializable.setField(8, event.getMac)
        rowSerializable.setField(9, event.getImsi)
        rowSerializable.setField(10, event.getIdfa)
        rowSerializable.setField(11, event.getUid)
        rowSerializable.setField(12, event.getLat)
        rowSerializable.setField(13, event.getLon)
        rowSerializable.setField(14, "北京")
        rowSerializable.setField(15, "北京")
        rowSerializable.setField(16, event.getCityKey)
        rowSerializable.setField(17, event.getOs)
        rowSerializable.setField(18, event.getOsVersion)
        rowSerializable.setField(19, event.getPkg)
        rowSerializable.setField(20, event.getAppVersionCode)
        rowSerializable.setField(21, event.getSdkVersion)
        rowSerializable.setField(22, event.getAppVersion)
        rowSerializable.setField(23, "212121")
        rowSerializable.setField(24, "212121")
        rowSerializable.setField(25, event.getNetwork)
        rowSerializable.setField(26, event.getCountry)
        rowSerializable.setField(27, event.getDeviceSpec)
        rowSerializable.setField(28, event.getTimeZone)
        rowSerializable.setField(29, event.getServiceProvider)
        rowSerializable.setField(30, event.getLanguage)
        rowSerializable.setField(31, event.getChannel)
        rowSerializable.setField(32, event.getEvent)
        rowSerializable.setField(33, event.getEventTimeMs)
        rowSerializable.setField(34, event.getContentId)
        rowSerializable.setField(35, event.getContentModel)
        rowSerializable.setField(36, "cm")
        rowSerializable.setField(37, "cm")
        rowSerializable.setField(38, "cm")
        rowSerializable.setField(39, "cm")
        rowSerializable.setField(40, "cm")
        rowSerializable.setField(41, event.getPosition)
        rowSerializable.setField(42, event.getModule)
        rowSerializable.setField(43, event.getStartNo)
        rowSerializable.setField(44, event.getArgs)
        rowSerializable.setField(45, "arg")
        rowSerializable.setField(46, "arg")
        rowSerializable.setField(47, "arg")
        rowSerializable.setField(48, "arg")
        rowSerializable.setField(49, "arg")
        rowSerializable.setField(50, "arg")
        rowSerializable.setField(51, "arg")
        rowSerializable.setField(52, "arg")
        rowSerializable.setField(53, "arg")
        rowSerializable.setField(54, event.getClientIp)
        rowSerializable.setField(55, event.getUserAgent)
        rowSerializable.setField(56, event.getX3D)
        rowSerializable.setField(57, event.getY3D)
        rowSerializable.setField(58, event.getZ3D)
        rowSerializable
    }
}
