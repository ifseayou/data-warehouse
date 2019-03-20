package com.isea.imall.dw.mocker;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.isea.imall.dw.mocker.sender.LogUploader;
import com.isea.imall.dw.mocker.util.RanOpt;
import com.isea.imall.dw.mocker.util.RandomDate;
import com.isea.imall.dw.mocker.util.RandomNum;
import com.isea.imall.dw.mocker.util.RandomOptionGroup;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

// mocker 是模仿者的意思


public class JsonMocker {
    int startupNum = 100000;
    int eventNum = 200000;

    RandomDate logDateUtil = null;

    // 操作系统权重随机生成器
    RanOpt[] osOpts = {new RanOpt("ios", 3), new RanOpt("andriod", 7)};
    RandomOptionGroup<String> osOptionGroup = new RandomOptionGroup(osOpts);
    Date startTime = null;
    Date endTime = null;

    // 地区权重随机生产器
    RanOpt[] areaOpts = {new RanOpt("beijing", 10),
            new RanOpt("shanghai", 10), new RanOpt("guangdong", 20), new RanOpt("hebei", 5),
            new RanOpt("heilongjiang", 5), new RanOpt("shandong", 5), new RanOpt("tianjin", 5),
            new RanOpt("shan3xi", 5), new RanOpt("shan1xi", 5), new RanOpt("sichuan", 5)
    };
    RandomOptionGroup<String> areaOptionGroup = new RandomOptionGroup(areaOpts);

    String appId = "imall";

    // 版本权重随机生成器
    RanOpt[] vsOpts = {new RanOpt("1.2.0", 50), new RanOpt("1.1.2", 15),
            new RanOpt("1.1.3", 30),
            new RanOpt("1.1.1", 5)
    };
    RandomOptionGroup<String> vsOptionGroup = new RandomOptionGroup(vsOpts);

    // 时间权重随机生成器
    RanOpt[] eventOpts = {new RanOpt("addFavor", 10), new RanOpt("addComment", 30),
            new RanOpt("addCart", 20), new RanOpt("clickItem", 40)
    };
    RandomOptionGroup<String> eventOptionGroup = new RandomOptionGroup(eventOpts);

    // 渠道权重随机生成器
    RanOpt[] channelOpts = {new RanOpt("xiaomi", 10), new RanOpt("huawei", 20),
            new RanOpt("wandoujia", 30), new RanOpt("360", 20), new RanOpt("tencent", 20)
            , new RanOpt("baidu", 10), new RanOpt("website", 10)
    };
    RandomOptionGroup<String> channelOptionGroup = new RandomOptionGroup(channelOpts);

    // 是否退出权重随机生成器
    RanOpt[] quitOpts = {new RanOpt(true, 40), new RanOpt(false, 60)};
    RandomOptionGroup<Boolean> isQuitGroup = new RandomOptionGroup(quitOpts);

    public JsonMocker() {

    }

    public JsonMocker(String startTimeString, String endTimeString, int startupNum, int eventNum) {
        try {
            startTime = new SimpleDateFormat("yyyy-MM-dd").parse(startTimeString);
            endTime = new SimpleDateFormat("yyyy-MM-dd").parse(endTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logDateUtil = new RandomDate(startTime, endTime, startupNum + eventNum);
    }

    String initEventLog(String startLogJson) {
            /*`type` string   COMMENT '日志类型',
             `mid` string COMMENT '设备唯一 表示',
            `uid` string COMMENT '用户标识',
            `os` string COMMENT '操作系统',
            `appid` string COMMENT '应用id',
            `area` string COMMENT '地区' ,
            `evid` string COMMENT '事件id',
            `pgid` string COMMENT '当前页',
            `npgid` string COMMENT '跳转页',
            `itemid` string COMMENT '商品编号',
            `ts` bigint COMMENT '时间',*/

        JSONObject startLog = JSON.parseObject(startLogJson);
        String mid = startLog.getString("mid");
        String uid = startLog.getString("uid");
        String os = startLog.getString("os");
        String appid = this.appId;
        String area = startLog.getString("area");
        String evid = eventOptionGroup.getRandomOpt().getValue();
        int pgid = new Random().nextInt(50) + 1;
        int npgid = new Random().nextInt(50) + 1;
        int itemid = new Random().nextInt(50);
        //  long ts= logDateUtil.getRandomDate().getTime();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "event");
        jsonObject.put("mid", mid);
        jsonObject.put("uid", uid);
        jsonObject.put("os", os);
        jsonObject.put("appid", appid);
        jsonObject.put("area", area);
        jsonObject.put("evid", evid);
        jsonObject.put("pgid", pgid);
        jsonObject.put("npgid", npgid);
        jsonObject.put("itemid", itemid);
        return jsonObject.toJSONString();
    }


    String initStartupLog() {
   /* `type` string   COMMENT '日志类型' :startup   event,
      `mid` string COMMENT '设备唯一标识',
      `uid` string COMMENT '用户标识',
      `os` string COMMENT '操作系统', ,
      `appId` string COMMENT '应用id', ,
      `vs` string COMMENT '版本号',
      `ts` bigint COMMENT '启动时间',
      `area` string COMMENT '地区'
      `ch` string COMMENT '渠道'
     */


        String mid = "mid_" + RandomNum.getRandInt(400, 600);
        String uid = "" + RandomNum.getRandInt(1, 500);
        String os = osOptionGroup.getRandomOpt().getValue();
        String appid = this.appId;
        String area = areaOptionGroup.getRandomOpt().getValue();
        String vs = vsOptionGroup.getRandomOpt().getValue();
        //long ts= logDateUtil.getRandomDate().getTime(); 该时间统一使用服务器的时间
        String ch = os.equals("ios") ? "appstore" : channelOptionGroup.getRandomOpt().getValue();


        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "startup");
        jsonObject.put("mid", mid);
        jsonObject.put("uid", uid);
        jsonObject.put("os", os);
        jsonObject.put("appid", appid);
        jsonObject.put("area", area);
        jsonObject.put("ch", ch);
        jsonObject.put("vs", vs);
        return jsonObject.toJSONString();
    }

    /**
     * 生成日志
     */
    public static void genLog() {
        JsonMocker jsonMocker = new JsonMocker();
        jsonMocker.startupNum = 1000000;
        for (int i = 0; i < jsonMocker.startupNum; i++) {
            // 初始化一条启动日志，并发送到日志服务器
            String startupLog = jsonMocker.initStartupLog();
            jsonMocker.sendLog(startupLog);
            while (jsonMocker.isQuitGroup.getRandomOpt().getValue()) {
                // 在启动了之后，初始化一条事件日志，并发送到日志服务器
                String eventLog = jsonMocker.initEventLog(startupLog);
                jsonMocker.sendLog(eventLog);
            }
        }
    }


    /**
     * 发送日志到日志服务器
     * @param log 待发送的日志
     */
    public void sendLog(String log) {
        LogUploader.sendLogStream(log);
    }


    public static void main(String[] args) {
        genLog();
    }

    /**
     *  在日志服务器添加了时间信息之后，日志的信息是这样的：
     *
     * {"area":"shan1xi","uid":"309","itemid":22,"npgid":44,"evid":"addFavor","os":"ios","pgid":3,"appid":"imall","mid":"mid_213","type":"event","ts":1550823573057}
     * {"area":"tianjin","uid":"485","os":"andriod","ch":"website","appid":"imall","mid":"mid_116","type":"startup","vs":"1.1.3","ts":1550823573059}
     */
}
