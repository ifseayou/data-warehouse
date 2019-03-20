package com.isea.dw.imall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.isea.dw.imall.publisher.service.PublishService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublishController {

    @Autowired
    PublishService publishService;


    // 响应 需求1 date日期活跃用户总数，新增日活的总数，当日交易额的总数 （交易订单也应该在该方法中实现，但是还没有是实现）
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){

        int dauTotalByTime = publishService.getDauTotalByTime(date);

        List<Object> list = new ArrayList<>();

        HashMap<Object, Object> dauMap = new HashMap<>();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活"); // 这里其实是截止到当时的日活用户
        dauMap.put("value",dauTotalByTime);
        list.add(dauMap);


        HashMap<Object, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",1200);
        list.add(newMidMap);

        Map newOrderTotalByHours = publishService.getNewOrderTotalByHours(date);
        HashMap<Object, Object> orderTotalMap = new HashMap<>();
        orderTotalMap.put("id","totalAmount");
        orderTotalMap.put("name","新增交易额");
        orderTotalMap.put("value",newOrderTotalByHours);
        list.add(newOrderTotalByHours);

        return JSON.toJSONString(list);
    }

    // 获取实时的数据 活跃用户的分时，新增用户的分时，订单交易额的分时，订单数的分时，并且要有昨日的对比图
    @GetMapping("realtime-hours")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){
        if ("dau".equals(id)){
            HashMap<Object, Object> dauHourMap = new HashMap<>();

            // 获得今天的每小时日活
            Map dauByHoursTodayMap = publishService.getDauByHours(date);
            Date today = null;
            try {
                today = new SimpleDateFormat("yyyy-MM-dd").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Date yesterday = DateUtils.addDays(today, -1);
            String yesterdayDate = new SimpleDateFormat("yyyy-MM-dd").format(yesterday);

            // 获取昨天的每小时日活
            Map dauByHoursYesterdayMap = publishService.getDauByHours(yesterdayDate);

            dauHourMap.put("yesterday",dauByHoursYesterdayMap);
            dauHourMap.put("today",dauByHoursTodayMap);

            return JSON.toJSONString(dauHourMap);
        }else if ("totalamount".equals(id)){
            HashMap<Object, Object> totalamountHoursMap = new HashMap<>();

            // 获取今天每个小时的交易额
            Map totalamountHoursTodayMap = publishService.getNewOrderTotalByHours(date);

            // 将日期变为昨天
            Date today = null;
            try {
                today = new SimpleDateFormat("yyyy-MM-dd").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Date yesterday = DateUtils.addDays(today, -1);
            String yesterdayDate = new SimpleDateFormat("yyyy-MM-dd").format(yesterday);

            // 获取昨天每个小时的交易额
            Map totalamountHoursYesterdayMap = publishService.getNewOrderTotalByHours(yesterdayDate);

            totalamountHoursMap.put("yesterday",totalamountHoursYesterdayMap);
            totalamountHoursMap.put("today",totalamountHoursTodayMap);
            return JSON.toJSONString(totalamountHoursMap.toString());
        }
        return null;
    }
}
