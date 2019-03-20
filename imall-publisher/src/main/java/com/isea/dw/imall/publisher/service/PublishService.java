package com.isea.dw.imall.publisher.service;

import java.util.Map;

public interface PublishService {

    // 根据时间获取date那天的全天活跃用户
    public int getDauTotalByTime(String date);

    // 根据时间获取data那天每个小时的活跃用户
    public Map getDauByHours(String date);

    // 根据时间获取date那天每个小时的新订单
    public Map getNewOrderTotalByHours(String date);

    public Double getNewOrderTotalByDate(String date);
}
