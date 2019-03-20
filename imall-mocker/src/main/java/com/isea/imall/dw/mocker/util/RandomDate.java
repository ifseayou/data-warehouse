package com.isea.imall.dw.mocker.util;

import java.util.Date;
import java.util.Random;

/**
 * 该类的功能，传入一个日期起始时间，和一个日期终止时间，返回一个这个时间范围内的随机日期时间
 */
public class RandomDate {

    Long logDateTime = 0L;//
    int maxTimeStep = 0;

    public RandomDate(Date startDate, Date endDate, int num) {

        Long avgStepTime = (endDate.getTime() - startDate.getTime()) / num;
        this.maxTimeStep = avgStepTime.intValue() * 2;
        this.logDateTime = startDate.getTime();

    }

    public Date getRandomDate() {
        int timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime + timeStep;
        return new Date(logDateTime);
    }

}
