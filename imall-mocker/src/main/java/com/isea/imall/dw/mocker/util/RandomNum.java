package com.isea.imall.dw.mocker.util;

import java.util.Random;

public class RandomNum {
    /**
     * @param fromNum 数字的起始
     * @param toNum   数字的终止
     * @return 返回从起始开始到终止结束的数字范围 包含fromNum 和 toNum，
     */
    public static final int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}
