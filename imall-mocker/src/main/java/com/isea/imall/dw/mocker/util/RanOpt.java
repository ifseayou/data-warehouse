package com.isea.imall.dw.mocker.util;

/**
 * 该类的作用：将某个类型 和 该类型的权重的一个封装
 *
 * @param <T>
 */
public class RanOpt<T> {
    T value;
    int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
