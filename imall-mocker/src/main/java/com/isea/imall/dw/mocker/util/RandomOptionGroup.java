package com.isea.imall.dw.mocker.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *  该类的作用：形成一个T类型的大池子，这个大池中的T对象按照权重分布
 *  getRandomOpt 方法随机一个index ，然后数组[index] 以达到按权重随机的目的。
 * @param <T>
 */
public class RandomOptionGroup<T> {
    int totalWeight = 0;

    List<RanOpt> optList = new ArrayList();

    public RandomOptionGroup(RanOpt<T>... opts) {
        for (RanOpt opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }
        }
    }

    public RanOpt<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }

    public static void main(String[] args) {
        RanOpt[] opts = {new RanOpt("zhang3", 20), new RanOpt("li4", 30), new RanOpt("wang5", 50)};
        RandomOptionGroup randomOptionGroup = new RandomOptionGroup(opts);
        for (int i = 0; i < 10; i++) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }
    }
}
