package com.isea.dw.imall.publisher.service.impl;

import com.isea.dw.imall.publisher.service.PublishService;
import com.isea.imall.dw.common.constant.ImallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublishServiceImpl implements PublishService {

    @Autowired
    JestClient jestClient; // 访问的是在application.properties配置文件中的es服务器

    // 根据date日期，获取每天活跃用户的总数
    @Override
    public int getDauTotalByTime(String date) {

        int total = 0;

        // 封装DSL
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("logDate", date);

        boolQueryBuilder.filter(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(ImallConstant.ES_INDEX_DAU).addType(ImallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("total:" + total);
        return total;
    }

    // 计算date当天，每个小时的活跃用户
    @Override
    public Map getDauByHours(String date){
        HashMap<Object, Object> dauHourMap = new HashMap<>();


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("logDate", date);
        boolQueryBuilder.filter(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour.keyword").size(24);
        searchSourceBuilder.aggregation(termsBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(ImallConstant.ES_INDEX_DAU).addType(ImallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> entryList = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry entry : entryList) {
                dauHourMap.put(entry.getKey(),entry.getCount());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }



    // 计算 date当天的很多的数据每小时的新增订单，返回Map
    @Override
    public Map getNewOrderTotalByHours(String date) {

        HashMap<Object, Object> totalAmountHourMap = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤部分
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new MatchQueryBuilder("createDate",date));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合部分
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_creteHour").field("createHour").size(24);
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");

        // 子聚合
        termsBuilder.subAggregation(sumBuilder);

        searchSourceBuilder.aggregation(termsBuilder);
        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(ImallConstant.ES_INDEX_NEW_ORDER).addType(ImallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Double sumTotalAmount = bucket.getSumAggregation("sum_totalamount").getSum();
                totalAmountHourMap.put(key,sumTotalAmount);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(totalAmountHourMap.toString());

        return totalAmountHourMap;
    }

    // 求date当天的订单数量，这里只是需要将当天每个小时的数据累加起来
    @Override
    public Double getNewOrderTotalByDate(String date){
        Map map = getNewOrderTotalByHours(date);
         Double allTotalAmount = 0D;
        for (Object amount : map.values()) {
            Double totalAmount = (Double) amount;
            allTotalAmount += totalAmount;
        }
            return allTotalAmount;
    }
}
