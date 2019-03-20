package com.isea.imall.dw.imall.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.isea.imall.dw.common.constant.ImallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController  //  = Controller+ResponseBody
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("isea_log")  // 映射请求的URL
    //   会认为success 不是一个页面，而仅仅是个结果，就是一个字符串
    public String log(@RequestParam("log") String logJson) { // 请求的参数,注入到一个变量中，log必须是请求的时候，=左边的字符串
        // 这里是处理具体的业务->使用logger4j来专门的管理日志，将日志输出到目的地。

        System.out.println(logJson); // 在后端打印出请求传入的内容

        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());

        //离线数据发送到Kafka中
        sendToKafka(jsonObject);

        // 实时的数据发送到Logger日志服务器中
        logger.info(jsonObject.toJSONString());

        return "success..."; // 返回前端，或者是请求者的信息。多数时候这里是返回一个页面给前端。
    }

    private void sendToKafka(JSONObject jsonObject){
        // 根据logJSON中type的类型，来分到不同的topic中
        if (jsonObject.getString("type").equals("startup")){
            kafkaTemplate.send(ImallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else {
            kafkaTemplate.send(ImallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }
    }

}
