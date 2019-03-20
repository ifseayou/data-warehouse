package com.isea.imall.dw.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.CaseFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import com.isea.imall.dw.common.constant.ImallConstant;
import com.isea.imall.dw.common.util.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    // 该主函数开启了之后，能够一直监听canal服务器监听MySQL中数据库的变化，
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop101", 11111), "example", "", "");


        while (true) {
            // 建立canal服务器
            canalConnector.connect();

            // 订阅数据库和表
            canalConnector.subscribe("imall_mysql_one.order_info");

            // 获取发生的sql，得到message,我们这里一个message里面抓取100条SQL（但是不一定能够抓取到100条）
            Message message = canalConnector.get(100);
            System.out.println("获取到" + message.getEntries().size() + "个SQL");

            if (message.getEntries().size() == 0) {
                System.out.println("imall_mysql_one中的order_info表没有写操作");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 展开所有的SQL
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 过滤掉对数据不造成影响的SQL，是rowdata类型的才可以通过
                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                        try {
                            // 对entry反序列化，得到rowchange
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            // 从rowchange里面得到变化的行的集合
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            // 得到操作的类型：
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            // 得到表名
                            String tableName = entry.getHeader().getTableName();

                            // 将对某个表做个某个操作，受影响的行，做处理（最终发送到kafka中）
                            handle(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        // 这里捕获的是订单的新增，对于订单表的insert操作
        if ("order_info".equals(tableName) && eventType == CanalEntry.EventType.INSERT && rowDatasList.size() > 0) {
            for (CanalEntry.RowData rowData : rowDatasList) { // 展开行集
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList(); // 得到变化后列的集合
                JSONObject jsonObject = new JSONObject();
                // 展开列集
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName() + ";" + column.getValue());

                    // 做成json的格式
                    String colNameCamel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(colNameCamel, column.getValue());
                }
                // 将封装好的json串发送到Kafka中，发过去的是一行数据，
                MyKafkaSender.send(ImallConstant.KAFKA_TOPIC_ORDER, jsonObject.toJSONString());
            }
        }
    }
}
