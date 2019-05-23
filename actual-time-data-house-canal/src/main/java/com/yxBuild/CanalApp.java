package com.yxBuild;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yxBuild.handler.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {
        // 1、创建Canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop103", 1),
                "example",
                "",
                ""
        );
        //  2、创建循环监控MySQL数据表的变化
        while (true) {
            // 2.1、连接Canal
            canalConnector.connect();
            // 2.2、"订阅"需要监控的数据表
            canalConnector.subscribe("gmall.order_info");
            // 2.3、 获取Message,基本单位为Event
            Message message = canalConnector.get(100);
            // 2.4、获取entry集合
            List<CanalEntry.Entry> entries = message.getEntries();
            // 2.5、判断entry集合是否为空
            if (entries.isEmpty()) {
                System.out.println("没有数据,休息5秒!");
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                // 2.6、处理监听的消息
                for (CanalEntry.Entry entry : entries) {
                    // 定义存储改变行记录变量
                    CanalEntry.RowChange rowChange = null;
                    try {
                        rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    // 提取行集
                    List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    String tableName = entry.getHeader().getTableName();
                    //执行业务方法
                    CanalHandler.handle(tableName,eventType,rowDataList);
                }
            }


        }
    }
}
