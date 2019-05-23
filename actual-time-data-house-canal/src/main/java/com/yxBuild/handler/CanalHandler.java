package com.yxBuild.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.yxBuild.util.MyKafkaSender;
import yxBuild.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    /**
     * 执行业务处理
     *
     * @param tableName
     * @param eventType
     * @param rowDataList
     */
    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        //判断业务类型
        if("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT) && rowDataList != null && rowDataList.size()>0){  //下单业务
            for (CanalEntry.RowData rowData : rowDataList) {  //遍历行集
                JSONObject jsonObject=new JSONObject();
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();  //得到列集
                for (CanalEntry.Column column : afterColumnsList) {  //遍历列集
                    System.out.println(column.getName()+":::::"+column.getValue());
                    String property = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(property,column.getValue());
                }
                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }

        }
    }
}
