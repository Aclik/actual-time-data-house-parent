package com.yxBuild.service;

import java.util.Map;

public interface IPublisherService {

    /**
     * 查询日活总数
     *
     * @param date 日期,格式为：yyyy-MM-dd
     * @return
     */
    Integer getDauTotal(String date);

    /**
     * 查询日活分时汇总数据
     *
     * @param date
     * @return
     */
    Map getDauHoursMap(String date);

    /**
     * 查询单日收入总数
     *
     * @param date
     * @return
     */
    Double getOrderAmount(String date );

    /**
     * 查询单日分时收入
     *
     * @param date
     * @return
     */
    Map getOrderAmountHoursMap(String date);
}
