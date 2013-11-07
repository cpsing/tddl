package com.taobao.tddl.rule.model;

import java.util.Calendar;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 用于传递自增数字和自增数字对应在Calendar里的类型 继承Comparable是因为开始预留的接口是Comparable...
 * 
 * @author shenxun
 */
public class DateEnumerationParameter implements Comparable {

    /**
     * 默认使用Date作为日期类型的基本自增单位
     * 
     * @param atomicIncreateNumber
     */
    public DateEnumerationParameter(int atomicIncreateNumber){
        this.atomicIncreatementNumber = atomicIncreateNumber;
        this.calendarFieldType = Calendar.DATE;
    }

    public DateEnumerationParameter(int atomicIncreateNumber, int calendarFieldType){
        this.atomicIncreatementNumber = atomicIncreateNumber;
        this.calendarFieldType = calendarFieldType;
    }

    public final int atomicIncreatementNumber;
    public final int calendarFieldType;

    public int compareTo(Object o) {
        throw new IllegalArgumentException("should not be here !");
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
