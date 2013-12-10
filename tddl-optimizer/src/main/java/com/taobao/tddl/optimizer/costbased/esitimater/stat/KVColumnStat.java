package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 列的柱状图数据结构
 * 
 * @author danchen
 */
public class KVColumnStat {

    // 列名
    private String               columnName;
    // 采样大小
    private long                 numRows;
    // 不同列值个数的统计
    private Map<Integer, Long>   valueHistogramMap;
    // 不同列值个数的百分比
    private Map<Integer, Double> valuePercentMap;

    public KVColumnStat(String columnName, long realsampleRows){
        this.columnName = columnName;
        this.numRows = realsampleRows;
        this.valuePercentMap = new HashMap<Integer, Double>();
    }

    public String getColumnName() {
        return columnName;
    }

    public long getNum_rows() {
        return numRows;
    }

    public Map<Integer, Long> getValueHistogramMap() {
        return valueHistogramMap;
    }

    public void setValueHistogramMap(Map<Integer, Long> valueHistogramMap) {
        if (valueHistogramMap == null) {
            return;
        }
        this.valueHistogramMap = valueHistogramMap;
        for (Iterator<Integer> iterator = valueHistogramMap.keySet().iterator(); iterator.hasNext();) {
            int key = iterator.next();
            Long value = valueHistogramMap.get(key);
            Double percent = Double.valueOf(value) / Double.valueOf(numRows);
            valuePercentMap.put(key, percent);
        }
    }

    public Map<Integer, Double> getValuePercentMap() {
        return valuePercentMap;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
