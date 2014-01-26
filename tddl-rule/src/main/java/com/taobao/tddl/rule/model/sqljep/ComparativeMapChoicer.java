package com.taobao.tddl.rule.model.sqljep;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 参数筛选器
 * 
 * @author shenxun
 */
public interface ComparativeMapChoicer {

    /**
     * 根据PartinationSet 获取列名和他对应值的map.
     * 
     * @param arguments PrepareStatement设置的参数
     * @param colNameSet 指定的column set
     * @return
     */
    Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> colNameSet);

    /**
     * @param arguments PrepareStatement设置的参数
     * @param colName 指定的column name
     * @return
     */
    Comparative getColumnComparative(List<Object> arguments, String colName);
}
