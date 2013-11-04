package com.taobao.tddl.rule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.rule.model.virtualnode.DbTableMap;
import com.taobao.tddl.rule.model.virtualnode.TableSlotMap;

/**
 * <pre>
 * TDataSource持有所有虚拟表名到该对象的引用
 * tddl-client根据解析/预解析结果取得虚拟表名
 * 根据虚拟表名取得对应的VirtualTableRule对象
 * </pre>
 * 
 * @author linxuan
 */
public interface VirtualTableRule<D, T> {

    /**
     * 库规则链
     */
    List<Rule<String>> getDbShardRules();

    /**
     * 表规则链
     */
    List<Rule<String>> getTbShardRules();

    /**
     * 返回本规则实际对应的全部库表拓扑结构
     * 
     * @return key:dbIndex; value:实际物理表名的集合
     */
    Map<String, Set<String>> getActualTopology();

    Object getOuterContext();

    public TableSlotMap getTableSlotMap();

    public DbTableMap getDbTableMap();

    // =========================================================================
    // 规则和其他属性的分割线
    // =========================================================================

    DBType getDbType();

    boolean isAllowReverseOutput();

    boolean isAllowFullTableScan();

    boolean isNeedRowCopy();

    List<String> getUniqueKeys();

    public String getTbNamePattern();

    public String getDbNamePattern();

    public String[] getDbRuleStrs();

    public String[] getTbRulesStrs();
}
