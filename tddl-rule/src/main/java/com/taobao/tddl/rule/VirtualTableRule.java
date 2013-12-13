package com.taobao.tddl.rule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.rule.virtualnode.DBTableMap;
import com.taobao.tddl.rule.virtualnode.TableSlotMap;

/**
 * 对应月tddl中的一张逻辑表，每张逻辑表上存在db/tb的{@linkplain Rule}<br/>
 * 基于该静态Rule(利用枚举步长和次数)可简单推算出数据库拓扑结构
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

    public DBTableMap getDbTableMap();

    // =========================================================================
    // 规则和其他属性的分割线
    // =========================================================================

    DBType getDbType();

    boolean isAllowReverseOutput();

    boolean isAllowFullTableScan();

    public String getTbNamePattern();

    public String getDbNamePattern();

    public String[] getDbRuleStrs();

    public String[] getTbRulesStrs();

    public boolean isBroadcast();

    public String getJoinGroup();

    // ==================== 获取rule的一些信息，比如分区字段 =================

    /**
     * 获取当前默认版本，逻辑表的分区字段，如果存在多个规则时，返回多个规则分区字段的总和
     */
    public List<String> getShardColumns();
}
