package com.taobao.tddl.optimizer.costbased.esitimater.stat;

/**
 * 表上的统计信息
 */
public interface StatManager {

    KVIndexStat getKVIndex(String indexName);

    TableStat getTable(String tableName);
}
