package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * 表上的统计信息
 */
public interface StatManager extends Lifecycle {

    KVIndexStat getKVIndex(String indexName);

    TableStat getTable(String tableName);
}
