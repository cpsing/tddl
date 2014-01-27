package com.taobao.tddl.optimizer.config.table;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * 获取索引信息
 * 
 * @since 5.0.0
 */
public interface IndexManager extends Lifecycle {

    IndexMeta getIndexByName(String name);
}
