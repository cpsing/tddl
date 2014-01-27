package com.taobao.tddl.optimizer.rule;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.config.table.IndexManager;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.SchemaManager;

/**
 * 基于{@linkplain RuleSchemaManager}完成index的获取
 * 
 * @since 5.0.0
 */
public class RuleIndexManager extends AbstractLifecycle implements IndexManager {

    private SchemaManager schemaManager;

    public RuleIndexManager(SchemaManager schemaManager){
        this.schemaManager = schemaManager;
    }

    public IndexMeta getIndexByName(String name) {
        int index = name.indexOf(".");
        if (index < 0) {
            index = name.length();
        }
        String tableName = name.substring(0, index);
        IndexMeta in = schemaManager.getTable(tableName).getIndexMeta(name);
        if (in == null) {
            throw new IllegalArgumentException("table : " + tableName + " index : " + index + " is not found");
        }

        return in;
    }
}
