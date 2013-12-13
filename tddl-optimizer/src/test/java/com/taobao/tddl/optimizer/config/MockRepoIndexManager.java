package com.taobao.tddl.optimizer.config;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.config.table.IndexManager;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;

public class MockRepoIndexManager extends AbstractLifecycle implements IndexManager {

    private RepoSchemaManager schemaManager;

    public MockRepoIndexManager(RepoSchemaManager schemaManager){
        this.schemaManager = schemaManager;
    }

    public IndexMeta getIndexByName(String name) {
        int index = name.indexOf("\\.");
        if (index < 0) {
            index = name.length();
        }
        String tableName = name.substring(0, index);
        return schemaManager.getTable(tableName).getIndexMeta(name);
    }
}
