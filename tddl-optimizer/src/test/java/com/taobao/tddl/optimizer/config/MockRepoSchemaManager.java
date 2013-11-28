package com.taobao.tddl.optimizer.config;

import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;

@Activate(name = "mock")
public class MockRepoSchemaManager extends RepoSchemaManager {

    protected TableMeta getTable0(String tableName) {
        return super.getTable0(tableName);
    }

}
