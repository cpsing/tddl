package com.taobao.tddl.optimizer.config;

import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;

@Activate(name = "MYSQL_JDBC")
public class MockRepoSchemaManager extends RepoSchemaManager {

    protected void doInit() {
        // DO NOTHGING
    }

    protected TableMeta getTable0(String logicalTableName, String actualTableName) {
        return super.getTable0(logicalTableName, actualTableName);
    }

}
