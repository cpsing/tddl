package com.taobao.tddl.optimizer.config;

import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.RepoStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableStat;

@Activate(name = "MYSQL_JDBC")
public class MockRepoStatManager extends RepoStatManager {

    protected void doInit() {
        // DO NOTHGING
    }

    protected KVIndexStat getKVIndex0(String indexName) {
        return null;
    }

    protected TableStat getTable0(String tableName) {
        return null;
    }

}
