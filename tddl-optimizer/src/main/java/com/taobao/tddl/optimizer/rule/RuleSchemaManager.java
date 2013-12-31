package com.taobao.tddl.optimizer.rule;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;
import com.taobao.tddl.rule.model.TargetDB;

/**
 * 基于Rule获取到物理的group进行查找
 * 
 * @since 5.1.0
 */
public class RuleSchemaManager extends AbstractLifecycle implements SchemaManager {

    private OptimizerRule                          rule;
    private Matrix                                 matrix;
    private StaticSchemaManager                    local;
    private boolean                                useCache = true;
    private LoadingCache<Group, RepoSchemaManager> repos    = null;

    public RuleSchemaManager(OptimizerRule rule, Matrix matrix){
        this.rule = rule;
        this.matrix = matrix;
    }

    protected void doInit() throws TddlException {
        super.doInit();

        if (local != null) local.init();
        repos = CacheBuilder.newBuilder().build(new CacheLoader<Group, RepoSchemaManager>() {

            public RepoSchemaManager load(Group group) throws Exception {
                RepoSchemaManager repo = new RepoSchemaManager();
                repo.setGroup(group);
                repo.setLocal(local);
                repo.setUseCache(useCache);
                repo.setRule(rule);
                repo.init();
                return repo;
            }
        });
    }

    protected void doDestory() throws TddlException {
        super.doDestory();

        for (RepoSchemaManager repo : repos.asMap().values()) {
            repo.destory();
        }
    }

    public TableMeta getTable(String tableName) {

        TableMeta meta = null;
        if (local != null) {// 本地如果开启了，先找本地
            meta = local.getTable(tableName);
        }

        if (meta != null) {
            return meta;
        }

        TargetDB targetDB = rule.shardAny(tableName);
        Group group = matrix.getGroup(targetDB.getDbIndex()); // 先找到group
        try {
            return repos.get(group).getTable(tableName, targetDB.getTableNames().iterator().next());
        } catch (ExecutionException e) {
            throw new OptimizerException(e);
        }
    }

    public void putTable(String tableName, TableMeta tableMeta) {
        if (local != null) {
            local.putTable(tableName, tableMeta);
        }
    }

    public Collection<TableMeta> getAllTables() {
        if (local != null) {
            return local.getAllTables();
        } else {
            throw new NotSupportException();
        }
    }

    public void setRule(OptimizerRule rule) {
        this.rule = rule;
    }

    public void setLocal(StaticSchemaManager local) {
        this.local = local;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

}
