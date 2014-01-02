package com.taobao.tddl.optimizer.rule;

import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.LocalStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.RepoStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.StatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableStat;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;
import com.taobao.tddl.rule.model.TargetDB;

/**
 * 基于Rule获取到物理的group进行查找
 * 
 * @since 5.1.0
 */
public class RuleStatManager extends AbstractLifecycle implements StatManager {

    private OptimizerRule                        rule;
    private Matrix                               matrix;
    private LocalStatManager                     local;
    private boolean                              useCache;
    private LoadingCache<Group, RepoStatManager> repos = null;

    public RuleStatManager(OptimizerRule rule, Matrix matrix){
        this.rule = rule;
        this.matrix = matrix;
    }

    protected void doInit() throws TddlException {
        super.doInit();
        repos = CacheBuilder.newBuilder().build(new CacheLoader<Group, RepoStatManager>() {

            public RepoStatManager load(Group group) throws Exception {
                RepoStatManager repo = new RepoStatManager();
                repo.setGroup(group);
                repo.setLocal(local);
                repo.setUseCache(useCache);
                repo.init();
                return repo;
            }
        });
    }

    protected void doDestory() throws TddlException {
        super.doDestory();

        for (RepoStatManager repo : repos.asMap().values()) {
            repo.destory();
        }
    }

    public KVIndexStat getKVIndex(String indexName) {
        TargetDB targetDB = rule.shardAny(indexName);
        if (targetDB.getDbIndex() == null) {
            // 没有对应的规则，也没有default group，则可能是一个不存在的表
            // 尝试找一下local
            return local.getKVIndex(indexName);
        } else {
            Group group = matrix.getGroup(targetDB.getDbIndex()); // 先找到group
            try {
                return repos.get(group).getKVIndex(targetDB.getTableNames().iterator().next());
            } catch (ExecutionException e) {
                throw new OptimizerException(e);
            }
        }
    }

    public TableStat getTable(String tableName) {
        TargetDB targetDB = rule.shardAny(tableName);
        if (targetDB.getDbIndex() == null) {
            // 没有对应的规则，也没有default group，则可能是一个不存在的表
            // 尝试找一下local
            return local.getTable(tableName);
        } else {
            Group group = matrix.getGroup(targetDB.getDbIndex()); // 先找到group
            try {
                return repos.get(group).getTable(targetDB.getTableNames().iterator().next());
            } catch (ExecutionException e) {
                throw new OptimizerException(e);
            }
        }
    }

    public void setRule(OptimizerRule rule) {
        this.rule = rule;
    }

    public void setLocal(LocalStatManager local) {
        this.local = local;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

}
