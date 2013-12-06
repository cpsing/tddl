package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.optimizer.config.Group;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;

/**
 * 基于repo的{@linkplain StatManager}的委托实现
 * 
 * @author jianghang 2013-12-6 下午3:41:29
 * @since 5.1.0
 */
public class RepoStatManager extends AbstractLifecycle implements StatManager {

    private RepoStatManager                   delegate;
    private boolean                           isDelegate;
    private Group                             group;
    private LocalStatManager                  local;
    private LoadingCache<String, KVIndexStat> kvIndexCache = null;
    private LoadingCache<String, TableStat>   tableCache   = null;
    private boolean                           useCache;

    protected void doInit() throws TddlException {
        if (!isDelegate) {
            delegate = ExtensionLoader.load(RepoStatManager.class, group.getType().name());
            delegate.setGroup(group);
            delegate.setDelegate(true);
            delegate.init();

            kvIndexCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(30000, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<String, KVIndexStat>() {

                    public KVIndexStat load(String tableName) throws Exception {
                        return delegate.getKVIndex0(tableName);
                    }
                });

            tableCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(30000, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<String, TableStat>() {

                    public TableStat load(String tableName) throws Exception {
                        return delegate.getTable0(tableName);
                    }
                });
        }
    }

    public KVIndexStat getKVIndex(String indexName) {
        KVIndexStat stat = null;
        if (local != null) {// 本地如果开启了，先找本地
            stat = local.getKVIndex(indexName);
        }

        if (stat == null) {
            if (useCache) {
                try {
                    return kvIndexCache.get(indexName);
                } catch (ExecutionException e) {
                    throw new OptimizerException(e);
                }
            } else {
                return delegate.getKVIndex0(indexName);
            }
        }

        return stat;
    }

    public TableStat getTable(String tableName) {
        TableStat stat = null;
        if (local != null) {// 本地如果开启了，先找本地
            stat = local.getTable(tableName);
        }

        if (stat == null) {
            if (useCache) {
                try {
                    return tableCache.get(tableName);
                } catch (ExecutionException e) {
                    throw new OptimizerException(e);
                }
            } else {
                return delegate.getTable0(tableName);
            }
        }

        return stat;
    }

    protected KVIndexStat getKVIndex0(String indexName) {
        throw new NotSupportException("对应repo需要实现");
    }

    protected TableStat getTable0(String tableName) {
        throw new NotSupportException("对应repo需要实现");
    }

    public void setDelegate(boolean isDelegate) {
        this.isDelegate = isDelegate;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public void setLocal(LocalStatManager local) {
        this.local = local;
    }

}
