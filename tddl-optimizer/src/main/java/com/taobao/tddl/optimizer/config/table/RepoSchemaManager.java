package com.taobao.tddl.optimizer.config.table;

import java.util.Collection;
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
 * 基于repo的{@linkplain SchemaManager}的委托实现
 * 
 * @author jianghang 2013-11-19 下午4:53:08
 * @since 5.1.0
 */
public class RepoSchemaManager extends AbstractLifecycle implements SchemaManager {

    private RepoSchemaManager               delegate;
    private boolean                         isDelegate;
    private LocalSchemaManager              local;
    private Group                           group;
    private LoadingCache<String, TableMeta> cache = null;
    private boolean                         useCache;

    protected void doInit() throws TddlException {
        if (!isDelegate) {
            delegate = ExtensionLoader.load(RepoSchemaManager.class, group.getType().name());
            delegate.setGroup(group);
            delegate.setDelegate(true);
            delegate.init();

            cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(30000, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<String, TableMeta>() {

                    public TableMeta load(String tableName) throws Exception {
                        return delegate.getTable0(tableName);
                    }
                });
        }
    }

    public final TableMeta getTable(String tableName) {
        TableMeta meta = null;
        if (local != null) {// 本地如果开启了，先找本地
            meta = local.getTable(tableName);
        }

        if (meta == null) {// 本地没有
            if (useCache) {
                try {
                    return cache.get(tableName);
                } catch (ExecutionException e) {
                    throw new OptimizerException(e);
                }
            } else {
                return delegate.getTable0(tableName);
            }
        }

        return meta;
    }

    public final void putTable(String tableName, TableMeta tableMeta) {
        if (local != null) {// 本地如果开启了，先处理本地
            local.putTable(tableName, tableMeta);
        } else if (useCache) {
            cache.put(tableName, tableMeta);
        } else {
            delegate.putTable(tableName, tableMeta);
        }
    }

    public Collection<TableMeta> getAllTables() {
        if (local != null) {// 本地如果开启了，先处理本地
            return local.getAllTables();
        } else {
            return cache.asMap().values();
        }
    }

    /**
     * 需要各Repo来实现
     * 
     * @param tableName
     */
    protected TableMeta getTable0(String tableName) {
        throw new NotSupportException("对应repo需要实现");
    }

    protected void doDestory() throws TddlException {
        if (!isDelegate) {
            delegate.destory();
            cache.invalidateAll();
        }
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public Group getGroup() {
        return group;
    }

    public void setLocal(LocalSchemaManager local) {
        this.local = local;
    }

    public void setDelegate(boolean isDelegate) {
        this.isDelegate = isDelegate;
    }

}
