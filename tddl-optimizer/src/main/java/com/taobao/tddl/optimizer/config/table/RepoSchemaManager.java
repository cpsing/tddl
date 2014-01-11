package com.taobao.tddl.optimizer.config.table;

import java.util.Collection;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.optimizer.rule.OptimizerRule;

/**
 * 基于repo的{@linkplain SchemaManager}的委托实现
 * 
 * @author jianghang 2013-11-19 下午4:53:08
 * @since 5.1.0
 */
public class RepoSchemaManager extends AbstractLifecycle implements SchemaManager {

    private RepoSchemaManager   delegate;
    private boolean             isDelegate;
    private StaticSchemaManager local;
    private Group               group;

    protected OptimizerRule     rule = null;

    public void setRule(OptimizerRule rule) {
        this.rule = rule;
    }

    public OptimizerRule getRule() {
        return this.rule;
    }

    public static class LogicalAndActualTableName {

        public String logicalTableName;
        public String actualTableName;

        @Override
        public int hashCode() {
            return logicalTableName.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return logicalTableName.equals(((LogicalAndActualTableName) o).logicalTableName);
        }
    }

    @Override
    protected void doInit() throws TddlException {
        if (local != null) {
            local.init();
        }

        if (!isDelegate) {
            delegate = ExtensionLoader.load(RepoSchemaManager.class, group.getType().name());
            delegate.setGroup(group);
            delegate.setDelegate(true);
            delegate.setRule(rule);
            delegate.init();
        }
    }

    @Override
    public final TableMeta getTable(String tableName) {
        return getTable(tableName, null);
    }

    public final TableMeta getTable(String logicalTableName, String actualTableName) {
        TableMeta meta = null;
        if (local != null) {// 本地如果开启了，先找本地
            meta = local.getTable(logicalTableName);
        }

        if (meta == null) {// 本地没有
            meta = delegate.getTable0(logicalTableName, actualTableName);
        }

        return meta;
    }

    @Override
    public final void putTable(String tableName, TableMeta tableMeta) {
        if (local != null) {// 本地如果开启了，先处理本地
            local.putTable(tableName, tableMeta);
        } else {
            delegate.putTable(tableName, tableMeta);
        }
    }

    @Override
    public Collection<TableMeta> getAllTables() {
        if (local != null) {// 本地如果开启了，先处理本地
            return local.getAllTables();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * 需要各Repo来实现
     * 
     * @param tableName
     * @param actualTableName
     */
    protected TableMeta getTable0(String logicalTableName, String actualTableName) {
        throw new NotSupportException("对应repo需要实现");
    }

    @Override
    protected void doDestory() throws TddlException {
        if (local != null && local.isInited()) {
            local.destory();
        }

        if (!isDelegate) {
            delegate.destory();

        }
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public Group getGroup() {
        return group;
    }

    public void setLocal(StaticSchemaManager local) {
        this.local = local;
    }

    public void setDelegate(boolean isDelegate) {
        this.isDelegate = isDelegate;
    }

}
