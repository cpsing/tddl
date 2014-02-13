package com.taobao.tddl.executor.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.optimizer.config.table.IndexMeta;

/**
 * 一次执行过程中的上下文
 * 
 * @author whisper
 */
public class ExecutionContext {

    /**
     * 当前事务的执行group，只支持单group的事务
     */
    private String                         transactionGroup = null;

    /**
     * 当前运行时的存储对象
     */
    private IRepository                    currentRepository;

    /**
     * 是否自动关闭结果集。目前这个东西已经基本无效。除了在update等查询中有使用
     */
    private boolean                        closeResultSet;
    /**
     * 当前事务
     */
    private ITransaction                   transaction;
    /**
     * 当前查询所使用的IndexMeta/这个放这里不是非常明确，他其实和生命周期无关。只是为了统一返回值，所以放在一起。
     */
    private IndexMeta                      meta;
    /**
     * 当前查询所使用的table
     */
    private ITable                         table;

    private Map<String, Object>            extraCmds        = new HashMap();

    private Map<Integer, ParameterContext> params           = null;
    private String                         isolation        = null;

    private ExecutorService                concurrentService;

    private boolean                        autoCommit       = true;

    private String                         groupHint        = null;

    public ExecutionContext(){

    }

    public IRepository getCurrentRepository() {
        return currentRepository;
    }

    public void setCurrentRepository(IRepository currentRepository) {
        this.currentRepository = currentRepository;
    }

    public boolean isCloseResultSet() {
        return closeResultSet;
    }

    public void setCloseResultSet(boolean closeResultSet) {
        this.closeResultSet = closeResultSet;
    }

    public ITransaction getTransaction() {
        return transaction;
    }

    public void setTransaction(ITransaction transaction) {
        this.transaction = transaction;
        if (transaction != null) {
            this.transaction.setAutoCommit(isAutoCommit());
        }
    }

    public IndexMeta getMeta() {
        return meta;
    }

    public void setMeta(IndexMeta meta) {
        this.meta = meta;
    }

    public ITable getTable() {
        return table;
    }

    public void setTable(ITable table) {
        this.table = table;
    }

    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;
    }

    public Map<Integer, ParameterContext> getParams() {
        return params;
    }

    public void setParams(Map<Integer, ParameterContext> params) {
        this.params = params;
    }

    public String getIsolation() {
        return isolation;
    }

    public void setIsolation(String isolation) {
        this.isolation = isolation;
    }

    public ExecutorService getExecutorService() {
        return this.concurrentService;
    }

    public void setExecutorService(ExecutorService concurrentService) {
        this.concurrentService = concurrentService;
    }

    public String getTransactionGroup() {
        return transactionGroup;
    }

    public void setTransactionGroup(String transactionGroup) {
        this.transactionGroup = transactionGroup;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        if (this.getTransaction() != null) {
            this.getTransaction().setAutoCommit(autoCommit);
        }
    }

    public String getGroupHint() {
        return groupHint;
    }

    public void setGroupHint(String groupHint) {
        this.groupHint = groupHint;
    }
}
