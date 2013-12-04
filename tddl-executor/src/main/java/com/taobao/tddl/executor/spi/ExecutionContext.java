package com.taobao.tddl.executor.spi;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;

/**
 * 一次执行过程中的上下文
 * 
 * @author whisper
 */
public class ExecutionContext {

    /**
     * 当前的事务的事务全局ID
     */
    private Long                   transactionSequence;

    /**
     * 当前运行时的存储对象
     */
    private Repository             currentRepository;
    /**
     * 是否创建事务
     */
    private boolean                createTxn;

    /**
     * 是否自动关闭结果集。目前这个东西已经基本无效。除了在update等查询中有使用
     */
    private boolean                closeResultSet;
    /**
     * 当前事务
     */
    private Transaction            transaction;
    /**
     * 当前查询所使用的IndexMeta/这个放这里不是非常明确，他其实和生命周期无关。只是为了统一返回值，所以放在一起。
     */
    private IndexMeta              meta;
    /**
     * 当前查询所使用的table
     */
    private Table                  table;

    String                         actualTable;

    TopologyHandler                topology  = null;

    Map<String, Comparable>        extraCmds = null;
    Map<Integer, ParameterContext> params    = null;

    public ExecutionContext(){

    }

    public Long getTransactionSequence() {
        return transactionSequence;
    }

    public void setTransactionSequence(Long transactionSequence) {
        this.transactionSequence = transactionSequence;
    }

    public Repository getCurrentRepository() {
        return currentRepository;
    }

    public void setCurrentRepository(Repository currentRepository) {
        this.currentRepository = currentRepository;
    }

    public boolean isCreateTxn() {
        return createTxn;
    }

    public void setCreateTxn(boolean createTxn) {
        this.createTxn = createTxn;
    }

    public boolean isCloseResultSet() {
        return closeResultSet;
    }

    public void setCloseResultSet(boolean closeResultSet) {
        this.closeResultSet = closeResultSet;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public IndexMeta getMeta() {
        return meta;
    }

    public void setMeta(IndexMeta meta) {
        this.meta = meta;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getDbName() {
        return actualTable;
    }

    public void setActualTable(String actualTable) {
        this.actualTable = actualTable;
    }

    public TopologyHandler getTopology() {
        return topology;
    }

    public void setTopology(TopologyHandler topology) {
        this.topology = topology;
    }

    public Map<String, Comparable> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Comparable> extraCmds) {
        this.extraCmds = extraCmds;
    }

    public Map<Integer, ParameterContext> getParams() {
        return params;
    }

    public void setParams(Map<Integer, ParameterContext> params) {
        this.params = params;
    }

}
