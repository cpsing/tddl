package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public abstract class DataNodeExecutor<RT extends IDataNodeExecutor> implements IDataNodeExecutor<RT> {

    protected String  requestHostName;
    protected Long    requestID;
    protected Integer subRequestID;
    protected String  targetNode;
    protected boolean consistentRead = true;
    protected Integer thread;
    protected Object  extra;
    protected boolean useBIO         = false;
    protected String  sql;
    protected boolean streaming      = false;

    public RT executeOn(String targetNode) {
        this.targetNode = targetNode;
        return (RT) this;
    }

    public boolean getConsistent() {
        return consistentRead;
    }

    public Object getExtra() {
        return this.extra;
    }

    public String getDataNode() {
        return targetNode;
    }

    public String getRequestHostName() {
        return requestHostName;
    }

    public Long getRequestID() {
        return requestID;
    }

    public String getSql() {
        return this.sql;
    }

    public Integer getSubRequestID() {
        return subRequestID;
    }

    public Integer getThread() {
        return this.thread;
    }

    public boolean isStreaming() {
        return this.streaming;
    }

    public boolean isUseBIO() {
        return this.useBIO;
    }

    public RT setConsistent(boolean consistent) {
        this.consistentRead = consistent;
        return (RT) this;
    }

    public RT setExtra(Object obj) {
        this.extra = obj;
        return (RT) this;
    }

    public RT setRequestHostName(String requestHostName) {
        this.requestHostName = requestHostName;
        return (RT) this;
    }

    public RT setRequestID(Long requestID) {
        this.requestID = requestID;
        return (RT) this;
    }

    public RT setSql(String sql) {
        this.sql = sql;
        return (RT) this;
    }

    public RT setStreaming(boolean streaming) {
        this.streaming = streaming;
        return (RT) this;
    }

    public RT setSubRequestID(Integer subRequestID) {
        this.subRequestID = subRequestID;
        return (RT) this;
    }

    /**
     * 表明一个建议的用于执行该节点的线程id
     */
    public RT setThread(Integer i) {
        this.thread = i;
        return (RT) this;
    }

    public RT setUseBIO(boolean useBIO) {
        this.useBIO = useBIO;
        return (RT) this;
    }

}
