package com.taobao.tddl.executor.spi;

import com.taobao.tddl.executor.IExecutor;

public interface ITopologyExecutor extends IExecutor {

    /**
     * 该executor所在节点的名字
     * 
     * @return
     */
    public String getDataNode();

}
