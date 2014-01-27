package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.executor.IExecutor;

/**
 * 用于主备切换等group操作
 * 
 * @author mengshi.sunmengshi 2013-12-6 下午1:45:56
 * @since 5.0.0
 */
public interface IGroupExecutor extends IExecutor {

    public Group getGroupInfo();

    /**
     * 可能是个datasource ，也可能是个rpc客户端
     */
    public Object getRemotingExecutableObject();

}
