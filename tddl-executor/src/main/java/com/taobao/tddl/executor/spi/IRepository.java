package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.executor.IExecutor;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.optimizer.config.table.TableMeta;

/**
 * 每个存储一个，主入口
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:59:13
 * @since 5.0.0
 */
public interface IRepository extends Lifecycle {

    /**
     * 获取对应的表结构
     */
    ITable getTempTable(TableMeta meta) throws TddlException;

    /**
     * 获取一个表对象 ，在任何sql操作中都会根据table schema找到对应的数据库实例对象的。 表对象包含核心数据和对应的二级索引。
     * 
     * @param meta
     * @return
     * @throws Exception
     */
    ITable getTable(TableMeta meta, String groupNode) throws TddlException;

    /**
     * 针对这个存储引擎，开始一个事务。
     * 
     * @param conf
     * @return
     * @throws Exception
     */
    ITransaction beginTransaction(TransactionConfig conf) throws TddlException;

    /**
     * 获取当前存储引擎的一些配置信息。
     * 
     * @return
     */
    RepositoryConfig getRepoConfig();

    /**
     * 判断当前引擎是否是read only的，如果为readOnly则不可写，只可读，用于容灾。
     * 
     * @return
     */
    boolean isWriteAble();

    /**
     * 是否是加强型模式，简单来说，加强模式就是目前bdb的模式，执行query和join，尽可能先在本机内执行完毕后再去远程执行。
     * 而mysql有自己的协议，无法做到这一点，所以应该返回false.
     * 
     * @return
     */
    boolean isEnhanceExecutionModel(String groupKey);

    /**
     * cursor实现类
     * 
     * @return
     */
    ICursorFactory getCursorFactory();

    /**
     * 获取对应的 handler构造器
     * 
     * @return
     */
    ICommandHandlerFactory getCommandExecutorFactory();

    /**
     * 获取对应的group {@link IExecutor}
     * 
     * @param group
     * @return
     */
    IGroupExecutor getGroupExecutor(Group group);
}
