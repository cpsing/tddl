package com.taobao.tddl.executor.spi;

import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.optimizer.config.table.TableMeta;

/**
 * 每个存储一个，主入口
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:59:13
 * @since 5.1.0
 */
public interface IRepository {

    /**
     * 获取一个表对象 ，在任何sql操作中都会根据table schema找到对应的数据库实例对象的。 表对象包含核心数据和对应的二级索引。
     * 
     * @param meta
     * @return
     * @throws Exception
     */
    ITable getTable(TableMeta meta, String groupNode, long requestID) throws TddlException;

    /**
     * 关闭存储引擎所使用的对象。
     */
    void close() throws TddlException;

    /**
     * 针对这个存储引擎，开始一个事务。
     * 
     * @param conf
     * @return
     * @throws Exception
     */
    ITransaction beginTransaction(TransactionConfig conf) throws TddlException;

    /**
     * 获取所有的表对象
     * 
     * @return
     */
    Map<String, ITable> getTables();

    /**
     * 初始化时调用的方法。
     * 
     * @param conf
     */
    void init();

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

    // void cleanTempTable();

    ICommandHandlerFactory getCommandExecutorFactory();

    // DataSource getDataSource(String groupNode);

    IGroupExecutor buildGroupExecutor(Group group);

    ITempTable createTempTable();
}
