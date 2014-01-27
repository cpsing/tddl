package com.taobao.tddl.optimizer.core.plan.query;

import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * @since 5.0.0
 */
public interface IQuery extends IParallelizableQueryTree<IQueryTree> {

    /**
     * key 过滤器。 最重要的过滤器哦。 用来使用kv中的key进行查找。
     * 比较高的查询效率，这里的filter相当于使用map.get()的方式来取数据，所以效率很高。
     * 
     * @return
     */
    public IFilter getKeyFilter();

    /**
     * key 过滤器。 最重要的过滤器哦。 用来使用kv中的key进行查找。
     * 比较高的查询效率，这里的filter相当于使用map.get()的方式来取数据，所以效率很高。
     * 
     * @param keyFilter
     * @return
     */
    public IQuery setKeyFilter(IFilter keyFilter);

    /**
     * 锁模式。 目前没用
     * 
     * @return
     */
    public LOCK_MODEL getLockModel();

    public IQuery setLockModel(LOCK_MODEL lockModel);

    public IQuery setTableName(String actualTableName);

    /**
     * 实际表名，student_0000
     * 
     * @return
     */
    public String getTableName();

    /**
     * 索引名，逻辑上的，student._id
     * 
     * @return
     */
    public String getIndexName();

    /**
     * TABLENAME._IDXNAME
     * 
     * @param actualTable
     * @return
     */
    public IQuery setIndexName(String indexName);

    /**
     * 设置子查询
     * 
     * @param queryCommon
     * @return
     */
    public IQuery setSubQuery(IQueryTree queryCommon);

    /**
     * 获取子查询
     * 
     * @return
     */
    public IQueryTree getSubQuery();

}
