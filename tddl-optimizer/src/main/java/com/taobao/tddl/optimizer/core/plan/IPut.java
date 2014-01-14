package com.taobao.tddl.optimizer.core.plan;

import java.util.List;

import com.taobao.tddl.optimizer.core.expression.ISelectable;

public interface IPut<RT extends IPut> extends IDataNodeExecutor<RT> {

    public enum PUT_TYPE {
        REPLACE, INSERT, DELETE, UPDATE;
    }

    /**
     * depend query command
     * 
     * @return
     */
    IQueryTree getQueryTree();

    /**
     * @param queryCommon
     */
    RT setQueryTree(IQueryTree queryTree);

    /**
     * set a = 1 ,b = 2 , c = 3 那么这个应该是 [‘a‘,‘b‘,‘c‘]
     * 
     * @param columns
     */
    RT setUpdateColumns(List<ISelectable> columns);

    List<ISelectable> getUpdateColumns();

    /**
     * IdxName
     * 
     * @param indexName
     */
    RT setTableName(String indexName);

    String getTableName();

    RT setIndexName(String indexName);

    String getIndexName();

    /**
     * set a = 1 ,b = 2 , c = 3 那么这个应该是 [1，2，3]
     * 
     * @param columns
     */
    RT setUpdateValues(List<Object> values);

    List<Object> getUpdateValues();

    PUT_TYPE getPutType();

    RT setIgnore(boolean ignore);

    boolean isIgnore();

    /**
     * 用于多值insert
     * 
     * @return
     */
    public List<List<Object>> getMultiValues();

    public RT setMultiValues(List<List<Object>> multiValues);

    public boolean isMutiValues();

    public RT setMutiValues(boolean isMutiValues);

    public int getMuiltValuesSize();

    public List<Object> getValues(int index);
}
