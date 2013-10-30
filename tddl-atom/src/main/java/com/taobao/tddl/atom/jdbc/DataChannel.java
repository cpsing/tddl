package com.taobao.tddl.atom.jdbc;


/**
 * @author JIECHEN
 */
public interface DataChannel {

    /**
     * 传递该sql的元信息给底层
     * 
     * @param sqlMetaData
     */
    public void fillMetaData(SqlMetaData sqlMetaData);

    public SqlMetaData getSqlMetaData();

}
