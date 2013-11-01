package com.taobao.tddl.common.model;

import java.util.List;

public interface SqlMetaData {

    /**
     * 原始的sql,即应用直接给到底层的sql
     * 
     * @return
     */
    public String getOriSql();

    /**
     * 针对格式或者参数个数不同，但表达语义一致的sql的统一格式化 例如 id in(?,?...) 统一为 id in (?)
     * 
     * @return
     */
    public String getLogicSql();

    /**
     * @return
     */
    public List<String> getLogicTables();

    /**
     * sql是否被解析过
     * 
     * @return
     */
    public boolean isParsed();

}
