package com.taobao.tddl.rule.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 目标数据库特征 包含读写目标ds的id 以及该ds中符合要求的表名列表。
 * 
 * @author shenxun
 */
public class TargetDB {

    /**
     * 这个库在TDatasource索引中的索引
     */
    private String             dbIndex;

    /**
     * 这个规则下的符合查询条件的表名列表
     */
    private Map<String, Field> tableNames;

    /**
     * 返回表名的结果集
     * 
     * @return 空Set if 没有表 表名结果集
     */
    public Set<String> getTableNames() {
        if (tableNames == null) {
            return null;
        }
        return tableNames.keySet();
    }

    public void setTableNames(Map<String, Field> tableNames) {
        this.tableNames = tableNames;
    }

    public Map<String, Field> getTableNameMap() {
        return tableNames;
    }

    public void addOneTable(String table) {
        if (tableNames == null) {
            tableNames = new HashMap<String, Field>();
        }
        tableNames.put(table, Field.EMPTY_FIELD);
    }

    public void addOneTable(String table, Field field) {
        if (tableNames == null) {
            tableNames = new HashMap<String, Field>();
        }
        tableNames.put(table, field);
    }

    public void addOneTableWithSameTable(String table, Field field) {
        if (tableNames == null) {
            tableNames = new HashMap<String, Field>();
            tableNames.put(table, field);
        } else {
            Field inField = tableNames.get(table);
            if (inField == null) {
                tableNames.put(table, field);
            } else {
                if (field.getSourceKeys() != null) {
                    for (Map.Entry<String, Set<Object>> entry : field.getSourceKeys().entrySet()) {
                        inField.getSourceKeys().get(entry.getKey()).addAll(entry.getValue());
                    }
                }
            }
        }
    }

    public String getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
