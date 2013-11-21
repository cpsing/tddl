package com.taobao.tddl.atom;

import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.model.DataSourceType;

/**
 * 数据库类型枚举类型
 * 
 * @author qihao
 */
public enum TAtomDbTypeEnum {

    ORACLE,

    MYSQL;

    private String driverClass;
    private String sorterClass;

    private void init(DataSourceType dataSourceType) {
        if (dataSourceType == DataSourceType.DruidDataSource) {
            if (this == TAtomDbTypeEnum.ORACLE) {
                this.driverClass = TAtomConstants.DEFAULT_ORACLE_DRIVER_CLASS;
                this.sorterClass = TAtomConstants.DEFAULT_DRUID_ORACLE_SORTER_CLASS;
            } else {
                this.driverClass = TAtomConstants.DEFAULT_MYSQL_DRIVER_CLASS;
                this.sorterClass = TAtomConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS;
            }
        }
    }

    public static TAtomDbTypeEnum getAtomDbTypeEnum(String dbType, DataSourceType dataSourceType) {
        try {
            TAtomDbTypeEnum atomDbTypeEnum = TAtomDbTypeEnum.valueOf(dbType.trim().toUpperCase());
            atomDbTypeEnum.init(dataSourceType);
            return atomDbTypeEnum;
        } catch (Exception e) {
            return null;
        }
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getSorterClass() {
        return sorterClass;
    }

}
