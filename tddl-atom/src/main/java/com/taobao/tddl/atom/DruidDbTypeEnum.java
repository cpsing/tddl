package com.taobao.tddl.atom;

/**
 * 数据库类型枚举类型
 * 
 * @author qihao
 */
public enum DruidDbTypeEnum {

    ORACLE(DruidConstants.DEFAULT_ORACLE_DRIVER_CLASS, DruidConstants.DEFAULT_DRUID_ORACLE_SORTER_CLASS),

    MYSQL(DruidConstants.DEFAULT_MYSQL_DRIVER_CLASS, DruidConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS);

    private String driverClass;
    private String sorterClass;

    DruidDbTypeEnum(String driverClass, String sorterClass){
        this.driverClass = driverClass;
        this.sorterClass = sorterClass;
    }

    public static DruidDbTypeEnum getAtomDbTypeEnumByType(String type) {
        try {
            return DruidDbTypeEnum.valueOf(type.trim().toUpperCase());
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
