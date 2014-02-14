package com.taobao.tddl.qatest;

import com.taobao.tddl.common.exception.NotSupportException;

/**
 * 此类对运行的表做定义 数组中均为表名 表名后缀是Index表示二级索引，包含一个字段的索引 表名后缀是twoIndex表示二级索引，包含两个字段的组合索引
 * 表名后缀是threeIndex表示二级索引，包含两三字段的组合索引 表名后缀是oneGroup_oneAtom 表示单库单表
 * 表名后缀是oneGroup_mutilAtom 表示单库多表 表名后缀是oneGroup_mutilAtom 表示多库多表
 * 表中名包含msyql字段的调用的mysql数据库，其他的均为bdb数据库
 * 表normaltbl_mutilGroup_twoIndex_complexRule为模拟线上配置规则，特别测试用例
 */
public class ExecuteTableName {

    // 几种case :
    // [0]单库单表 （一个group，对应一个atom）
    // [1]单库单表 二级索引是一个字段组成
    // [2]单库单表 二级索引是二个字段组成
    // [3]单库单表 二级索引是三个字段组成
    // [4]单库多表（有分表情况，一个group对应多个atom）
    // [5]单库多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
    // [6]单库多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
    // [7]单库多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是strongConsistent
    // [8]单库多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
    // [9]多库多表（有分库分表情况，一个group对应多个atom）
    // [10]多库多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
    // [11]多库多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是strongConsistent
    // [12]多库多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
    // [13]多库多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是strongConsistent
    // [14]多库多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
    /**
     * 单个表，表名为normaltbl
     */
    public static String[][] normaltblTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // case[0]
                    { "normaltbl_oneGroup_oneAtom" },
                    // case[4]
                    { "normaltbl_oneGroup_mutilAtom" },
                    // case[9]
                    { "normaltbl_mutilGroup" }, };

            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // case[0]
                    { "tdhs_normaltbl_oneGroup_oneAtom" },
                    // case[4]
                    { "tdhs_normaltbl_oneGroup_mutilAtom" },
                    // case[9]
                    { "tdhs_normaltbl_mutilGroup" }, };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // case[0]
                    { "mysql_normaltbl_oneGroup_oneAtom" },
                    // case[4]
                    { "mysql_normaltbl_oneGroup_mutilAtom" },
                    // case[9]
                    { "mysql_normaltbl_mutilGroup" },
                    // case[0] oceanbase
                    { "ob_normaltbl_oneGroup_oneAtom" }, };
            return object;
        } else if (db.equals("hbase")) {
            String[][] object = {
            // case[0]
            { "hbase_normaltbl_oneGroup_oneAtom" } };
            return object;
        }

        throw new NotSupportException();
    }

    /**
     * 单个表，表名为student
     */
    public static String[][] studentTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // case[0]
                    { "student_oneGroup_oneAtom" },
                    // case[4]
                    { "student_oneGroup_mutilAtom" },
                    // case[9]
                    { "student_mutilGroup" } };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // case[0]
                    { "mysql_student_oneGroup_oneAtom" },
                    // case[4]
                    { "mysql_student_oneGroup_mutilAtom" },
                    // case[9]
                    { "mysql_student_mutilGroup" },
                    // case[0] oceanbase
                    { "ob_student_oneGroup_oneAtom" }, };
            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // case[0]
                    { "tdhs_student_oneGroup_oneAtom" },
                    // case[4]
                    { "tdhs_student_oneGroup_mutilAtom" },
                    // case[9]
                    { "tdhs_student_mutilGroup" } };
            return object;
        }

        throw new NotSupportException();
    }

    /**
     * 两个表，表名分表为normaltbl，student
     * 
     * @return
     */
    public static String[][] normaltblStudentTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // case[0]
                    { "normaltbl_oneGroup_oneAtom", "student_oneGroup_oneAtom" },
                    // case[4]
                    { "normaltbl_oneGroup_mutilAtom", "student_oneGroup_mutilAtom" },
                    // case[9]
                    { "normaltbl_mutilGroup", "student_mutilGroup" }, };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // case[0]
                    { "mysql_normaltbl_oneGroup_oneAtom", "mysql_student_oneGroup_oneAtom" },
                    // case[4]
                    { "mysql_normaltbl_oneGroup_mutilAtom", "mysql_student_oneGroup_mutilAtom" },
                    // case[9]
                    { "mysql_normaltbl_mutilGroup", "mysql_student_mutilGroup" },
                    // case[0] oceanbase
                    { "ob_normaltbl_oneGroup_oneAtom", "ob_student_oneGroup_oneAtom" },
                    // case[0] mysql join oceanbase
                    { "mysql_normaltbl_oneGroup_oneAtom", "ob_student_oneGroup_oneAtom" },
                    { "ob_normaltbl_oneGroup_oneAtom", "mysql_student_oneGroup_oneAtom" },
                    // case[4] mysql join oceanbase
                    { "mysql_normaltbl_oneGroup_mutilAtom", "ob_student_oneGroup_oneAtom" },
                    { "ob_normaltbl_oneGroup_oneAtom", "mysql_student_oneGroup_mutilAtom" },
                    // case[9] mysql join oceanbase
                    { "mysql_normaltbl_mutilGroup", "ob_student_oneGroup_oneAtom" },
                    { "ob_normaltbl_oneGroup_oneAtom", "mysql_student_mutilGroup" },

            };
            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // case[0]
                    { "tdhs_normaltbl_oneGroup_oneAtom", "tdhs_student_oneGroup_oneAtom" },
                    // case[4]
                    { "tdhs_normaltbl_oneGroup_mutilAtom", "tdhs_student_oneGroup_mutilAtom" },
                    // case[9]
                    { "tdhs_normaltbl_mutilGroup", "tdhs_student_mutilGroup" }, };
            return object;
        }

        throw new NotSupportException();
    }

    /**
     * mysql两个表，表名分表为host_info，hostgroup_info
     * 
     * @return
     */
    public static String[][] mysqlHostinfoHostgroupTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {};
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // case[0]
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom" },
                    // case[4]
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_oneGroup_mutilAtom" },
                    // case[9]
                    { "mysql_host_info_mutilGroup", "mysql_hostgroup_info_mutilGroup" },
                    // case[0] oceanbase
                    { "ob_host_info_oneGroup_oneAtom", "ob_hostgroup_info_oneGroup_oneAtom" },
                    // case[0] mysql join oceanbase
                    { "mysql_host_info_oneGroup_oneAtom", "ob_hostgroup_info_oneGroup_oneAtom" },
                    { "ob_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom" },
                    // case[4] mysql join oceanbase
                    { "mysql_host_info_oneGroup_mutilAtom", "ob_hostgroup_info_oneGroup_oneAtom" },
                    { "ob_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_mutilAtom" },
                    // case[9] mysql join oceanbase
                    { "mysql_host_info_mutilGroup", "ob_hostgroup_info_oneGroup_oneAtom" },
                    { "ob_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_mutilGroup" }, };
            return object;
        }

        throw new NotSupportException();
    }

    /**
     * 五张表，表名分别为host_infoTableName，hostgroup_infoTableName
     * studentTableName，module_infoTableName，module_hostTableName
     */
    public static String[][] hostinfoHostgoupStudentModuleinfoModulehostTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // case[0]
                    { "host_info_oneGroup_oneAtom", "hostgroup_info_oneGroup_oneAtom", "student_oneGroup_oneAtom",
                            "module_info_oneGroup_oneAtom", "module_host_oneGroup_oneAtom" },
                    // case[0]特殊
                    // （一个group，对应一个atom,其中mysql_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "host_info_oneGroup_oneAtom", "hostgroup_info_oneGroup_oneAtom_otherDb",
                            "student_oneGroup_oneAtom", "module_info_oneGroup_oneAtom", "module_host_oneGroup_oneAtom" },
                    // case[4]
                    { "host_info_oneGroup_mutilAtom", "hostgroup_info_oneGroup_mutilAtom",
                            "student_oneGroup_mutilAtom", "module_info_oneGroup_mutilAtom",
                            "module_host_oneGroup_mutilAtom" },
                    // case[9]
                    { "host_info_mutilGroup", "hostgroup_info_mutilGroup", "student_mutilGroup",
                            "module_info_mutilGroup", "module_host_oneGroup_mutilAtom" },
                    // case[9]特殊
                    // 其中host_info_mutilGroup不带索引，hostgroup_info_mutilGroup带索引
                    { "host_info_oneGroup_mutilAtom", "hostgroup_info_mutilGroup", "student_mutilGroup",
                            "module_info_mutilGroup", "module_host_oneGroup_mutilAtom" } };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // case[0]
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom",
                            "mysql_student_oneGroup_oneAtom", "mysql_module_info_oneGroup_oneAtom",
                            "mysql_module_host_oneGroup_oneAtom" },
                    // case[0]特殊
                    // （一个group，对应一个atom,其中mysql_mysql_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom_otherDb",
                            "mysql_student_oneGroup_oneAtom", "mysql_module_info_oneGroup_oneAtom",
                            "mysql_module_host_oneGroup_oneAtom" },
                    // case[4]
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_oneGroup_mutilAtom",
                            "mysql_student_oneGroup_mutilAtom", "mysql_module_info_oneGroup_mutilAtom",
                            "mysql_module_host_oneGroup_mutilAtom" },
                    // case[9]
                    { "mysql_host_info_mutilGroup", "mysql_hostgroup_info_mutilGroup", "mysql_student_mutilGroup",
                            "mysql_module_info_mutilGroup", "mysql_module_host_oneGroup_mutilAtom" },
                    // case[9]特殊
                    // 其中mysql_host_info_mutilGroup不带索引，mysql_hostgroup_info_mutilGroup带索引
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_mutilGroup",
                            "mysql_student_mutilGroup", "mysql_module_info_mutilGroup",
                            "mysql_module_host_oneGroup_mutilAtom" }

            };
            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // case[0]
                    { "tdhs_host_info_oneGroup_oneAtom", "tdhs_hostgroup_info_oneGroup_oneAtom",
                            "tdhs_student_oneGroup_oneAtom", "tdhs_module_info_oneGroup_oneAtom",
                            "tdhs_module_host_oneGroup_oneAtom" },
                    // case[1]特殊
                    // （一个group，对应一个atom,其中tdhs_tdhs_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "tdhs_host_info_oneGroup_oneAtom", "tdhs_hostgroup_info_oneGroup_oneAtom_otherDb",
                            "tdhs_student_oneGroup_oneAtom", "tdhs_module_info_oneGroup_oneAtom",
                            "tdhs_module_host_oneGroup_oneAtom" },
                    // case[4]
                    { "tdhs_host_info_oneGroup_mutilAtom", "tdhs_hostgroup_info_oneGroup_mutilAtom",
                            "tdhs_student_oneGroup_mutilAtom", "tdhs_module_info_oneGroup_mutilAtom",
                            "tdhs_module_host_oneGroup_mutilAtom" },
                    // case[9]
                    { "tdhs_host_info_mutilGroup", "tdhs_hostgroup_info_mutilGroup", "tdhs_student_mutilGroup",
                            "tdhs_module_info_mutilGroup", "tdhs_module_host_oneGroup_mutilAtom" },
                    // case[9]特殊
                    // 其中tdhs_host_info_mutilGroup不带索引，tdhs_hostgroup_info_mutilGroup带索引
                    { "tdhs_host_info_oneGroup_mutilAtom", "tdhs_hostgroup_info_mutilGroup", "tdhs_student_mutilGroup",
                            "tdhs_module_info_mutilGroup", "tdhs_module_host_oneGroup_mutilAtom" } };
            return object;
        }

        throw new NotSupportException();

    }

}
