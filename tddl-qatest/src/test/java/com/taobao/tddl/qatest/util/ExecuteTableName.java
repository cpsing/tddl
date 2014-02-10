package com.taobao.tddl.qatest.util;

/**
 * 此类对运行的表做定义 数组中均为表名 表名后缀是Index表示二级索引，包含一个字段的索引 表名后缀是twoIndex表示二级索引，包含两个字段的组合索引
 * 表名后缀是threeIndex表示二级索引，包含两三字段的组合索引 表名后缀是oneGroup_oneAtom 表示单裤单表
 * 表名后缀是oneGroup_mutilAtom 表示单裤多表 表名后缀是oneGroup_mutilAtom 表示多库多表
 * 表中名包含msyql字段的调用的mysql数据库，其他的均为bdb数据库
 * 表normaltbl_mutilGroup_twoIndex_complexRule为模拟线上配置规则，特别测试用例
 */
public class ExecuteTableName {

    /**
     * 单个表，表名为normaltbl
     */
    public static String[][] normaltblTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "normaltbl_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "normaltbl_oneGroup_oneAtom_Index" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "normaltbl_oneGroup_oneAtom_twoIndex" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "normaltbl_oneGroup_oneAtom_threeIndex" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "normaltbl_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_Index" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex_Consistent" },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_threeIndex" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "normaltbl_mutilGroup" },
            // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "normaltbl_mutilGroup_Index" },
            // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
            // strongConsistent
            // { "normaltbl_mutilGroup_Index_Consistent" },
            // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // { "normaltbl_mutilGroup_twoIndex" },
            // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "normaltbl_mutilGroup_twoIndex_Consistent" },
            // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // 分库采用复杂分库规则
            // { "normaltbl_mutilGroup_twoIndex_complexRule" },
            // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "normaltbl_mutilGroup_threeIndex" }
            };

            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "tdhs_normaltbl_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "tdhs_normaltbl_oneGroup_oneAtom_Index" },
                    // // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "tdhs_normaltbl_oneGroup_oneAtom_twoIndex" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "tdhs_normaltbl_oneGroup_oneAtom_threeIndex" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "tdhs_normaltbl_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_Index" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_twoIndex" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_twoIndex_Consistent"
                    // },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_threeIndex" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "tdhs_normaltbl_mutilGroup" },
            // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "tdhs_normaltbl_mutilGroup_Index" },
            // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
            // strongConsistent
            // { "tdhs_normaltbl_mutilGroup_Index_Consistent" },
            // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // { "tdhs_normaltbl_mutilGroup_twoIndex" },
            // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "tdhs_normaltbl_mutilGroup_twoIndex_Consistent" },
            // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // 分库采用复杂分库规则
            // { "tdhs_normaltbl_mutilGroup_twoIndex_complexRule" },
            // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "tdhs_normaltbl_mutilGroup_threeIndex" }
            };

            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "mysql_normaltbl_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "mysql_normaltbl_oneGroup_oneAtom_Index" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "mysql_normaltbl_oneGroup_oneAtom_twoIndex" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "mysql_normaltbl_oneGroup_oneAtom_threeIndex" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "mysql_normaltbl_oneGroup_mutilAtom_Index" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "mysql_normaltbl_oneGroup_mutilAtom_twoIndex" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // {
                    // "mysql_normaltbl_oneGroup_mutilAtom_twoIndex_Consistent"
                    // },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "mysql_normaltbl_oneGroup_mutilAtom_threeIndex" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_mutilGroup" },
            // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_normaltbl_mutilGroup_Index" },
            // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
            // strongConsistent
            // { "mysql_normaltbl_mutilGroup_Index_Consistent" },
            // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // { "mysql_normaltbl_mutilGroup_twoIndex" },
            // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "mysql_normaltbl_mutilGroup_twoIndex_Consistent" },
            // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // 分库采用复杂分库规则
            // { "mysql_normaltbl_mutilGroup_twoIndex_complexRule" },
            // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "mysql_normaltbl_mutilGroup_threeIndex" }
            };

            return object;
        } else if (db.equals("hbase")) {
            String[][] object = {
            // 单库单表 （一个group，对应一个atom）
            { "hbase_normaltbl_oneGroup_oneAtom" }

            };

            return object;
        } else {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "normaltbl_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "normaltbl_oneGroup_oneAtom_Index" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "normaltbl_oneGroup_oneAtom_twoIndex" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "normaltbl_oneGroup_oneAtom_threeIndex" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "normaltbl_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_Index" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex_Consistent" },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_threeIndex" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "normaltbl_mutilGroup" },
                    // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
                    // { "normaltbl_mutilGroup_Index" },
                    // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_mutilGroup_Index_Consistent" },
                    // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // { "normaltbl_mutilGroup_twoIndex" },
                    // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_mutilGroup_twoIndex_Consistent" },
                    // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // 分库采用复杂分库规则
                    // { "normaltbl_mutilGroup_twoIndex_complexRule" },
                    // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
                    // { "normaltbl_mutilGroup_threeIndex" },

                    // [16]mysql单裤单表 （一个group，对应一个atom）
                    { "mysql_normaltbl_oneGroup_oneAtom" },
                    // [17]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_oneGroup_mutilAtom" },
                    // [18]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_mutilGroup" },
            // [19]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_normaltbl_mutilGroup_Index" },
            // [20]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是两个字段组成
            // { "mysql_normaltbl_mutilGroup_twoIndex" }
            };
            return object;
        }

    }

    /**
     * 单个表，表名为student
     */
    public static String[][] studentTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // [0]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "student_oneGroup_oneAtom" },
                    // [1]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "student_oneGroup_mutilAtom" },
                    // [2]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "student_mutilGroup" } };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // [0]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_student_oneGroup_oneAtom" },
                    // [1]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_student_oneGroup_mutilAtom" },
                    // [2]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_student_mutilGroup" } };
            return object;
        } else if (db.equals("tddl")) {
            String[][] object = {
                    // [0]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_student_oneGroup_oneAtom" },
                    // [1]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_student_oneGroup_mutilAtom" },
                    // [2]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_student_mutilGroup" } };
            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // [0]tdhs单裤多表（有分表情况，一个group对应多个atom）
                    { "tdhs_student_oneGroup_oneAtom" },
                    // [1]tdhs多裤多表（有分库分表情况，一个group对应多个atom）
                    { "tdhs_student_oneGroup_mutilAtom" },
                    // [2]tdhs多裤多表（有分库分表情况，一个group对应多个atom）
                    { "tdhs_student_mutilGroup" } };
            return object;
        } else {
            String[][] object = {
                    // [0]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "student_oneGroup_oneAtom" },
                    // [1]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "student_oneGroup_mutilAtom" },
                    // [2]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "student_mutilGroup" },

                    // [3]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_student_oneGroup_oneAtom" },
                    // [4]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_student_oneGroup_mutilAtom" },
                    // [5]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_student_mutilGroup" } };
            return object;
        }
    }

    /**
     * 两个表，表名分表为normaltbl，student
     * 
     * @return
     */
    public static String[][] normaltblStudentTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // [0]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "normaltbl_oneGroup_oneAtom", "student_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "normaltbl_oneGroup_oneAtom_Index",
                    // "student_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "normaltbl_oneGroup_oneAtom_twoIndex",
                    // "student_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "normaltbl_oneGroup_oneAtom_threeIndex",
                    // "student_oneGroup_oneAtom" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "normaltbl_oneGroup_mutilAtom", "student_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_Index",
                    // "student_oneGroup_mutilAtom" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex",
                    // "student_oneGroup_mutilAtom" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex_Consistent",
                    // "student_oneGroup_mutilAtom" },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_threeIndex",
                    // "student_oneGroup_mutilAtom" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "normaltbl_mutilGroup", "student_mutilGroup" },
            // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "normaltbl_mutilGroup_Index", "student_mutilGroup" },
            // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
            // strongConsistent
            // { "normaltbl_mutilGroup_Index_Consistent",
            // "student_mutilGroup" },
            // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // { "normaltbl_mutilGroup_twoIndex", "student_mutilGroup" },
            // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // 分库采用复杂分库规则
            // { "normaltbl_mutilGroup_twoIndex_complexRule",
            // "student_mutilGroup" },
            // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "normaltbl_mutilGroup_twoIndex_Consistent",
            // "student_mutilGroup" },
            // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "normaltbl_mutilGroup_threeIndex", "student_mutilGroup" }
            };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // [0]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_oneGroup_oneAtom", "mysql_student_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "mysql_normaltbl_oneGroup_oneAtom_Index",
                    // "mysql_student_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "mysql_normaltbl_oneGroup_oneAtom_twoIndex",
                    // "mysql_student_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "mysql_normaltbl_oneGroup_oneAtom_threeIndex",
                    // "mysql_student_oneGroup_oneAtom" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_oneGroup_mutilAtom", "mysql_student_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "mysql_normaltbl_oneGroup_mutilAtom_Index",
                    // "mysql_student_oneGroup_mutilAtom" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "mysql_normaltbl_oneGroup_mutilAtom_twoIndex",
                    // "mysql_student_oneGroup_mutilAtom" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // {
                    // "mysql_normaltbl_oneGroup_mutilAtom_twoIndex_Consistent",
                    // "mysql_student_oneGroup_mutilAtom" },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "mysql_normaltbl_oneGroup_mutilAtom_threeIndex",
                    // "mysql_student_oneGroup_mutilAtom" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_mutilGroup", "mysql_student_mutilGroup" },
            // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_normaltbl_mutilGroup_Index",
            // "mysql_student_mutilGroup" },
            // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
            // strongConsistent
            // { "mysql_normaltbl_mutilGroup_Index_Consistent",
            // "mysql_student_mutilGroup" },
            // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // { "mysql_normaltbl_mutilGroup_twoIndex",
            // "mysql_student_mutilGroup" },
            // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // 分库采用复杂分库规则
            // { "mysql_normaltbl_mutilGroup_twoIndex_complexRule",
            // "mysql_student_mutilGroup" },
            // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "mysql_normaltbl_mutilGroup_twoIndex_Consistent",
            // "mysql_student_mutilGroup" },
            // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "mysql_normaltbl_mutilGroup_threeIndex",
            // "mysql_student_mutilGroup" }
            };
            return object;
        } else if (db.equals("tddl")) {
            String[][] object = {

            { "mysql_normaltbl_oneGroup_oneAtom", "mysql_student_oneGroup_oneAtom" },
                    { "mysql_normaltbl_oneGroup_mutilAtom", "mysql_student_oneGroup_mutilAtom" },

                    { "mysql_normaltbl_mutilGroup", "mysql_student_mutilGroup" }, };
            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // [0]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "tdhs_normaltbl_oneGroup_oneAtom", "tdhs_student_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "tdhs_normaltbl_oneGroup_oneAtom_Index",
                    // "tdhs_student_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "tdhs_normaltbl_oneGroup_oneAtom_twoIndex",
                    // "tdhs_student_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "tdhs_normaltbl_oneGroup_oneAtom_threeIndex",
                    // "tdhs_student_oneGroup_oneAtom" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "tdhs_normaltbl_oneGroup_mutilAtom", "tdhs_student_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_Index",
                    // "tdhs_student_oneGroup_mutilAtom" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_twoIndex",
                    // "tdhs_student_oneGroup_mutilAtom" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // {
                    // "tdhs_normaltbl_oneGroup_mutilAtom_twoIndex_Consistent",
                    // "tdhs_student_oneGroup_mutilAtom" },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "tdhs_normaltbl_oneGroup_mutilAtom_threeIndex",
                    // "tdhs_student_oneGroup_mutilAtom" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "tdhs_normaltbl_mutilGroup", "tdhs_student_mutilGroup" },
            // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "tdhs_normaltbl_mutilGroup_Index",
            // "tdhs_student_mutilGroup" },
            // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
            // strongConsistent
            // { "tdhs_normaltbl_mutilGroup_Index_Consistent",
            // "tdhs_student_mutilGroup" },
            // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // { "tdhs_normaltbl_mutilGroup_twoIndex",
            // "tdhs_student_mutilGroup" },
            // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
            // 分库采用复杂分库规则
            // { "tdhs_normaltbl_mutilGroup_twoIndex_complexRule",
            // "tdhs_student_mutilGroup" },
            // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "tdhs_normaltbl_mutilGroup_twoIndex_Consistent",
            // "tdhs_student_mutilGroup" },
            // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "tdhs_normaltbl_mutilGroup_threeIndex",
            // "tdhs_student_mutilGroup" }
            };
            return object;
        } else {
            String[][] object = {
                    // [0]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "normaltbl_oneGroup_oneAtom", "student_oneGroup_oneAtom" },
                    // [1]bdb单裤单表 二级索引是一个字段组成
                    // { "normaltbl_oneGroup_oneAtom_Index",
                    // "student_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是二个字段组成
                    // { "normaltbl_oneGroup_oneAtom_twoIndex",
                    // "student_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是三个字段组成
                    // { "normaltbl_oneGroup_oneAtom_threeIndex",
                    // "student_oneGroup_oneAtom" },

                    // [4]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "normaltbl_oneGroup_mutilAtom", "student_oneGroup_mutilAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是一个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_Index",
                    // "student_oneGroup_mutilAtom" },
                    // [6]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是二个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex",
                    // "student_oneGroup_mutilAtom" },
                    // [7]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是两个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_oneGroup_mutilAtom_twoIndex_Consistent",
                    // "student_oneGroup_mutilAtom" },
                    // [8]bdb单裤多表（有分表情况，一个group对应多个atom）二级索引是三个字段组成
                    // { "normaltbl_oneGroup_mutilAtom_threeIndex",
                    // "student_oneGroup_mutilAtom" },

                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "normaltbl_mutilGroup", "student_mutilGroup" },
                    // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
                    // { "normaltbl_mutilGroup_Index", "student_mutilGroup" },
                    // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_mutilGroup_Index_Consistent",
                    // "student_mutilGroup" },
                    // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // { "normaltbl_mutilGroup_twoIndex", "student_mutilGroup"
                    // },
                    // [13]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // 分库采用复杂分库规则
                    // { "normaltbl_mutilGroup_twoIndex_complexRule",
                    // "student_mutilGroup" },
                    // [14]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
                    // strongConsistent
                    // { "normaltbl_mutilGroup_twoIndex_Consistent",
                    // "student_mutilGroup" },
                    // [15]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
                    // { "normaltbl_mutilGroup_threeIndex", "student_mutilGroup"
                    // },

                    // [16]mysql单裤单表 （一个group，对应一个atom）
                    { "mysql_normaltbl_oneGroup_oneAtom", "mysql_student_oneGroup_oneAtom" },
                    // [17]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_oneGroup_mutilAtom", "mysql_student_oneGroup_mutilAtom" },
                    // [18]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_normaltbl_mutilGroup", "mysql_student_mutilGroup" },
            // [19]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_normaltbl_mutilGroup_Index",
            // "mysql_student_mutilGroup" },
            // [20]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_normaltbl_mutilGroup_twoIndex",
            // "mysql_student_mutilGroup" }
            };
            return object;
        }

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
                    // [0]mysql单裤单表 （一个group，对应一个atom）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom" },
                    // [1]mysql单裤单表 二级索引是一个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_Index",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_Index" },
                    // [2]mysql单裤单表 二级索引是二个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_twoIndex",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_twoIndex" },
                    // [3]mysql单裤单表 二级索引是三个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_threeIndex",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_threeIndex" },

                    // [4]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_oneGroup_mutilAtom" },

                    // [5]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_host_info_mutilGroup", "mysql_hostgroup_info_mutilGroup" },
            // [6]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_host_info_mutilGroup_Index",
            // "mysql_hostgroup_info_mutilGroup_Index" },

            // [7]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是两个个字段组成
            // { "mysql_host_info_mutilGroup_twoIndex",
            // "mysql_hostgroup_info_mutilGroup_twoIndex" },
            // [8]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "mysql_host_info_mutilGroup_twoIndex_Consistent",
            // "mysql_hostgroup_info_mutilGroup_twoIndex_Consistent" },
            // [9]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "mysql_host_info_mutilGroup_threeIndex",
            // "mysql_hostgroup_info_mutilGroup_threeIndex" },
            // [10*]mysql多裤多表（主表和索引表不在同一个库里面） 二级索引是一个字段组成
            // { "mysql_host_info_mutilGroup_Index_difgroup",
            // "mysql_hostgroup_info_mutilGroup_Index_difgroup" },
            // [11*]mysql多裤多表(主表和索引表不在同一个库里面)二级索引是二个字段组成
            // { "mysql_host_info_mutilGroup_twoIndex_difgroup",
            // "mysql_hostgroup_info_mutilGroup_twoIndex_difgroup" },
            // [12*]mysql多裤多表（主表和索引表不在同一个库里面） 二级索引是三个字段组成
            // { "mysql_host_info_mutilGroup_threeIndex_difgroup",
            // "mysql_hostgroup_info_mutilGroup_threeIndex_difgroup" },
            };

            return object;
        } else {
            String[][] object = {
                    // [0]mysql单裤单表 （一个group，对应一个atom）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom" },
                    // [1]mysql单裤单表 二级索引是一个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_Index",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_Index" },
                    // [2]mysql单裤单表 二级索引是二个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_twoIndex",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_twoIndex" },
                    // [3]mysql单裤单表 二级索引是三个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_threeIndex",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_threeIndex" },

                    // [4]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_oneGroup_mutilAtom" },

                    // [5]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_host_info_mutilGroup", "mysql_hostgroup_info_mutilGroup" },
            // [6]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
            // { "mysql_host_info_mutilGroup_Index",
            // "mysql_hostgroup_info_mutilGroup_Index" },

            // [7]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是两个个字段组成
            // { "mysql_host_info_mutilGroup_twoIndex",
            // "mysql_hostgroup_info_mutilGroup_twoIndex" },
            // [8]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
            // strongConsistent
            // { "mysql_host_info_mutilGroup_twoIndex_Consistent",
            // "mysql_hostgroup_info_mutilGroup_twoIndex_Consistent" },
            // [9]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
            // { "mysql_host_info_mutilGroup_threeIndex",
            // "mysql_hostgroup_info_mutilGroup_threeIndex" },
            // [10*]mysql多裤多表（主表和索引表不在同一个库里面） 二级索引是一个字段组成
            // { "mysql_host_info_mutilGroup_Index_difgroup",
            // "mysql_hostgroup_info_mutilGroup_Index_difgroup" },
            // [11*]mysql多裤多表(主表和索引表不在同一个库里面)二级索引是二个字段组成
            // { "mysql_host_info_mutilGroup_twoIndex_difgroup",
            // "mysql_hostgroup_info_mutilGroup_twoIndex_difgroup" },
            // [12*]mysql多裤多表（主表和索引表不在同一个库里面） 二级索引是三个字段组成
            // { "mysql_host_info_mutilGroup_threeIndex_difgroup",
            // "mysql_hostgroup_info_mutilGroup_threeIndex_difgroup" },
            };
            return object;
        }
    }

    /**
     * 五张表，表名分别为host_infoTableName，hostgroup_infoTableName
     * studentTableName，module_infoTableName，module_hostTableName
     */
    public static String[][] hostinfoHostgoupStudentModuleinfoModulehostTable(String db) {
        if (db.equals("bdb")) {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "host_info_oneGroup_oneAtom", "hostgroup_info_oneGroup_oneAtom", "student_oneGroup_oneAtom",
                            "module_info_oneGroup_oneAtom", "module_host_oneGroup_oneAtom" },
                    // [1]bdb单裤单表
                    // （一个group，对应一个atom,其中mysql_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "host_info_oneGroup_oneAtom", "hostgroup_info_oneGroup_oneAtom_otherDb",
                            "student_oneGroup_oneAtom", "module_info_oneGroup_oneAtom", "module_host_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是一个字段组成
                    // { "host_info_oneGroup_oneAtom_Index",
                    // "hostgroup_info_oneGroup_oneAtom_Index",
                    // "student_oneGroup_oneAtom",
                    // "module_info_oneGroup_oneAtom",
                    // "module_host_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是二个字段组成
                    // { "host_info_oneGroup_oneAtom_twoIndex",
                    // "hostgroup_info_oneGroup_oneAtom_twoIndex",
                    // "student_oneGroup_oneAtom",
                    // "module_info_oneGroup_oneAtom",
                    // "module_host_oneGroup_oneAtom" },
                    // [4]bdb单裤单表 二级索引是三个字段组成
                    // { "host_info_oneGroup_oneAtom_threeIndex",
                    // "hostgroup_info_oneGroup_oneAtom_threeIndex",
                    // "student_oneGroup_oneAtom",
                    // "module_info_oneGroup_oneAtom",
                    // "module_host_oneGroup_oneAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "host_info_oneGroup_mutilAtom", "hostgroup_info_oneGroup_mutilAtom",
                            "student_oneGroup_mutilAtom", "module_info_oneGroup_mutilAtom",
                            "module_host_oneGroup_mutilAtom" },
                    // [6]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "host_info_mutilGroup", "hostgroup_info_mutilGroup", "student_mutilGroup",
                            "module_info_mutilGroup", "module_host_oneGroup_mutilAtom" },
                    // [7]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
                    // { "host_info_mutilGroup_Index",
                    // "hostgroup_info_mutilGroup_Index",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // [8]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // { "host_info_mutilGroup_twoIndex",
                    // "hostgroup_info_mutilGroup_twoIndex",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
                    // strongConsistent
                    // { "host_info_mutilGroup_twoIndex_Consistent",
                    // "hostgroup_info_mutilGroup_twoIndex_Consistent",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
                    // { "host_info_mutilGroup_threeIndex",
                    // "hostgroup_info_mutilGroup_threeIndex",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成,
                    // 其中host_info_mutilGroup带索引，hostgroup_info_mutilGroup不带索引
                    // { "host_info_mutilGroup_twoIndex",
                    // "hostgroup_info_mutilGroup", "student_mutilGroup",
                    // "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // 其中host_info_mutilGroup不带索引，hostgroup_info_mutilGroup带索引
                    { "host_info_oneGroup_mutilAtom", "hostgroup_info_mutilGroup", "student_mutilGroup",
                            "module_info_mutilGroup", "module_host_oneGroup_mutilAtom" } };
            return object;
        } else if (db.equals("mysql")) {
            String[][] object = {
                    // [0]mysql单裤单表 （一个group，对应一个atom）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom",
                            "mysql_student_oneGroup_oneAtom", "mysql_module_info_oneGroup_oneAtom",
                            "mysql_module_host_oneGroup_oneAtom" },
                    // [1]mysql单裤单表
                    // （一个group，对应一个atom,其中mysql_mysql_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom_otherDb",
                            "mysql_student_oneGroup_oneAtom", "mysql_module_info_oneGroup_oneAtom",
                            "mysql_module_host_oneGroup_oneAtom" },
                    // [2]mysql单裤单表 二级索引是一个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_Index",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_Index",
                    // "mysql_student_oneGroup_oneAtom",
                    // "mysql_module_info_oneGroup_oneAtom",
                    // "mysql_module_host_oneGroup_oneAtom" },
                    // [3]mysql单裤单表 二级索引是二个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_twoIndex",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_twoIndex",
                    // "mysql_student_oneGroup_oneAtom",
                    // "mysql_module_info_oneGroup_oneAtom",
                    // "mysql_module_host_oneGroup_oneAtom" },
                    // [4]mysql单裤单表 二级索引是三个字段组成
                    // { "mysql_host_info_oneGroup_oneAtom_threeIndex",
                    // "mysql_hostgroup_info_oneGroup_oneAtom_threeIndex",
                    // "mysql_student_oneGroup_oneAtom",
                    // "mysql_module_info_oneGroup_oneAtom",
                    // "mysql_module_host_oneGroup_oneAtom" },
                    // [5]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_oneGroup_mutilAtom",
                            "mysql_student_oneGroup_mutilAtom", "mysql_module_info_oneGroup_mutilAtom",
                            "mysql_module_host_oneGroup_mutilAtom" },
                    // [6]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_host_info_mutilGroup", "mysql_hostgroup_info_mutilGroup", "mysql_student_mutilGroup",
                            "mysql_module_info_mutilGroup", "mysql_module_host_oneGroup_mutilAtom" },
                    // [7]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
                    // { "mysql_host_info_mutilGroup_Index",
                    // "mysql_hostgroup_info_mutilGroup_Index",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [8*]mysql多裤多表（主表和索引表不在同一个库里面） 二级索引是一个字段组成
                    // { "mysql_host_info_mutilGroup_Index_difgroup",
                    // "mysql_hostgroup_info_mutilGroup_Index_difgroup",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [9]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // { "mysql_host_info_mutilGroup_twoIndex",
                    // "mysql_hostgroup_info_mutilGroup_twoIndex",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [10*]mysql多裤多表(主表和索引表不在同一个库里面)二级索引是二个字段组成
                    // {
                    // "mysql_host_info_mutilGroup_twoIndex_difgroup",
                    // "mysql_hostgroup_info_mutilGroup_twoIndex_difgroup",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [11]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
                    // strongConsistent
                    // {
                    // "mysql_host_info_mutilGroup_twoIndex_Consistent",
                    // "mysql_hostgroup_info_mutilGroup_twoIndex_Consistent",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [12]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
                    // { "mysql_host_info_mutilGroup_threeIndex",
                    // "mysql_hostgroup_info_mutilGroup_threeIndex",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [13*]mysql多裤多表（主表和索引表不在同一个库里面） 二级索引是三个字段组成
                    // {
                    // "mysql_host_info_mutilGroup_threeIndex_difgroup",
                    // "mysql_hostgroup_info_mutilGroup_threeIndex_difgroup",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [14]MYSQL多裤多表（有分库分表情况，一个GROUP对应多个ATOM） 二级索引是二个字段组成,
                    // 其中mysql_host_info_mutilGroup带索引，mysql_hostgroup_info_mutilGroup不带索引
                    // { "mysql_host_info_mutilGroup_twoIndex",
                    // "mysql_hostgroup_info_mutilGroup",
                    // "mysql_student_mutilGroup",
                    // "mysql_module_info_mutilGroup",
                    // "mysql_module_host_oneGroup_mutilAtom" },
                    // [15]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // 其中mysql_host_info_mutilGroup不带索引，mysql_hostgroup_info_mutilGroup带索引
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_mutilGroup",
                            "mysql_student_mutilGroup", "mysql_module_info_mutilGroup",
                            "mysql_module_host_oneGroup_mutilAtom" }

            };

            return object;
        } else if (db.equals("tdhs")) {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "tdhs_host_info_oneGroup_oneAtom", "tdhs_hostgroup_info_oneGroup_oneAtom",
                            "tdhs_student_oneGroup_oneAtom", "tdhs_module_info_oneGroup_oneAtom",
                            "tdhs_module_host_oneGroup_oneAtom" },
                    // [1]bdb单裤单表
                    // （一个group，对应一个atom,其中tdhs_tdhs_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "tdhs_host_info_oneGroup_oneAtom", "tdhs_hostgroup_info_oneGroup_oneAtom_otherDb",
                            "tdhs_student_oneGroup_oneAtom", "tdhs_module_info_oneGroup_oneAtom",
                            "tdhs_module_host_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是一个字段组成
                    // { "tdhs_host_info_oneGroup_oneAtom_Index",
                    // "tdhs_hostgroup_info_oneGroup_oneAtom_Index",
                    // "tdhs_student_oneGroup_oneAtom",
                    // "tdhs_module_info_oneGroup_oneAtom",
                    // "tdhs_module_host_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是二个字段组成
                    // { "tdhs_host_info_oneGroup_oneAtom_twoIndex",
                    // "tdhs_hostgroup_info_oneGroup_oneAtom_twoIndex",
                    // "tdhs_student_oneGroup_oneAtom",
                    // "tdhs_module_info_oneGroup_oneAtom",
                    // "tdhs_module_host_oneGroup_oneAtom" },
                    // [4]bdb单裤单表 二级索引是三个字段组成
                    // { "tdhs_host_info_oneGroup_oneAtom_threeIndex",
                    // "tdhs_hostgroup_info_oneGroup_oneAtom_threeIndex",
                    // "tdhs_student_oneGroup_oneAtom",
                    // "tdhs_module_info_oneGroup_oneAtom",
                    // "tdhs_module_host_oneGroup_oneAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "tdhs_host_info_oneGroup_mutilAtom", "tdhs_hostgroup_info_oneGroup_mutilAtom",
                            "tdhs_student_oneGroup_mutilAtom", "tdhs_module_info_oneGroup_mutilAtom",
                            "tdhs_module_host_oneGroup_mutilAtom" },
                    // [6]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "tdhs_host_info_mutilGroup", "tdhs_hostgroup_info_mutilGroup", "tdhs_student_mutilGroup",
                            "tdhs_module_info_mutilGroup", "tdhs_module_host_oneGroup_mutilAtom" },
                    // [7]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
                    // { "tdhs_host_info_mutilGroup_Index",
                    // "tdhs_hostgroup_info_mutilGroup_Index",
                    // "tdhs_student_mutilGroup",
                    // "tdhs_module_info_mutilGroup",
                    // "tdhs_module_host_oneGroup_mutilAtom" },
                    // [8]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // { "tdhs_host_info_mutilGroup_twoIndex",
                    // "tdhs_hostgroup_info_mutilGroup_twoIndex",
                    // "tdhs_student_mutilGroup",
                    // "tdhs_module_info_mutilGroup",
                    // "tdhs_module_host_oneGroup_mutilAtom" },
                    // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
                    // strongConsistent
                    // {
                    // "tdhs_host_info_mutilGroup_twoIndex_Consistent",
                    // "tdhs_hostgroup_info_mutilGroup_twoIndex_Consistent",
                    // "tdhs_student_mutilGroup",
                    // "tdhs_module_info_mutilGroup",
                    // "tdhs_module_host_oneGroup_mutilAtom" },
                    // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
                    // { "tdhs_host_info_mutilGroup_threeIndex",
                    // "tdhs_hostgroup_info_mutilGroup_threeIndex",
                    // "tdhs_student_mutilGroup",
                    // "tdhs_module_info_mutilGroup",
                    // "tdhs_module_host_oneGroup_mutilAtom" },
                    // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成,
                    // 其中tdhs_host_info_mutilGroup带索引，tdhs_hostgroup_info_mutilGroup不带索引
                    // { "tdhs_host_info_mutilGroup_twoIndex",
                    // "tdhs_hostgroup_info_mutilGroup",
                    // "tdhs_student_mutilGroup",
                    // "tdhs_module_info_mutilGroup",
                    // "tdhs_module_host_oneGroup_mutilAtom" },
                    // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // 其中tdhs_host_info_mutilGroup不带索引，tdhs_hostgroup_info_mutilGroup带索引
                    { "tdhs_host_info_oneGroup_mutilAtom", "tdhs_hostgroup_info_mutilGroup", "tdhs_student_mutilGroup",
                            "tdhs_module_info_mutilGroup", "tdhs_module_host_oneGroup_mutilAtom" } };
            return object;
        } else {
            String[][] object = {
                    // [0]bdb单裤单表 （一个group，对应一个atom）
                    { "host_info_oneGroup_oneAtom", "hostgroup_info_oneGroup_oneAtom", "student_oneGroup_oneAtom",
                            "module_info_oneGroup_oneAtom", "module_host_oneGroup_oneAtom" },
                    // [1]bdb单裤单表
                    // （一个group，对应一个atom,其中mysql_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "host_info_oneGroup_oneAtom", "hostgroup_info_oneGroup_oneAtom_otherDb",
                            "student_oneGroup_oneAtom", "module_info_oneGroup_oneAtom", "module_host_oneGroup_oneAtom" },
                    // [2]bdb单裤单表 二级索引是一个字段组成
                    // { "host_info_oneGroup_oneAtom_Index",
                    // "hostgroup_info_oneGroup_oneAtom_Index",
                    // "student_oneGroup_oneAtom",
                    // "module_info_oneGroup_oneAtom",
                    // "module_host_oneGroup_oneAtom" },
                    // [3]bdb单裤单表 二级索引是二个字段组成
                    // { "host_info_oneGroup_oneAtom_twoIndex",
                    // "hostgroup_info_oneGroup_oneAtom_twoIndex",
                    // "student_oneGroup_oneAtom",
                    // "module_info_oneGroup_oneAtom",
                    // "module_host_oneGroup_oneAtom" },
                    // // [4]bdb单裤单表 二级索引是三个字段组成
                    // { "host_info_oneGroup_oneAtom_threeIndex",
                    // "hostgroup_info_oneGroup_oneAtom_threeIndex",
                    // "student_oneGroup_oneAtom",
                    // "module_info_oneGroup_oneAtom",
                    // "module_host_oneGroup_oneAtom" },
                    // [5]bdb单裤多表（有分表情况，一个group对应多个atom）
                    { "host_info_oneGroup_mutilAtom", "hostgroup_info_oneGroup_mutilAtom",
                            "student_oneGroup_mutilAtom", "module_info_oneGroup_mutilAtom",
                            "module_host_oneGroup_mutilAtom" },
                    // [6]bdb多裤多表（有分库分表情况，一个group对应多个atom）
                    { "host_info_mutilGroup", "hostgroup_info_mutilGroup", "student_mutilGroup",
                            "module_info_mutilGroup", "module_host_oneGroup_mutilAtom" },
                    // [7]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是一个字段组成
                    // { "host_info_mutilGroup_Index",
                    // "hostgroup_info_mutilGroup_Index",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // // [8]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // { "host_info_mutilGroup_twoIndex",
                    // "hostgroup_info_mutilGroup_twoIndex",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // // [9]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成 二级索引是
                    // // strongConsistent
                    // { "host_info_mutilGroup_twoIndex_Consistent",
                    // "hostgroup_info_mutilGroup_twoIndex_Consistent",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // // [10]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是三个字段组成
                    // { "host_info_mutilGroup_threeIndex",
                    // "hostgroup_info_mutilGroup_threeIndex",
                    // "student_mutilGroup", "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // // [11]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成,
                    // //
                    // 其中host_info_mutilGroup带索引，hostgroup_info_mutilGroup不带索引
                    // { "host_info_mutilGroup_twoIndex",
                    // "hostgroup_info_mutilGroup", "student_mutilGroup",
                    // "module_info_mutilGroup",
                    // "module_host_oneGroup_mutilAtom" },
                    // [12]bdb多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是二个字段组成
                    // 其中host_info_mutilGroup不带索引，hostgroup_info_mutilGroup带索引
                    { "host_info_oneGroup_mutilAtom", "hostgroup_info_mutilGroup", "student_mutilGroup",
                            "module_info_mutilGroup", "module_host_oneGroup_mutilAtom" },
                    // [13]mysql单裤单表 （一个group，对应一个atom）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom",
                            "mysql_student_oneGroup_oneAtom", "mysql_module_info_oneGroup_oneAtom",
                            "mysql_module_host_oneGroup_oneAtom" },
                    // [14]mysql单裤单表
                    // （一个group，对应一个atom,其中mysql_hostgroup_info_oneGroup_oneAtom_otherDb在不同的数据库中）
                    { "mysql_host_info_oneGroup_oneAtom", "mysql_hostgroup_info_oneGroup_oneAtom_otherDb",
                            "mysql_student_oneGroup_oneAtom", "mysql_module_info_oneGroup_oneAtom",
                            "mysql_module_host_oneGroup_oneAtom" },
                    // [15]mysql单裤多表（有分表情况，一个group对应多个atom）
                    { "mysql_host_info_oneGroup_mutilAtom", "mysql_hostgroup_info_oneGroup_mutilAtom",
                            "mysql_student_oneGroup_mutilAtom", "mysql_module_info_oneGroup_mutilAtom",
                            "mysql_module_host_oneGroup_mutilAtom" },
                    // [16]mysql多裤多表（有分库分表情况，一个group对应多个atom）
                    { "mysql_host_info_mutilGroup", "mysql_hostgroup_info_mutilGroup", "mysql_student_mutilGroup",
                            "mysql_module_info_mutilGroup", "mysql_module_host_oneGroup_mutilAtom" },
            // [17]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是两个字段组成
            // { "mysql_host_info_mutilGroup_twoIndex",
            // "mysql_hostgroup_info_mutilGroup_twoIndex",
            // "mysql_student_mutilGroup",
            // "mysql_module_info_mutilGroup",
            // "mysql_module_host_oneGroup_mutilAtom" },
            // // [18]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是两个字段组成
            // // 其中host_info_mutilGroup是带索引，tdhs_hostgroup_info_mutilGroup不带索引
            // { "mysql_host_info_mutilGroup_twoIndex",
            // "mysql_hostgroup_info_mutilGroup",
            // "mysql_student_mutilGroup",
            // "mysql_module_info_mutilGroup",
            // "mysql_module_host_oneGroup_mutilAtom" },
            // // [19]mysql多裤多表（有分库分表情况，一个group对应多个atom） 二级索引是两个字段组成
            // // 其中host_info_mutilGroup是不带索引，tdhs_hostgroup_info_mutilGroup带索引
            // { "mysql_host_info_mutilGroup",
            // "mysql_hostgroup_info_mutilGroup_twoIndex",
            // "mysql_student_mutilGroup",
            // "mysql_module_info_mutilGroup",
            // "mysql_module_host_oneGroup_mutilAtom" }
            };
            return object;
        }

    }

}
