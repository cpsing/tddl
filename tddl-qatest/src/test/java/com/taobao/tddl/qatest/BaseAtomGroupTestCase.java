package com.taobao.tddl.qatest;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.TAtomDataSource;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.atom.config.TAtomConfParser;
import com.taobao.tddl.atom.config.TAtomDsConfDO;
import com.taobao.tddl.config.ConfigServerHelper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.qatest.util.DateUtil;
import com.taobao.tddl.qatest.util.FixDataSource;
import com.taobao.tddl.qatest.util.LoadPropsUtil;

@Ignore(value = "提供初始化环境的实际方法")
public class BaseAtomGroupTestCase extends BaseTestCase {

    protected static final String     QATEST_DATASOURCE_PATH           = "classpath:atom/tddl_qatest_db.xml";
    protected static FixDataSource    fixDataSource;

    static {
        if (fixDataSource == null) {
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] { QATEST_DATASOURCE_PATH });
            fixDataSource = (FixDataSource) context.getBean("fixDataSource");
        }
    }

    public static final int           INTERVAL_TIME                    = 2;
    public static final int           SLEEP_TIME                       = INTERVAL_TIME * 3;
    public static final String        PASSWD_PATH_FULL                 = "atom/passwd.properties";
    public static final String        GLOBAL_PATH_SUFFIX               = "/global.properties";
    public static final String        APP_PATH_SUFFIX                  = "/app.properties";
    public static Map<String, String> dataMap                          = null;
    // --------------------mysql orcle共用
    public static final String        APPNAME                          = "tddl_qatest";
    public static final String        APPNAME_UTE                      = "UTE";
    public static final String        TDDL_DBGROUPS                    = "tddl_dbgroups";
    public static final String        PROPERTIES_FILE                  = ".properties";

    // ---------------------mysql
    public static final String        ATOM_PATH                        = "atom/";
    public static final String        GROUP_PATH                       = "group/";
    public static final String        MATRIX_PATH                      = "matrix/";
    public static final String        ATOM_NORMAL_0_PATH               = "atom/qatest_normal_0";
    public static final String        ATOM_NORMAL_1_PATH               = "atom/qatest_normal_1";
    public static final String        ATOM_NORMAL_2_PATH               = "atom/qatest_normal_2";
    public static final String        ATOM_NORMAL_0_BAC_PATH           = "atom/qatest_normal_0_bac";
    public static final String        ATOM_NORMAL_1_BAC_PATH           = "atom/qatest_normal_1_bac";
    public static final String        ATOM_NORMAL_2_BAC_PATH           = "atom/qatest_normal_2_bac";
    public static final String        GROUP_NORMAL_0_PATH              = "group/tddl_group_0.properties";
    public static final String        GROUP_NORMAL_1_PATH              = "group/tddl_group_1.properties";
    public static final String        GROUP_NORMAL_2_PATH              = "group/tddl_group_2.properties";
    public static final String        GROUP_NORMAL_COMPLEX_PATH        = "group/tddl_group_complex.properties";

    public static final String        MATRIX_DBGROUPS_PATH             = "matrix/tddl_dbgroups.properties";
    public static final String        MATRIX_SQLEXECUTOR_PATH          = "matrix/rules/tddl_sqlexecutor.properties";
    public static final String        MATRIX_RULE_PROPS_PATH           = "matrix/rules/tddl_rule.properties";
    public static final String        MATRIX_RULE_XML_PATH             = "classpath:matrix/rules/tddl_rule.xml";
    public static final String        MATRIX_IBATIS_CONTEXT_PATH       = "classpath:matrix/ibatis/spring_context.xml";
    public static final String        MATRIX_IBATIS_RULE_PATH          = "classpath:matrix/ibatis/tddl_rule_complex.xml";
    public static final String        MATRIX_RULE_FULLTBLSCAN_PATH     = "matrix/rules/tddl_rule_fulltblscan.properties";
    public static final String        DBKEY_0                          = "qatest_normal_0";
    public static final String        DBKEY_1                          = "qatest_normal_1";
    public static final String        DBKEY_2                          = "qatest_normal_2";
    public static final String        DBKEY_0_BAC                      = "qatest_normal_0_bac";
    public static final String        DBKEY_1_BAC                      = "qatest_normal_1_bac";
    public static final String        DBKEY_2_BAC                      = "qatest_normal_2_bac";
    public static final String        GROUPKEY_0                       = "tddl_group_0";
    public static final String        GROUPKEY_1                       = "tddl_group_1";
    public static final String        GROUPKEY_2                       = "tddl_group_2";
    public static final String        GROUPKEY_COMPLEX                 = "tddl_group_complex";
    public static final String        DBTYPE_MYSQL                     = "mysql";

    // ----------------------------oracle
    protected static final String     DBTYPE_ORACLE                    = "oracle";
    protected static final String     DBKEY_ORA_0                      = "qatest_normal_ora_0";
    protected static final String     DBKEY_ORA_1                      = "qatest_normal_ora_1";
    protected static final String     DBKEY_ORA_0_BAC                  = "qatest_normal_ora_0_bac";
    protected static final String     DBKEY_ORA_1_BAC                  = "qatest_normal_ora_1_bac";
    protected static final String     GROUPKEY_ORA_0                   = "tddl_group_ora_0";
    protected static final String     GROUPKEY_ORA_1                   = "tddl_group_ora_1";
    protected static final String     ATOM_ORA_PATH                    = "atom/oracle/";
    protected static final String     GROUP_ORA_PATH                   = "group/oracle/";
    protected static final String     MATRIX_DBGROUPS_ORA_PATH         = "matrix/groups/tddl_dbgroups_ora.properties";
    protected static final String     MATRIX_RULE_ORA_XML_PATH         = "classpath:matrix/rules/tddl_rule_ora.xml";
    protected static final String     MATRIX_RULE_ORA_FULLTBLSCAN_PATH = "matrix/rules/tddl_rule_fulltblscan_ora.properties";

    // -----------------------------公共变量
    protected static final int        RANDOM_ID                        = Integer.valueOf(RandomStringUtils.randomNumeric(8));
    protected static String           time                             = DateUtil.formatDate(new Date(),
                                                                           DateUtil.DATE_FULLHYPHEN);
    protected static String           nextDay                          = DateUtil.getDiffDate(1,
                                                                           DateUtil.DATE_FULLHYPHEN);
    protected static int              resultSetType                    = ResultSet.TYPE_FORWARD_ONLY;                              ;
    protected static int              resultSetConcurrency             = ResultSet.CONCUR_READ_ONLY;
    protected static int              holdablity                       = -1;
    protected static boolean          SOME_SHOULD_NOT_BE_TEST          = true;
    protected static boolean          ASTATICISM_TEST                  = true;

    // 新规则
    protected static final String     NEW_NUMBER_MOD_RULES_PATH        = "classpath:matrix/rules/newrules/number_mod_rules.xml";
    protected static final String     NEW_DATE_MOD_RULES_PATH          = "classpath:matrix/rules/newrules/date_mod_rule.xml";
    protected static final String     NEW_GROOVY_SCRIPT_RULES_PATH     = "classpath:matrix/rules/newrules/groovy_script_rule.xml";

    @BeforeClass
    public static void atomSetUp() {
        TAtomDataSource.cleanAllDataSource();
    }

    @AfterClass
    public static void atomTearDown() {
        TAtomDataSource.cleanAllDataSource();
    }

    protected static void initAtomConfig(String path, String appName, String dbKey) throws Exception {
        String globaStr = LoadPropsUtil.loadProps2Str(path + GLOBAL_PATH_SUFFIX);
        dataMap.put(TAtomConstants.getGlobalDataId(dbKey), globaStr);
        String appStr = LoadPropsUtil.loadProps2Str(path + APP_PATH_SUFFIX);
        dataMap.put(TAtomConstants.getAppDataId(appName, dbKey), appStr);
        TAtomDsConfDO tAtomDsConfDO = TAtomConfParser.parserTAtomDsConfDO(globaStr, appStr);
        String passwdStr = LoadPropsUtil.loadProps2Str(PASSWD_PATH_FULL);
        dataMap.put(TAtomConstants.getPasswdDataId(tAtomDsConfDO.getDbName(),
            tAtomDsConfDO.getDbType(),
            tAtomDsConfDO.getUserName()),
            passwdStr);
    }

    protected static void setMatrixMockInfo(String groupsPath, String key) throws Exception {
        setMatrixMockInfo(groupsPath, key, false);
    }

    protected static void setMatrixMockInfo(String groupsPath, String key, boolean isOracle) throws Exception {
        // -----------------获取groups信息
        String groupsStr = LoadPropsUtil.loadProps2OneLine(groupsPath, key);
        if (groupsStr == null || StringUtils.isBlank(groupsStr)) {
            throw new Exception("指定path = " + groupsPath + ",key = " + key + "的groups信息为null或者为空字符。");
        }

        // -----------------oracle or mysql
        String groupPath = BaseAtomGroupTestCase.GROUP_PATH;
        String atomPath = BaseAtomGroupTestCase.ATOM_PATH;
        if (isOracle) {
            groupPath = BaseAtomGroupTestCase.GROUP_ORA_PATH;
            atomPath = BaseAtomGroupTestCase.ATOM_ORA_PATH;
        }

        // -----------------获取group信息
        BaseAtomGroupTestCase.dataMap = new HashMap<String, String>();
        String[] groupArr = groupsStr.split(",");

        for (String group : groupArr) {
            group = group.trim();
            String groupStr = "";
            groupStr = LoadPropsUtil.loadProps2OneLine(groupPath + group + BaseAtomGroupTestCase.PROPERTIES_FILE, group);
            if (groupsStr != null && StringUtils.isNotBlank(groupsStr)) {
                // 获取atom信息
                String[] atomArr = groupStr.split(",");
                for (String atom : atomArr) {
                    atom = atom.trim();
                    atom = atom.substring(0, atom.indexOf(":"));
                    BaseAtomGroupTestCase.initAtomConfig(atomPath + atom, BaseAtomGroupTestCase.APPNAME, atom);
                }
                // 获取groupkey
                BaseAtomGroupTestCase.dataMap.put(TGroupDataSource.getFullDbGroupKey(group), groupStr);
            }
        }

        // -----------------dbgroups
        BaseAtomGroupTestCase.dataMap.put(new MessageFormat(ConfigServerHelper.DATA_ID_DB_GROUP_KEYS).format(new Object[] { BaseAtomGroupTestCase.APPNAME }),
            groupsStr);

        // 建立MockServer
        MockServer.setConfigInfos(BaseAtomGroupTestCase.dataMap);
    }

    protected static void clearData(JdbcTemplate tddlJTX, String sql, Object[] args) {
        if (args == null) {
            args = new Object[] {};
        }
        // 确保数据清除成功
        try {
            tddlJTX.update(sql, args);
        } catch (Exception e) {
            tddlJTX.update(sql, args);
        }
    }

    protected static void prepareData(JdbcTemplate tddlJTX, String sql, Object[] args) {
        if (args == null) {
            args = new Object[] {};
        }

        // 确保数据准备成功
        try {
            int rs = tddlJTX.update(sql, args);
            if (rs <= 0) {
                tddlJTX.update(sql, args);
            }
        } catch (Exception e) {
            int rs = tddlJTX.update(sql, args);
            if (rs <= 0) {
                tddlJTX.update(sql, args);
            }
        }
    }

    protected String buildupParamMarks(Object[] objArr) {
        StringBuilder marks = new StringBuilder();
        String comma = "";
        for (int i = 0; i < objArr.length; i++) {
            marks.append(comma).append("?");
            comma = ",";
        }
        return marks.toString();
    }

    protected String buildupParams(Object[] objArr) {
        StringBuilder params = new StringBuilder();
        String comma = "";
        for (Object id : objArr) {
            params.append(comma).append(id);
            comma = ",";
        }
        return params.toString();
    }

    /**
     * 取平均值
     */
    protected Double getAvg(Object[] objArr) {
        Double sum = new Double(0);
        for (Object id : objArr) {
            sum = sum + new Double(id.toString());
        }
        return sum * 1.0 / objArr.length;
    }

    /**
     * 取得总和值
     */
    protected Double getSum(Object[] objArr) {
        Double sum = new Double(0);
        for (Object id : objArr) {
            sum = sum + new Double(id.toString());
        }
        return sum;
    }

    /**
     * 取得总和值
     */
    protected int[] getSumWithOddOrEven(Integer[] idArr) {
        int[] result = new int[2];
        int oddSum = 0;
        int evenSum = 0;
        for (int id : idArr) {
            if (id / 4 % 2 == 0) {

                evenSum = evenSum + id;
            } else {
                oddSum = oddSum + id;
            }
        }

        result[0] = oddSum < evenSum ? oddSum : evenSum;
        result[1] = oddSum > evenSum ? oddSum : evenSum;
        return result;
    }

    protected Object getValue(Object target, String fieldName) {
        Class<? extends Object> targetClass = target.getClass();
        try {

            Field field = targetClass.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (Exception e) {
            try {
                targetClass = targetClass.getSuperclass();
                Field field = targetClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (Exception e1) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
