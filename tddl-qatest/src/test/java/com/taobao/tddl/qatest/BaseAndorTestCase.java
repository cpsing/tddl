package com.taobao.tddl.qatest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.matrix.jdbc.TDataSource;

/**
 * 基本测试类，提供创建AndorServer和AndorClient方法
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-2-16 下午2:05:24
 */
@SuppressWarnings("rawtypes")
public class BaseAndorTestCase extends BaseTestCase {

    protected static final ExecutorService pool                    = Executors.newCachedThreadPool();
    protected static PrepareData           prepareData             = new PrepareData();
    private static String                  ruleFile                = "V0#classpath:client/";
    private static String                  rule                    = "rule.xml";

    private static String                  schemaFile              = "client/";
    private static String                  schema                  = "schema.xml";
    // dbType为mysql运行mysql测试，bdb值为bdb运行bdb测试，如果为空则运行bdb和mysql测试

    protected static String                dbType                  = null;

    protected static boolean               needPerparedData        = true;
    private static String                  machineTapologyFile     = "src/test/resources/client/server_topology.xml";
    private static String                  machineTapologyAyncFile = "src/test/resources/client/server_async_topology.xml";

    // --------------server的配置文件路径
    private static String                  groupAtomServer         = "server/server_group_atom.properties";
    private static String                  group0Atom0Server       = "server/server_group_0_atom0.properties";
    private static String                  group0Atom1Server       = "server/server_group_0_atom1.properties";
    private static String                  group0Atom2Server       = "server/server_group_0_atom2.properties";
    private static String                  group1Atom0Server       = "server/server_group_1_atom0.properties";
    private static String                  group1Atom1Server       = "server/server_group_1_atom1.properties";
    private static String                  group1Atom2Server       = "server/server_group_1_atom2.properties";
    private static String                  group2Atom0Server       = "server/server_group_2_atom0.properties";
    private static String                  group2Atom1Server       = "server/server_group_2_atom1.properties";

    private static String                  typeFile                = "db_type.properties";

    static {
        dbType = LoadPropsUtil.loadProps2Str(typeFile).getProperty("dbType");
    }

    @BeforeClass
    public static void IEnvInit() throws Exception {
        if (us == null) {
            if (dbType.equals("bdb") || dbType == "") {
                JDBCClient(dbType);
            } else if (dbType.equals("mysql") || dbType.equals("tdhs") || dbType.equals("tddl")
                       || dbType.equals("hbase")) {
                JDBCClient(dbType, false);
            }
        }
    }

    public static void JDBCClient(String dbType) throws Exception {
        JDBCClient(dbType, false);
    }

    public static void JDBCClient(String dbTypeStack, boolean async) throws Exception {
        us = new TDataSource();

        if ("tddl".equalsIgnoreCase(dbTypeStack) || "mysql".equalsIgnoreCase(dbTypeStack)) {
            us.setAppName("andor_mysql_qatest");
        } else if ("tdhs".equalsIgnoreCase(dbTypeStack)) {
            us.setAppName("andor_tdhs_qatest");
        } else if ("hbase".equalsIgnoreCase(dbTypeStack)) {
            us.setAppName("andor_hbase_qatest");
        }

        us.setRuleFile(ruleFile + dbTypeStack + "_" + rule);

        if ("tddl".equalsIgnoreCase(dbTypeStack)) {
        }
        if ((!"tddl".equalsIgnoreCase(dbTypeStack)) && (!"tdhs".equalsIgnoreCase(dbTypeStack))) {
            us.setTopologyFile(machineTapologyFile);
            us.setSchemaFile(schemaFile + dbTypeStack + "_" + schema);
        }

        Map<String, Comparable> cp = new HashMap<String, Comparable>();
        if ("tdhs".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.ConnectionExtraCmd.USE_TDHS_FOR_DEFAULT, "true");
        }
        if (async && "mysql".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.ExecutionExtraCmd.ALLOW_BIO, "FALSE");
            us.setTopologyFile(machineTapologyAyncFile);
        }

        if ("hbase".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.ExecutionExtraCmd.HBASE_MAPPING_FILE, "client/hbase_mapping.xml");
        }
        // cp.put(ExtraCmd.ConnectionExtraCmd.INIT_TEMP_TABLE, "true");
        // cp.put(ExtraCmd.ConnectionExtraCmd.INIT_TDDL_DATASOURCE, "true");
        us.setConnectionProperties(cp);
        try {
            us.init();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("", e);
        }
    }

    public static JdbcTemplate JdbcTemplateClient(String dbType) throws Exception {
        us = new TDataSource();
        us.setRuleFile(ruleFile + dbType + "_" + rule);
        us.setTopologyFile(machineTapologyFile);
        us.setSchemaFile(schemaFile + dbType + "_" + schema);
        us.init();
        return new JdbcTemplate(us);
    }

}
