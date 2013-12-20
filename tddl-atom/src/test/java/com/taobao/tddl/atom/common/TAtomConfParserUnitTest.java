package com.taobao.tddl.atom.common;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.atom.config.TAtomConfParser;
import com.taobao.tddl.atom.config.TAtomDsConfDO;
import com.taobao.tddl.atom.securety.impl.PasswordCoder;
import com.taobao.tddl.atom.utils.ConnRestrictEntry;

public class TAtomConfParserUnitTest {

    @Test
    public void parserTAtomDsConfDO_解析全局配置() throws IOException {
        String globaFile = "conf/oracle/globa.properties";
        String globaStr = PropLoadTestUtil.loadPropFile2String(globaFile);
        TAtomDsConfDO tAtomDsConfDO = TAtomConfParser.parserTAtomDsConfDO(globaStr, null);
        Properties prop = PropLoadTestUtil.loadPropFromFile(globaFile);
        Assert.assertEquals(tAtomDsConfDO.getIp(), prop.get(TAtomConfParser.GLOBA_IP_KEY));
        Assert.assertEquals(tAtomDsConfDO.getPort(), prop.get(TAtomConfParser.GLOBA_PORT_KEY));
        Assert.assertEquals(tAtomDsConfDO.getDbName(), prop.get(TAtomConfParser.GLOBA_DB_NAME_KEY));
        Assert.assertEquals(tAtomDsConfDO.getDbType(), prop.get(TAtomConfParser.GLOBA_DB_TYPE_KEY));
        Assert.assertEquals(tAtomDsConfDO.getDbStatus(), prop.get(TAtomConfParser.GLOBA_DB_STATUS_KEY));
    }

    @Test
    public void parserTAtomDsConfDO_解析应用配置() throws IOException {
        String appFile = "conf/oracle/app.properties";
        String appStr = PropLoadTestUtil.loadPropFile2String(appFile);
        TAtomDsConfDO tAtomDsConfDO = TAtomConfParser.parserTAtomDsConfDO(null, appStr);
        Properties prop = PropLoadTestUtil.loadPropFromFile(appFile);
        Assert.assertEquals(tAtomDsConfDO.getUserName(), prop.get(TAtomConfParser.APP_USER_NAME_KEY));
        Assert.assertEquals(tAtomDsConfDO.getOracleConType(), prop.get(TAtomConfParser.APP_ORACLE_CON_TYPE_KEY));
        Assert.assertEquals(String.valueOf(tAtomDsConfDO.getMinPoolSize()),
            prop.get(TAtomConfParser.APP_MIN_POOL_SIZE_KEY));
        Assert.assertEquals(String.valueOf(tAtomDsConfDO.getMaxPoolSize()),
            prop.get(TAtomConfParser.APP_MAX_POOL_SIZE_KEY));
        Assert.assertEquals(String.valueOf(tAtomDsConfDO.getIdleTimeout()),
            prop.get(TAtomConfParser.APP_IDLE_TIMEOUT_KEY));
        Assert.assertEquals(String.valueOf(tAtomDsConfDO.getBlockingTimeout()),
            prop.get(TAtomConfParser.APP_BLOCKING_TIMEOUT_KEY));
        Assert.assertEquals(String.valueOf(tAtomDsConfDO.getPreparedStatementCacheSize()),
            prop.get(TAtomConfParser.APP_PREPARED_STATEMENT_CACHE_SIZE_KEY));
        Map<String, String> connectionProperties = TAtomConfParser.parserConPropStr2Map(prop.getProperty(TAtomConfParser.APP_CON_PROP_KEY));
        Assert.assertEquals(tAtomDsConfDO.getConnectionProperties(), connectionProperties);
    }

    @Test
    public void parserPasswd_解析密码() throws IOException, InvalidKeyException, NoSuchAlgorithmException,
                                   NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
        String passwdFile = "conf/oracle/passwd.properties";
        String passwdStr = PropLoadTestUtil.loadPropFile2String(passwdFile);
        String passwd = TAtomConfParser.parserPasswd(passwdStr);
        Properties prop = PropLoadTestUtil.loadPropFromFile(passwdFile);
        String encPasswd = prop.getProperty(TAtomConfParser.PASSWD_ENC_PASSWD_KEY);
        String encPasswdKey = prop.getProperty(TAtomConfParser.PASSWD_ENC_KEY_KEY);
        String tmpEncPsswd = new PasswordCoder().encode(encPasswdKey, passwd);
        Assert.assertEquals(encPasswd, tmpEncPsswd);
    }

    @Test
    public void parseConnRestrictEntries_解析应用连接限制() {
        String connRestrictStr = "K1,K2,K3,,K4:80%; K5:60%; K6,K7,:90%; ,K8:1%; K9,:10; ,K10,K11:70%; *:16,50%; *:40%; *:,30%; ~:20;";
        List<ConnRestrictEntry> connRestrictEntries = TAtomConfParser.parseConnRestrictEntries(connRestrictStr, 30);
        for (ConnRestrictEntry connRestrictEntry : connRestrictEntries) {
            System.out.println(connRestrictEntry.toString());
        }
        Assert.assertEquals(10, connRestrictEntries.size());
        Assert.assertEquals(24, connRestrictEntries.get(0).getLimits());
        Assert.assertEquals(18, connRestrictEntries.get(1).getLimits());
        Assert.assertEquals(27, connRestrictEntries.get(2).getLimits());
        Assert.assertEquals(1, connRestrictEntries.get(3).getLimits());
        Assert.assertEquals(10, connRestrictEntries.get(4).getLimits());
        Assert.assertEquals(21, connRestrictEntries.get(5).getLimits());
        Assert.assertEquals(16, connRestrictEntries.get(6).getHashSize());
        Assert.assertEquals(15, connRestrictEntries.get(6).getLimits());
        Assert.assertEquals(1, connRestrictEntries.get(7).getHashSize());
        Assert.assertEquals(12, connRestrictEntries.get(7).getLimits());
        Assert.assertEquals(1, connRestrictEntries.get(8).getHashSize());
        Assert.assertEquals(9, connRestrictEntries.get(8).getLimits());
        Assert.assertEquals(20, connRestrictEntries.get(9).getLimits());
    }

}
