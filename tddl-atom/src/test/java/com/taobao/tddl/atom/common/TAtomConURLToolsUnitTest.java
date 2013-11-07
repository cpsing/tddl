package com.taobao.tddl.atom.common;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TAtomConURLToolsUnitTest {

    @Test
    public void getOracleConURL_拼装Oracle_URL() {
        String ociUrl = TAtomConURLTools.getOracleConURL("192.168.1.1", "1521", "ociTest", "oci");
        String actualOci = "jdbc:oracle:oci:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.1)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICAT)(SERVICE_NAME=ociTest)))";
        Assert.assertEquals(ociUrl, actualOci);

        String thinUrl = TAtomConURLTools.getOracleConURL("192.168.1.1", "1521", "thinTest", "thin");
        String actualThin = "jdbc:oracle:thin:@192.168.1.1:1521:thinTest";
        Assert.assertEquals(thinUrl, actualThin);
    }

    @Test
    public void getMySqlConURL_拼装MySql_URL() {
        String mysqlUrl1 = TAtomConURLTools.getMySqlConURL("192.168.1.1", "3306", "mysqlTest", null);
        String actualMySql1 = "jdbc:mysql://192.168.1.1:3306/mysqlTest?characterEncoding=gbk";
        Assert.assertEquals(mysqlUrl1, actualMySql1);

        Map<String, String> prams = new HashMap<String, String>();
        prams.put("key1", "value1");
        prams.put("key2", "value2");
        String mysqlUrl2 = TAtomConURLTools.getMySqlConURL("192.168.1.1", "3306", "mysqlTest", prams);
        String actualMySql2 = "jdbc:mysql://192.168.1.1:3306/mysqlTest?key2=value2&key1=value1";
        Assert.assertEquals(mysqlUrl2, actualMySql2);
    }
}
