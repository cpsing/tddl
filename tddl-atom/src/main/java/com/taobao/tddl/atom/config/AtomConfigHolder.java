package com.taobao.tddl.atom.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;

public class AtomConfigHolder extends AbstractConfigDataHolder {

    private final String       appName;

    private final List<String> atomKeys;

    private final String       unitName;

    public AtomConfigHolder(String appName, List<String> atomKeys, String unitName){
        this.appName = appName;
        this.atomKeys = atomKeys;
        this.unitName = unitName;
    }

    public void init() {
        this.loadDelegateExtension();
        Map<String, String> fullGlobalKeys = getFullGlobalKeyMap(atomKeys);
        Map<String, String> globalResults = queryAndHold(values2List(fullGlobalKeys), unitName);

        Map<String, String> fullAppKeys = getFullAppKeyMap(atomKeys);
        Map<String, String> appKeyResults = queryAndHold(values2List(fullAppKeys), unitName);

        List<String> passWdKeys = new ArrayList<String>();
        for (String atomKey : atomKeys) {
            String globalValue = globalResults.get(fullGlobalKeys.get(atomKey));
            if (StringUtils.isEmpty(globalValue)) {
                throw new IllegalArgumentException("Global Config Is Null, AppName >> " + appName + " ## UnitName >> "
                                                   + unitName + " ## AtomKey >> " + atomKey);
            }
            Properties globalProperties = TAtomConfParser.parserConfStr2Properties(globalResults.get(fullGlobalKeys.get(atomKey)));
            String dbName = globalProperties.getProperty(TAtomConfParser.GLOBA_DB_NAME_KEY);
            String dbType = globalProperties.getProperty(TAtomConfParser.GLOBA_DB_TYPE_KEY);

            String appValue = appKeyResults.get(fullAppKeys.get(atomKey));
            if (StringUtils.isEmpty(appValue)) {
                throw new IllegalArgumentException("App Config Is Null, AppName >> " + appName + " ## UnitName >> "
                                                   + unitName + " ## AtomKey >> " + atomKey);
            }
            Properties dbKeyProperties = TAtomConfParser.parserConfStr2Properties(appValue);
            String userName = dbKeyProperties.getProperty(TAtomConfParser.APP_USER_NAME_KEY);

            passWdKeys.add(getPassWdKey(dbName, dbType, userName));
        }
        queryAndHold(passWdKeys, unitName);
    }

    private List<String> values2List(Map<String, String> map) {
        List<String> result = new ArrayList<String>();
        for (String string : map.values())
            result.add(string);
        return result;
    }

    private Map<String, String> getFullGlobalKeyMap(List<String> atomKeys) {
        Map<String, String> result = new HashMap<String, String>();
        for (String atomKey : atomKeys) {
            result.put(atomKey, TAtomConstants.getGlobalDataId(atomKey));
        }
        return result;
    }

    private Map<String, String> getFullAppKeyMap(List<String> atomKeys) {
        Map<String, String> result = new HashMap<String, String>();
        for (String atomKey : atomKeys) {
            result.put(atomKey, TAtomConstants.getAppDataId(appName, atomKey));
        }
        return result;
    }

    private String getPassWdKey(String dbName, String dbType, String userName) {
        return TAtomConstants.getPasswdDataId(dbName, dbType, userName);
    }

    public static void main(String[] args) {
        AtomConfigHolder holder = new AtomConfigHolder("JIECHEN_YUGONG_APP", Arrays.asList("yugong_test_1",
            "yugong_test_2"), null);
        holder.init();
        System.out.println("OUT");
    }
}
