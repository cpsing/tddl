package com.taobao.tddl.atom.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.model.Atom;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;

@Activate(name = "ATOM_CONFIG_HOLDER", order = 1)
public class AtomConfigHolder extends AbstractConfigDataHolder {

    private final String     appName;

    private final List<Atom> atoms;

    private final String     unitName;

    public AtomConfigHolder(String appName, List<Atom> atoms, String unitName){
        this.appName = appName;
        this.atoms = atoms;
        this.unitName = unitName;
    }

    public void init() {
        loadDelegateExtension();

        // 添加到当前holder配置，拦截对diamond的请求
        for (Atom atom : atoms) {
            addDatas(atom.getProperties());
        }

        Map<String, String> fullGlobalKeys = getFullGlobalKeyMap(atoms);
        Map<String, String> globalResults = queryAndHold(values2List(fullGlobalKeys), unitName);
        globalResults.putAll(configHouse);

        Map<String, String> fullAppKeys = getFullAppKeyMap(atoms);
        Map<String, String> appKeyResults = queryAndHold(values2List(fullAppKeys), unitName);
        appKeyResults.putAll(configHouse);

        List<String> passWdKeys = new ArrayList<String>();
        for (Atom atom : atoms) {
            String atomKey = atom.getName();
            String globalValue = globalResults.get(TAtomConstants.getGlobalDataId(atomKey));
            if (StringUtils.isEmpty(globalValue)) {
                throw new IllegalArgumentException("Global Config Is Null, AppName >> " + appName + " ## UnitName >> "
                                                   + unitName + " ## AtomKey >> " + atomKey);
            }
            Properties globalProperties = TAtomConfParser.parserConfStr2Properties(globalValue);
            String dbName = globalProperties.getProperty(TAtomConfParser.GLOBA_DB_NAME_KEY);
            String dbType = globalProperties.getProperty(TAtomConfParser.GLOBA_DB_TYPE_KEY);

            String appValue = appKeyResults.get(TAtomConstants.getAppDataId(appName, atomKey));
            if (StringUtils.isEmpty(appValue)) {
                throw new IllegalArgumentException("App Config Is Null, AppName >> " + appName + " ## UnitName >> "
                                                   + unitName + " ## AtomKey >> " + atomKey);
            }
            Properties dbKeyProperties = TAtomConfParser.parserConfStr2Properties(appValue);
            String userName = dbKeyProperties.getProperty(TAtomConfParser.APP_USER_NAME_KEY);

            String passwdKeyDataId = getPassWdKey(dbName, dbType, userName);
            if (!configHouse.containsKey(passwdKeyDataId)) {
                passWdKeys.add(getPassWdKey(dbName, dbType, userName));
            }
        }
        queryAndHold(passWdKeys, unitName);
    }

    private List<String> values2List(Map<String, String> map) {
        List<String> result = new ArrayList<String>();
        for (String string : map.values())
            result.add(string);
        return result;
    }

    private Map<String, String> getFullGlobalKeyMap(List<Atom> atoms) {
        Map<String, String> result = new HashMap<String, String>();
        for (Atom atom : atoms) {
            String globalDataId = TAtomConstants.getGlobalDataId(atom.getName());
            if (!configHouse.containsKey(globalDataId)) {
                result.put(atom.getName(), globalDataId);
            }
        }
        return result;
    }

    private Map<String, String> getFullAppKeyMap(List<Atom> atoms) {
        Map<String, String> result = new HashMap<String, String>();
        for (Atom atom : atoms) {
            String appDataId = TAtomConstants.getAppDataId(appName, atom.getName());
            if (!configHouse.containsKey(appDataId)) {
                result.put(atom.getName(), appDataId);
            }
        }
        return result;
    }

    private String getPassWdKey(String dbName, String dbType, String userName) {
        return TAtomConstants.getPasswdDataId(dbName, dbType, userName);
    }

}
