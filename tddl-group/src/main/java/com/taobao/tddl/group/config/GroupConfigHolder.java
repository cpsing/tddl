package com.taobao.tddl.group.config;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.config.ConfigServerHelper;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

/**
 * @author mengshi.sunmengshi 2013-11-12 下午5:16:38
 * @since 5.1.0
 */
public class GroupConfigHolder extends AbstractConfigDataHolder {

    private final String        appName;

    private final List<String>  groups;

    private final String        unitName;

    private static final String ATOM_CONFIG_HOLDER_NAME = "com.taobao.tddl.atom.config.AtomConfigHolder";

    public GroupConfigHolder(String appName, List<String> groups, String unitName){
        this.appName = appName;
        this.groups = groups;
        this.unitName = unitName;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void initSonHolder(List<String> atomKeys) throws Exception {
        Class sonHolderClass = Class.forName(ATOM_CONFIG_HOLDER_NAME);
        Constructor constructor = sonHolderClass.getConstructor(String.class, List.class, String.class);
        sonConfigDataHolder = (AbstractConfigDataHolder) constructor.newInstance(this.appName, atomKeys, this.unitName);
        sonConfigDataHolder.init();
    }

    public void init() {
        loadDelegateExtension();

        List<String> fullGroupKeys = getFullDbGroupKeys(groups);
        Map<String, String> queryResults = queryAndHold(fullGroupKeys, unitName);
        initExtraConfigs();

        List<String> atomKeys = new ArrayList<String>();
        for (Map.Entry<String, String> entry : queryResults.entrySet()) {
            if (StringUtils.isEmpty(entry.getValue())) {
                throw new IllegalArgumentException("Group Config Is Null, AppName >> " + appName + " ## UnitName >> "
                                                   + unitName + " ## GroupKey >> " + entry.getKey());
            }
            String[] dsWeightArray = entry.getValue().split(",");
            for (String inValue : dsWeightArray) {
                atomKeys.add(inValue.split(":")[0]);
            }
        }

        try {
            initSonHolder(atomKeys);
        } catch (Exception e) {
            throw new IllegalStateException("Init SonConfigHolder Error, Class is : " + ATOM_CONFIG_HOLDER_NAME, e);
        }
    }

    private void initExtraConfigs() {
        List<String> extraConfKeys = getExtraConfKeys(groups);
        extraConfKeys.add(ConfigServerHelper.getTddlConfigDataId(appName));
        queryAndHold(extraConfKeys, unitName);
    }

    private List<String> getExtraConfKeys(List<String> groupKeys) {
        List<String> result = new ArrayList<String>();
        for (String key : groupKeys) {
            result.add(getExtraConfKey(key));
        }
        return result;
    }

    private String getExtraConfKey(String groupKey) {
        return TGroupDataSource.EXTRA_PREFIX + groupKey + "." + appName;
    }

    private List<String> getFullDbGroupKeys(List<String> groupKeys) {
        List<String> result = new ArrayList<String>();
        for (String key : groupKeys) {
            result.add(getFullDbGroupKey(key));
        }
        return result;
    }

    private String getFullDbGroupKey(String groupKey) {
        return TGroupDataSource.PREFIX + groupKey;
    }

    public static void main(String[] args) {
        GroupConfigHolder holder = new GroupConfigHolder("JIECHEN_YUGONG_APP", Arrays.asList("YUGONG_TEST_APP_GROUP_1",
            "YUGONG_TEST_APP_GROUP_2"), null);
        holder.init();
        System.out.println("OUT");
    }
}
