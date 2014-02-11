package com.taobao.tddl.group.config;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.model.Atom;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

/**
 * @author mengshi.sunmengshi 2013-11-12 下午5:16:38
 * @since 5.0.0
 */
public class GroupConfigHolder extends AbstractConfigDataHolder {

    private static final String ATOM_CONFIG_HOLDER_NAME = "com.taobao.tddl.atom.config.AtomConfigHolder";
    private final String        appName;
    private final List<Group>   groups;
    private final String        unitName;

    public GroupConfigHolder(String appName, List<Group> groups, String unitName){
        this.appName = appName;
        this.groups = groups;
        this.unitName = unitName;
    }

    protected void initSonHolder(List<Atom> atomKeys) throws Exception {
        Class sonHolderClass = Class.forName(ATOM_CONFIG_HOLDER_NAME);
        Constructor constructor = sonHolderClass.getConstructor(String.class, List.class, String.class);
        sonConfigDataHolder = (AbstractConfigDataHolder) constructor.newInstance(this.appName, atomKeys, this.unitName);
        sonConfigDataHolder.init();
        delegateDataHolder.setSonConfigDataHolder(sonConfigDataHolder);// 传递给deletegate，由它进行son传递
    }

    public void init() {
        loadDelegateExtension();

        // 添加到当前holder配置，拦截对diamond的请求
        for (Group group : groups) {
            addDatas(group.getProperties());
        }

        List<String> fullGroupKeys = getFullDbGroupKeys(groups);
        Map<String, String> queryResults = queryAndHold(fullGroupKeys, unitName);
        initExtraConfigs();

        queryResults.putAll(configHouse);
        List<Atom> atomKeys = new ArrayList<Atom>();
        for (Group group : groups) {
            String groupKey = group.getName();
            String atomConfig = queryResults.get(getFullDbGroupKey(groupKey));
            if (StringUtils.isEmpty(atomConfig)) {
                throw new IllegalArgumentException("Group Config Is Null, AppName >> " + appName + " ## UnitName >> "
                                                   + unitName + " ## GroupKey >> " + groupKey);
            }

            String[] dsWeightArray = atomConfig.split(",");
            for (String inValue : dsWeightArray) {
                String atomKey = inValue.split(":")[0];
                atomKeys.add(getOrCreateAtom(atomKey));
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
        // extraConfKeys.add(ConfigServerHelper.getTddlConfigDataId(appName));
        queryAndHold(extraConfKeys, unitName);
    }

    private List<String> getExtraConfKeys(List<Group> groupKeys) {
        List<String> result = new ArrayList<String>();
        for (Group group : groupKeys) {
            String groupExtraConfKey = getExtraConfKey(group.getName());
            if (!configHouse.containsKey(groupExtraConfKey)) {
                // 没有的配置才向远程取
                result.add(groupExtraConfKey);
            }
        }
        return result;
    }

    private String getExtraConfKey(String groupKey) {
        return TGroupDataSource.EXTRA_PREFIX + groupKey + "." + appName;
    }

    private List<String> getFullDbGroupKeys(List<Group> groupKeys) {
        List<String> result = new ArrayList<String>();
        for (Group group : groupKeys) {
            String groupKey = getFullDbGroupKey(group.getName());
            if (!configHouse.containsKey(groupKey)) {
                // 没有的配置才向远程取
                result.add(groupKey);
            }
        }
        return result;
    }

    private String getFullDbGroupKey(String groupKey) {
        return TGroupDataSource.PREFIX + groupKey;
    }

    private Atom getOrCreateAtom(String name) {
        for (Group group : groups) {
            Atom atom = group.getAtom(name);
            if (atom != null) {
                return atom;
            }
        }

        Atom atom = new Atom();
        atom.setName(name);
        return atom;
    }

}
