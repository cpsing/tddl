package com.taobao.tddl.config.diamond;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.diamond.client.BatchHttpResult;
import com.taobao.diamond.client.impl.DiamondEnv;
import com.taobao.diamond.client.impl.DiamondEnvRepo;
import com.taobao.diamond.client.impl.DiamondUnitSite;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;
import com.taobao.tddl.config.impl.holder.ConfigDataHolder;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class DiamondConfigHolder extends AbstractConfigDataHolder {

    private static final Logger   log                 = LoggerFactory.getLogger(DiamondConfigHolder.class);

    protected ConfigDataHolder    sonConfigDataHolder = null;

    protected Map<String, String> configHouse         = new HashMap<String, String>();

    protected Map<String, String> queryAndHold(List<String> dataIds, String unitName) {
        DiamondEnv env = null;

        // TODO 外部直接指定ip进行访问
        if (!TStringUtil.isEmpty(unitName)) {
            env = DiamondUnitSite.getDiamondUnitEnv(unitName);
        } else {
            env = DiamondEnvRepo.defaultEnv;
        }

        BatchHttpResult queryResult = env.batchQuery(dataIds, "DEFAULT_GROUP", 30000);
        Map<String, String> result = new HashMap<String, String>();
        if (queryResult.isSuccess()) {
            List<ConfigInfoEx> configs = queryResult.getResult();
            for (ConfigInfoEx config : configs) {
                result.put(config.getDataId(), config.getContent());
            }
            addDatas(result);
        } else {
            log.warn("Batch Query Failed. Status Code is " + queryResult.getStatusCode());
        }
        return result;
    }

    protected void addDatas(Map<String, String> confMap) {
        configHouse.putAll(confMap);
    }

    @Override
    public Map<String, String> getData(List<String> dataIds) {
        Map<String, String> result = new HashMap<String, String>();
        for (String dataId : dataIds) {
            result.put(dataId, getData(dataId));
        }
        return result;
    }

    @Override
    public String getData(String dataId) {
        return configHouse.containsKey(dataId) ? configHouse.get(dataId) : getDataFromSonHolder(dataId);
    }

    protected String getDataFromSonHolder(String dataId) {
        return sonConfigDataHolder == null ? null : sonConfigDataHolder.getData(dataId);
    }

    @Override
    public void init() {

    }
}
