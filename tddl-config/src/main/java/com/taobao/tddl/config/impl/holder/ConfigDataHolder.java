package com.taobao.tddl.config.impl.holder;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.config.ConfigDataHandler;

/**
 * 抽取Holder实现，允许预先构建cache环境，加速{@linkplain ConfigDataHandler}.getData()方法的数据获取
 * 
 * @author jianghang 2013-10-28 下午5:45:07
 * @since 5.0.0
 */
public interface ConfigDataHolder extends Lifecycle {

    public String getData(String dataId);

    public Map<String, String> getData(List<String> dataIds);

}
