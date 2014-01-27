package com.taobao.tddl.executor.spi;

import java.util.Map;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:21:43
 * @since 5.0.0
 */
public interface IRepositoryFactory {

    IRepository buildRepository(Map<String, String> properties);

}
