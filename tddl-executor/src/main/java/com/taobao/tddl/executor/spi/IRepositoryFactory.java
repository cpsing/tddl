package com.taobao.tddl.executor.spi;

import com.taobao.tddl.executor.repo.RepositoryConfig;


/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:21:43
 * @since 5.1.0
 */
public interface IRepositoryFactory {

    IRepository buildReponsitory(RepositoryConfig conf);

}
