package com.taobao.tddl.repo.mysql.spi;

import com.taobao.tddl.executor.spi.RepoConfig;
import com.taobao.tddl.executor.spi.Repository;

public class RepositoryFactoryMyImp implements IRepoFactory {

    @Override
    public Repository buildReponsitory(RepoConfig conf, AndorContext clientContext) {
        My_Reponsitory myRepo = new My_Reponsitory();
        myRepo.init(conf, clientContext);
        return myRepo;
    }

}
