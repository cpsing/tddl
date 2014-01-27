package com.taobao.tddl.repo.oceanbase.spi;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.repo.mysql.spi.My_Transaction;

/**
 * @author dreamond 2014年1月9日 下午4:58:58
 * @since 5.0.0
 */
public class Ob_Transaction extends My_Transaction {

    public Ob_Transaction(boolean autoCommit){
        super(autoCommit);
        // TODO Auto-generated constructor stub
    }

    protected final static Logger logger = LoggerFactory.getLogger(Ob_Transaction.class);

}
