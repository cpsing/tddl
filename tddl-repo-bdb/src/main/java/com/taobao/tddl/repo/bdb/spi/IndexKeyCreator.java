package com.taobao.tddl.repo.bdb.spi;

import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.taobao.tddl.optimizer.config.table.IndexMeta;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class IndexKeyCreator implements SecondaryKeyCreator, SecondaryMultiKeyCreator {

    IndexMeta primaryMeta;
    IndexMeta secondaryMeta;

    // SecondaryKeyGen keygen;

    public IndexKeyCreator(IndexMeta primaryMeta, IndexMeta secondaryMeta){
        this.primaryMeta = primaryMeta;
        this.secondaryMeta = secondaryMeta;
        // this.keygen = new SecondaryKeyGen(primaryMeta,secondaryMeta);
    }

    @Override
    public boolean createSecondaryKey(SecondaryDatabase secondary, final DatabaseEntry key, final DatabaseEntry data,
                                      final DatabaseEntry result) {
        // KVPair kv = keygen.createSecondaryRecord(key.getData(),
        // data.getData());

        // result.setData(keygen.getSecondaryKeyCodec().encode(kv.getKey()));
        // return OperationStatus.SUCCESS == secondary.get(null, result, data,
        // LockMode.DEFAULT);
        return true;
    }

    @Override
    public void createSecondaryKeys(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data,
                                    Set<DatabaseEntry> results) {
        // List<KVPair> sKeys = keygen.createSecondaryRecords(key.getData(),
        // data.getData());
        // if (sKeys != null) {
        // for (KVPair kv : sKeys) {
        // results.add(new
        // DatabaseEntry(keygen.getSecondaryKeyCodec().encode(kv.getKey())));
        // }
        // }
    }
}
