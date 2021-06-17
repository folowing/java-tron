package org.tron.core.db.api;

import java.util.Iterator;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.AbiCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.store.AbiStore;
import org.tron.core.store.ContractStore;

@Slf4j(topic = "DB")
public class MoveAbiHelper {

  private int count;

  private final ChainBaseManager chainBaseManager;

  public MoveAbiHelper(ChainBaseManager chainBaseManager) {
    this.chainBaseManager = chainBaseManager;
  }

  public void doWork() {
    long start = System.currentTimeMillis();
    logger.info("Start to move abi back");
    AbiStore abiStore = chainBaseManager.getAbiStore();
    ContractStore contractStore = chainBaseManager.getContractStore();
    abiStore.iterator().forEachRemaining(e -> {
      AbiCapsule abiCapsule = e.getValue();
      ContractCapsule contractCapsule = contractStore.get(e.getKey());
      contractStore.put(e.getKey(), new ContractCapsule(
          contractCapsule.getInstance().toBuilder().setAbi(abiCapsule.getInstance()).build()));
      count += 1;
      if (count % 100_000 == 0) {
        logger.info("Doing the abi move back, current count: {} {}", count,
            System.currentTimeMillis());
      }
    });
    chainBaseManager.getDynamicPropertiesStore().saveAbiMoveDone(0);
    logger.info(
        "Complete the abi move back, total time: {} milliseconds, total count: {}",
        System.currentTimeMillis() - start, count);
  }
}
