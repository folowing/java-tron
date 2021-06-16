package org.tron.core.db.api;

import java.util.Iterator;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.tron.core.ChainBaseManager;
import org.tron.core.store.AbiStore;
import org.tron.core.store.ContractStore;
import org.tron.protos.contract.SmartContractOuterClass.SmartContractNoABI;

@Slf4j(topic = "DB")
public class MoveAbiHelper {

  private int count;

  private final ChainBaseManager chainBaseManager;

  public MoveAbiHelper(ChainBaseManager chainBaseManager) {
    this.chainBaseManager = chainBaseManager;
  }

  public void doWork() {
    long start = System.currentTimeMillis();
    logger.info("Start to move abi");
    AbiStore abiStore = chainBaseManager.getAbiStore();
    ContractStore contractStore = chainBaseManager.getContractStore();
    Iterator<Map.Entry<byte[], byte[]>> it = contractStore.getIterator();
    it.forEachRemaining(e -> {
      try {
        SmartContractNoABI contract = SmartContractNoABI.parseFrom(e.getValue());
        byte[] abi = contract.getAbi().toByteArray();
        if (abi != null && abi.length != 0) {
          abiStore.put(e.getKey(), abi);
        }
      } catch (InvalidProtocolBufferException exception) {
        exception.printStackTrace();
      }
      count += 1;
      if (count % 100_000 == 0) {
        logger.info("Doing the abi move, current contracts: {} {}", count,
            System.currentTimeMillis());
      }
    });
    chainBaseManager.getDynamicPropertiesStore().saveAbiMoveDone(1);
    logger.info("Check store size: contract {} abi {}",
        contractStore.getTotalContracts(), abiStore.getTotalABIs());
    logger.info(
        "Complete the abi move, total time:{} milliseconds",
        System.currentTimeMillis() - start);
  }
}
