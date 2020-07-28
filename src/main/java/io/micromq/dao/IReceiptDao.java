package io.micromq.dao;

import io.micromq.model.Receipt;

import java.util.List;

public interface IReceiptDao {
    /**
     * Get receipt by client and
     * @param client
     * @param queue
     * @return
     */
    Receipt getReceipt(String client, String queue);

    /**
     * Save single receipt
     * @param receipt
     * @return
     */
    Integer updateReceipt(Receipt receipt);

    /**
     * Update receipt list
     * @param receipts
     * @return
     */
    Integer updateReceiptList(List<Receipt> receipts);
}
