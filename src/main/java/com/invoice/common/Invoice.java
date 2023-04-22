package com.invoice.common;

import java.security.SecureRandom;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Invoice {

    private final long customerId;
    private final String customerName;
    private final double amount;

    @Override
    public String toString() {
        return "Invoice{" +
                "customerId='" + customerId + '\'' +
                ", amount=" + amount + '\'' +
                ", customerName=" + customerName +
                '}';
    }

    public static Invoice generateRandomInvoice() {
        // simulate generating an invoice
        SecureRandom random = new SecureRandom();
        int id = random.nextInt(5);
        String customerName = "customer-" + id;
        double amount = Math.random() * 100;
        return new Invoice(id, customerName, amount);
    }

}
