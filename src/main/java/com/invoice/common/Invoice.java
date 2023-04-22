package com.invoice.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.SecureRandom;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Invoice {

    private long customerId;
    private String customerName;
    private double amount;

    @JsonIgnore
    ObjectMapper mapper = new ObjectMapper();

    public Invoice(int id, String customerName, double amount) {
        this.customerId = id;
        this.customerName = customerName;
        this.amount = amount;
    }


    @SneakyThrows
    @Override
    public String toString() {
        return mapper.writeValueAsString(this);
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
