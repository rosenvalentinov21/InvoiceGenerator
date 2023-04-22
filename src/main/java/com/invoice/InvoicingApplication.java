package com.invoice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.invoice.common.Invoice;
import java.util.concurrent.ExecutionException;

public class InvoicingApplication {

    public static void main(String[] args)
            throws ExecutionException, InterruptedException, JsonProcessingException {
        // Here producers and consumers would be started on separated threads simultaneously to
        // replicate streaming experience

        var invoice = new Invoice(2, "customer-2", 20.0);
        var json = new ObjectMapper().writeValueAsString(invoice);
        System.out.println(json);
        var deserInvoice = new ObjectMapper().readValue(json, Invoice.class);
    }

}
