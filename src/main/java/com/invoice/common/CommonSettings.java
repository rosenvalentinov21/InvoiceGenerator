package com.invoice.common;

public interface CommonSettings {
    String EXIT_CMD = "exit";
    String MESSAGE_CMD = "msg:";
    int CUT_OFF_OFFSET = MESSAGE_CMD.length();

    // Invoice producers vars
    String TOPIC_NAME = "invoices";
    String BOOTSTRAP_SERVERS = "localhost:29092";

}