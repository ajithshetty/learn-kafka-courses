package io.confluent.developer.bank;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.confluent.developer.avro.Bank;
import lombok.Builder;

import java.math.BigDecimal;
import java.util.Date;

public class BankTransaction {/*
    private Long id;
    private BigDecimal amount = BigDecimal.ZERO;
    @JsonFormat(shape = JsonFormat.Shape.STRING,
            pattern = "dd-MM-yyyy hh:mm:ss")
    private long lastUpdate;
    private Bank latestTransaction;

    public Bank process(Bank bankTransaction) {
        this.id = bankTransaction.getBalanceId();
        this.latestTransaction = bankTransaction;
        if(this.amount.add(bankTransaction.getAmount()).compareTo(BigDecimal.ZERO) >= 0) {
            this.latestTransaction.getBankTransactionState().(Bank.newBuilder().getBankTransactionState());
            this.amount = this.amount.add(bankTransaction.getAmount());
        } else {
            this.latestTransaction.setState(BankTransaction.BankTransactionState.REJECTED);
        }
        this.lastUpdate = bankTransaction.getTime();
        return this;
    }*/
}
