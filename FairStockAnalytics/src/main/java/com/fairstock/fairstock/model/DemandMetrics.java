package com.fairstock.fairstock.model;

import java.io.Serializable;

public class DemandMetrics implements Serializable {

    public String productId;

    public long requests;
    public long created;
    public long rejected;
    public long cancelled;
    public long expired;
    public long paymentStarted;
    public long paymentConfirmed;
    public long paymentFailed;
    public long otpFailed;

    public DemandMetrics() {
    }

    public DemandMetrics(String productId) {
        this.productId = productId;
    }

    public double rejectionRate() {
        if (requests == 0) {
            return 0.0;
        }

        return (double) rejected / requests;
    }
}
