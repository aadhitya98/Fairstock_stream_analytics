package com.fairstock.fairstock.model;

import java.io.Serializable;

public class ReservationEvent implements Serializable {

    public String eventId;
    public String eventType;
    public String productId;
    public String reservationId;
    public String customerId;

    public ReservationEvent() { }


    public ReservationEvent(
            String eventId,
            String eventType,
            String productId,
            String reservationId,
            String customerId) {

        this.eventId = eventId;
        this.eventType = eventType;
        this.productId = productId;
        this.reservationId = reservationId;
        this.customerId = customerId;
    }

}
