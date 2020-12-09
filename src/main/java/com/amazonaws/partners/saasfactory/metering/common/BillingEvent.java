/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package com.amazonaws.partners.saasfactory.metering.common;

import java.time.Instant;

public class BillingEvent implements Comparable<BillingEvent> {

    private final Instant eventTime;
    private final String tenantID;
    private final Long quantity;
    private final String nonce;

    private BillingEvent(String tenantID,
                         Instant eventTime,
                         Long quantity,
                         String nonce) {
        this.eventTime = eventTime;
        this.tenantID = tenantID;
        this.quantity = quantity;
        this.nonce = nonce;
    }

    public static BillingEvent createBillingEvent(String tenantID, Instant eventTime, Long quantity) {
        return new BillingEvent(tenantID,
                eventTime,
                quantity,
                "");
    }

    public static BillingEvent createBillingEvent(String tenantID, Instant eventTime, Long quantity, String nonce) {
        return new BillingEvent(tenantID,
                eventTime,
                quantity,
                nonce);
    }

    public static BillingEvent createBillingEvent(EventBridgeBillingEvent event) {
        String tenantID = event.getDetail().getTenantID();
        if (tenantID.isEmpty()) {
            return null;
        }
        Long quantity = event.getDetail().getQuantity();
        if (quantity == null || quantity <= 0) {
            return null;
        }
        Instant timestamp = Instant.now();

        return new BillingEvent(tenantID,
                timestamp,
                quantity,
                "");
    }

    public Instant getEventTime() { return this.eventTime; }

    public String getTenantID() { return this.tenantID; }

    public Long getQuantity() { return this.quantity; }

    public String getNonce() { return this.nonce; }

    @Override
    public int compareTo(BillingEvent event) {
        return this.eventTime.compareTo(event.eventTime);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }

        if (o.getClass() != this.getClass()) { return false; }

        if (o == this) { return true; }

        BillingEvent event = (BillingEvent) o;

        return this.eventTime.equals(event.eventTime) &&
                this.tenantID.equals(event.tenantID) &&
                this.quantity.equals(event.quantity) &&
                this.nonce.equals(event.nonce);
    }

    @Override
    public int hashCode() {
        return this.eventTime.hashCode() +
                this.tenantID.hashCode() +
                this.quantity.hashCode() +
                this.nonce.hashCode();
    }

}
