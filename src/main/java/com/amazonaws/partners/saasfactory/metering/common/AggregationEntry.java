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

import static com.amazonaws.partners.saasfactory.metering.common.Constants.AGGREGATION_ENTRY_PREFIX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.ATTRIBUTE_DELIMITER;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.TRUNCATION_UNIT;

import java.time.Instant;

public class AggregationEntry {

    private final String tenantID;
    private final Instant periodStart;
    private final Long quantity;
    private final String idempotencyKey;

    public AggregationEntry(String tenantID, Instant periodStart, Long quantity, String idempotencyKey) {
       this.tenantID = tenantID;
       this.periodStart = periodStart;
       this.quantity = quantity;
       this.idempotencyKey = idempotencyKey;
    }

    public String getTenantID() { return tenantID; }

    public Instant getPeriodStart() { return periodStart; }

    public Long getQuantity() { return quantity; }

    public String getIdempotencyKey() { return idempotencyKey; }

    public String toString() {
        return String.format("%s%s%s%s%d",
                AGGREGATION_ENTRY_PREFIX,
                ATTRIBUTE_DELIMITER,
                TRUNCATION_UNIT.toString().toUpperCase(),
                ATTRIBUTE_DELIMITER,
                periodStart.toEpochMilli());
    }
}
