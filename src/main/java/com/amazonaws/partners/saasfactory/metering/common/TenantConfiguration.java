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

import org.slf4j.Logger;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import static com.amazonaws.partners.saasfactory.metering.common.Constants.CLOSING_INVOICE_TIME_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.CONFIG_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.CONFIG_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.CONFIG_SORT_KEY_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EXTERNAL_SUBSCRIPTION_IDENTIFIER_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.formatTenantEntry;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TenantConfiguration {

    private final String tenantID;
    private final String externalSubscriptionIdentifier;
    private final Instant invoiceClosingTime;

    private TenantConfiguration(String tenantID,
                                String externalSubscriptionIdentifier,
                                String invoiceClosingTime) {
       this.tenantID = tenantID;
       this.externalSubscriptionIdentifier = externalSubscriptionIdentifier;
       if (invoiceClosingTime == null) {
           this.invoiceClosingTime = null;
       } else {
           this.invoiceClosingTime = Instant.parse(invoiceClosingTime);
       }
    }

    private TenantConfiguration() {
        this.tenantID = "";
        this.externalSubscriptionIdentifier = "";
        this.invoiceClosingTime = null;
    }

    public String getTenantID() { return tenantID; }

    public String getExternalSubscriptionIdentifier() { return externalSubscriptionIdentifier; }

    public Instant getInvoiceClosingTime() { return this.invoiceClosingTime; }

    public boolean isEmpty() {
        return this.tenantID.isEmpty() &&
               this.externalSubscriptionIdentifier.isEmpty() &&
               this.invoiceClosingTime == null;
    }

    public boolean isInvoiceClosed() {
        // Is the invoice time older than the current time?
        return this.invoiceClosingTime.compareTo(Instant.now()) < 0;
    }

    public static List<TenantConfiguration> getTenantConfigurations(TableConfiguration tableConfig, DynamoDbClient ddb, Logger logger) {
        // Add support for the invoice end field here
        // https://stripe.com/docs/api/invoices/upcoming
        // When the application retrieves the tenant configuration, check for this attribute.
        // if it exists, move on. If it doesn't, retrieve it from Stripe, add it to the returned
        // tenant configuration and put it back into DDB. The retrieval part should be part of
        // the Lambda function that integrates Stripe
        List<TenantConfiguration> tenantIDs = new ArrayList<>();

        HashMap<String,String> expressionNames = new HashMap<>();
        expressionNames.put(CONFIG_EXPRESSION_NAME, SORT_KEY_NAME);

        HashMap<String,AttributeValue> expressionValues = new HashMap<>();
        AttributeValue sortKeyValue = AttributeValue.builder()
                .s(CONFIG_SORT_KEY_VALUE)
                .build();
        expressionValues.put(CONFIG_EXPRESSION_VALUE, sortKeyValue);
        QueryResponse result = null;
        do {
            QueryRequest request = QueryRequest.builder()
                    .tableName(tableConfig.getTableName())
                    .indexName(tableConfig.getIndexName())
                    .keyConditionExpression(String.format("%s = %s", CONFIG_EXPRESSION_NAME, CONFIG_EXPRESSION_VALUE))
                    .expressionAttributeNames(expressionNames)
                    .expressionAttributeValues(expressionValues)
                    .build();
            if (result != null && !result.lastEvaluatedKey().isEmpty()) {
                request = request.toBuilder()
                        .exclusiveStartKey(result.lastEvaluatedKey())
                        .build();
            }
            try {
                result = ddb.query(request);
            } catch (ResourceNotFoundException e) {
                logger.error("Table {} does not exist", tableConfig.getTableName());
                return new ArrayList<>();
            } catch (InternalServerErrorException e) {
                logger.error(e.getMessage());
                return new ArrayList<>();
            }
            for (Map<String, AttributeValue> item : result.items()) {
                String tenantID = item.get(PRIMARY_KEY_NAME).s();
                String externalProductCode = item.get(EXTERNAL_SUBSCRIPTION_IDENTIFIER_ATTRIBUTE_NAME).s();
                String invoiceClosingTime = item.getOrDefault(
                        CLOSING_INVOICE_TIME_ATTRIBUTE_NAME,
                        AttributeValue.builder()
                                .nul(true)
                                .build())
                        .s();
                TenantConfiguration tenant;
                try {
                    tenant = new TenantConfiguration(
                            tenantID,
                            externalProductCode,
                            invoiceClosingTime);
                } catch (DateTimeParseException e) {
                    logger.error("Could not parse the invoice closing date for tenant {}", tenantID);
                    continue;
                }
                logger.info("Found tenant ID {}", tenantID);
                tenantIDs.add(tenant);
            }
        } while (!result.lastEvaluatedKey().isEmpty());
        return tenantIDs;
    }

    public static TenantConfiguration getTenantConfiguration(String tenantID, TableConfiguration tableConfig, DynamoDbClient ddb, Logger logger) {

        Map<String, AttributeValue> compositeKey = new HashMap<>();
        AttributeValue primaryKeyValue = AttributeValue.builder()
                .s(formatTenantEntry(tenantID))
                .build();
        compositeKey.put(PRIMARY_KEY_NAME, primaryKeyValue);
        AttributeValue sortKeyValue = AttributeValue.builder()
                .s(CONFIG_SORT_KEY_VALUE)
                .build();
        compositeKey.put(SORT_KEY_NAME, sortKeyValue);

        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableConfig.getTableName())
                .key(compositeKey)
                .build();

        Map<String, AttributeValue> item;
        try {
            item = ddb.getItem(request).item();
        } catch (ResourceNotFoundException e) {
            logger.error("Table {} does not exist", tableConfig.getTableName());
            return new TenantConfiguration();
        } catch (InternalServerErrorException e) {
            logger.error(e.getMessage());
            return new TenantConfiguration();
        }

        TenantConfiguration tenant;
        if (!item.isEmpty()) {
            String externalSubscriptionIdentifier = item.get(EXTERNAL_SUBSCRIPTION_IDENTIFIER_ATTRIBUTE_NAME).s();
            String invoiceClosingTime = item.getOrDefault(
                    CLOSING_INVOICE_TIME_ATTRIBUTE_NAME,
                    AttributeValue.builder()
                            .nul(true)
                            .build())
                    .s();
            try {
                tenant = new TenantConfiguration(
                        tenantID,
                        externalSubscriptionIdentifier,
                        invoiceClosingTime);
            } catch (DateTimeParseException e) {
                logger.error("Could not parse the invoice closing date for tenant {}", tenantID);
                return new TenantConfiguration();
            }
        } else {
            return new TenantConfiguration();
        }

        return tenant;
    }
}
