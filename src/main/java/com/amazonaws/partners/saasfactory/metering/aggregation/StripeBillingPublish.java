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
package com.amazonaws.partners.saasfactory.metering.aggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import com.amazonaws.partners.saasfactory.metering.common.AggregationEntry;
import com.amazonaws.partners.saasfactory.metering.common.BillingProviderConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.TenantConfiguration;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.AGGREGATION_ENTRY_PREFIX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.AGGREGATION_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.ATTRIBUTE_DELIMITER;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.IDEMPOTENTCY_KEY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.KEY_SUBMITTED_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PERIOD_START_ARRAY_LOCATION;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.QUANTITY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.STRIPE_IDEMPOTENCY_REPLAYED;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SUBMITTED_KEY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SUBMITTED_KEY_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.TENANT_ID_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.formatAggregationEntry;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeBillingProviderConfiguration;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeDynamoDBClient;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeTableConfiguration;

import com.stripe.Stripe;
import com.stripe.exception.StripeException;
import com.stripe.model.UsageRecord;
import com.stripe.net.RequestOptions;
import com.stripe.param.UsageRecordCreateOnSubscriptionItemParams;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StripeBillingPublish implements RequestStreamHandler {

    private final DynamoDbClient ddb;
    private final Logger logger;
    private final TableConfiguration tableConfig;
    private final BillingProviderConfiguration billingConfig;

    public StripeBillingPublish() {
        this.ddb = initializeDynamoDBClient();
        this.logger = LoggerFactory.getLogger(StripeBillingPublish.class);
        this.tableConfig = initializeTableConfiguration(this.logger);
        this.billingConfig = initializeBillingProviderConfiguration(this.logger);
        Stripe.apiKey = this.billingConfig.getApiKey();
    }

    public StripeBillingPublish(DynamoDbClient ddb,
                                TableConfiguration tableConfig,
                                BillingProviderConfiguration billingConfig,
                                String stripeOverrideUrl) {
        this.ddb = ddb;
        this.logger = LoggerFactory.getLogger(StripeBillingPublish.class);
        this.tableConfig = tableConfig;
        this.billingConfig = billingConfig;
        Stripe.apiKey = this.billingConfig.getApiKey();
        Stripe.overrideApiBase(stripeOverrideUrl);
    }

    private List<AggregationEntry> getAggregationEntries(String tenantID) {
        HashMap<String,String> expressionNames = new HashMap<>();
        expressionNames.put(PRIMARY_KEY_EXPRESSION_NAME, PRIMARY_KEY_NAME);
        expressionNames.put(SORT_KEY_EXPRESSION_NAME, SORT_KEY_NAME);
        expressionNames.put(SUBMITTED_KEY_EXPRESSION_NAME, SUBMITTED_KEY_ATTRIBUTE_NAME);

        HashMap<String, AttributeValue> expressionValues = new HashMap<>();
        AttributeValue tenantIDValue = AttributeValue.builder()
                .s(tenantID)
                .build();
        expressionValues.put(TENANT_ID_EXPRESSION_VALUE, tenantIDValue);

        AttributeValue aggregationEntryPrefixValue = AttributeValue.builder()
                .s(AGGREGATION_ENTRY_PREFIX)
                .build();
        expressionValues.put(AGGREGATION_EXPRESSION_VALUE, aggregationEntryPrefixValue);

        // Filter for those entries that have not yet been submitted to the billing provider
        AttributeValue keySubmittedValue = AttributeValue.builder()
                .bool(false)
                .build();
        expressionValues.put(KEY_SUBMITTED_EXPRESSION_VALUE, keySubmittedValue);

        QueryResponse result = null;
        List<AggregationEntry> aggregationEntries = new ArrayList<>();
        do {
            QueryRequest request = QueryRequest.builder()
                    .tableName(this.tableConfig.getTableName())
                    .keyConditionExpression(String.format("%s = %s and begins_with(%s, %s)",
                                                PRIMARY_KEY_EXPRESSION_NAME,
                                                TENANT_ID_EXPRESSION_VALUE,
                                                SORT_KEY_EXPRESSION_NAME,
                                                AGGREGATION_EXPRESSION_VALUE))
                    .filterExpression(String.format("%s = %s", SUBMITTED_KEY_EXPRESSION_NAME, KEY_SUBMITTED_EXPRESSION_VALUE))
                    .expressionAttributeNames(expressionNames)
                    .expressionAttributeValues(expressionValues)
                    .build();
            if (result != null && !result.lastEvaluatedKey().isEmpty()) {
                request = request.toBuilder()
                        .exclusiveStartKey(result.lastEvaluatedKey())
                        .build();
            }
            try {
                result = this.ddb.query(request);
            } catch (ResourceNotFoundException e) {
                this.logger.error("Table {} does not exist", this.tableConfig.getTableName());
                return new ArrayList<>();
            } catch (InternalServerErrorException e) {
                this.logger.error(e.getMessage());
                return new ArrayList<>();
            }
            for (Map<String, AttributeValue> item : result.items()) {
                String[] aggregationInformation = item.get(SORT_KEY_NAME).s().split(ATTRIBUTE_DELIMITER);
                Instant periodStart = Instant.ofEpochMilli(Long.parseLong(aggregationInformation[PERIOD_START_ARRAY_LOCATION]));
                Long quantity = Long.valueOf(item.get(QUANTITY_ATTRIBUTE_NAME).n());
                String idempotencyKey = item.get(IDEMPOTENTCY_KEY_ATTRIBUTE_NAME).s();
                AggregationEntry entry = new AggregationEntry(tenantID,
                        periodStart,
                        quantity,
                        idempotencyKey);
                aggregationEntries.add(entry);
            }
        } while (!result.lastEvaluatedKey().isEmpty());
        return aggregationEntries;
    }

    private void addUsageToSubscriptionItem(String subscriptionItemId, AggregationEntry aggregationEntry) {
        UsageRecord usageRecord = null;

        UsageRecordCreateOnSubscriptionItemParams params =
                UsageRecordCreateOnSubscriptionItemParams.builder()
                    .setQuantity(aggregationEntry.getQuantity())
                    .setTimestamp(aggregationEntry.getPeriodStart().truncatedTo(ChronoUnit.SECONDS).getEpochSecond())
                    .build();

        RequestOptions requestOptions = RequestOptions
                .builder()
                .setIdempotencyKey(aggregationEntry.getIdempotencyKey())
                .build();

        try {
            usageRecord = UsageRecord.createOnSubscriptionItem(subscriptionItemId, params, requestOptions);
        } catch(StripeException e) {
            this.logger.error("Stripe exception:\n{}", e.getMessage());
            this.logger.error("Timestamp: {}", aggregationEntry.getPeriodStart());
            return;
        }
        Map<String, List<String>> responseHeaders = usageRecord.getLastResponse().headers().map();
        // Check for idempotency key in use; if it is, then this is likely a situation where the
        // item was already submitted, but not marked as published
        if (responseHeaders.containsKey(STRIPE_IDEMPOTENCY_REPLAYED)) {
            String aggregationEntryString = formatAggregationEntry(aggregationEntry.getPeriodStart().toEpochMilli());
            this.logger.info("Aggregation entry {} for tenant {} already published; marking as published",
                            aggregationEntryString,
                            aggregationEntry.getTenantID());
        }
        markAggregationRecordAsSubmitted(aggregationEntry);
    }

    private void markAggregationRecordAsSubmitted(AggregationEntry aggregationEntry) {
        // Update the attribute that marks an item as submitted
        Map<String, AttributeValue> aggregationEntryKey = new HashMap<>();
        AttributeValue tenantIDValue = AttributeValue.builder()
                .s(aggregationEntry.getTenantID())
                .build();
        aggregationEntryKey.put(PRIMARY_KEY_NAME, tenantIDValue);

        AttributeValue aggregationStringValue = AttributeValue.builder()
                .s(formatAggregationEntry(aggregationEntry.getPeriodStart().toEpochMilli()))
                .build();
        aggregationEntryKey.put(SORT_KEY_NAME, aggregationStringValue);

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(SUBMITTED_KEY_EXPRESSION_NAME, SUBMITTED_KEY_ATTRIBUTE_NAME);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

        AttributeValue keySubmittedValue = AttributeValue.builder()
                .bool(true)
                .build();
        expressionAttributeValues.put(KEY_SUBMITTED_EXPRESSION_VALUE, keySubmittedValue);

        String updateExpression = String.format("SET %s = %s",
                                                SUBMITTED_KEY_EXPRESSION_NAME,
                                                KEY_SUBMITTED_EXPRESSION_VALUE);

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(this.tableConfig.getTableName())
                .key(aggregationEntryKey)
                .updateExpression(updateExpression)
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        try {
            ddb.updateItem(updateRequest);
        } catch (ResourceNotFoundException|InternalServerErrorException|TransactionCanceledException e) {
            this.logger.error(e.getMessage());
        }

        String aggregationEntryString = formatAggregationEntry(aggregationEntry.getPeriodStart().toEpochMilli());
        this.logger.info("Marked aggregation record {} for tenant {} as published",
                aggregationEntry.getTenantID(),
                aggregationEntryString
                );
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        if (this.tableConfig.getTableName().isEmpty() || this.tableConfig.getIndexName().isEmpty()) {
            return;
        }

        if (this.billingConfig.getApiKey().isEmpty()) {
            return;
        }
        this.logger.info("Fetching tenant IDs in table {}", this.tableConfig.getTableName());
        List<TenantConfiguration> tenantConfigurations = TenantConfiguration.getTenantConfigurations(this.tableConfig, ddb, this.logger);
        if (tenantConfigurations.isEmpty()) {
            this.logger.info("No tenant configurations found in table {}",
                                this.tableConfig.getTableName());
            return;
        }
        this.logger.info("Resolved tenant IDs in table {}", this.tableConfig.getTableName());
        for (TenantConfiguration tenant: tenantConfigurations) {
            List<AggregationEntry> aggregationEntries = getAggregationEntries(tenant.getTenantID());
            if (aggregationEntries.isEmpty()) {
                this.logger.info("No unpublished aggregation entries found for tenant {}",
                                tenant.getTenantID());
            } else {
                if (aggregationEntries.size() == 1) {
                    this.logger.info("Found {} an unpublished aggregation entry for tenant {}",
                            aggregationEntries.size(),
                            tenant.getTenantID());
                } else{
                    this.logger.info("Found {} unpublished aggregation entries for tenant {}",
                            aggregationEntries.size(),
                            tenant.getTenantID());
                }
                for (AggregationEntry entry : aggregationEntries) {
                    String subscriptionID = tenant.getExternalSubscriptionIdentifier();
                    if (subscriptionID == null) {
                        this.logger.error("No subscription ID found associated with tenant {}",
                                            tenant.getTenantID());
                        String aggregationEntryString = formatAggregationEntry(entry.getPeriodStart().toEpochMilli());
                        this.logger.error("Unable to publish aggregation entry {} associated with tenant {}",
                                            aggregationEntryString,
                                            tenant.getTenantID());
                        continue;
                    }
                    addUsageToSubscriptionItem(tenant.getExternalSubscriptionIdentifier(), entry);
                }
            }
        }
    }
}
