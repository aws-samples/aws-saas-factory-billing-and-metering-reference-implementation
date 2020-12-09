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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

import com.amazonaws.partners.saasfactory.metering.common.BillingEvent;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.TenantConfiguration;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.ADD_TO_AGGREGATION_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.ATTRIBUTE_DELIMITER;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EVENT_PREFIX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EVENT_PREFIX_ATTRIBUTE_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EVENT_TIME_ARRAY_INDEX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.IDEMPOTENTCY_KEY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.MAXIMUM_BATCH_SIZE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.NONCE_ARRAY_INDEX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.QUANTITY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.QUANTITY_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SELECTED_UUID_INDEX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SUBMITTED_KEY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.TENANT_ID_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.TRUNCATION_UNIT;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.UUID_DELIMITER;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.formatAggregationEntry;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeDynamoDBClient;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeTableConfiguration;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class BillingEventAggregation implements RequestStreamHandler {

    private final DynamoDbClient ddb;
    private final Logger logger;
    private final TableConfiguration tableConfig;

    public BillingEventAggregation() {
        this.ddb = initializeDynamoDBClient();
        this.logger = LoggerFactory.getLogger(BillingEventAggregation.class);
        this.tableConfig = initializeTableConfiguration(this.logger);
    }

    // This is for testing
    public BillingEventAggregation(DynamoDbClient ddb, TableConfiguration tableConfig) {
        this.ddb = ddb;
        this.logger = LoggerFactory.getLogger(BillingEventAggregation.class);
        this.tableConfig = tableConfig;
    }

    private List<BillingEvent> getBillingEventsForTenant(String tenantID) {
        HashMap<String,String> expressionNames = new HashMap<>();
        expressionNames.put(PRIMARY_KEY_EXPRESSION_NAME, PRIMARY_KEY_NAME);
        expressionNames.put(SORT_KEY_EXPRESSION_NAME, SORT_KEY_NAME);

        HashMap<String,AttributeValue> expressionValues = new HashMap<>();

        AttributeValue tenantIDValue = AttributeValue.builder()
                .s(tenantID)
                .build();

        AttributeValue eventPrefixValue = AttributeValue.builder()
                .s(EVENT_PREFIX)
                .build();

        expressionValues.put(TENANT_ID_EXPRESSION_VALUE, tenantIDValue);
        expressionValues.put(EVENT_PREFIX_ATTRIBUTE_VALUE, eventPrefixValue);

        QueryResponse result = null;
        List<BillingEvent> billingEvents = new ArrayList<>();
        do {
            QueryRequest request = QueryRequest.builder()
                    .tableName(this.tableConfig.getTableName())
                    .keyConditionExpression(String.format("%s = %s and begins_with(%s, %s)",
                                                PRIMARY_KEY_EXPRESSION_NAME,
                                                TENANT_ID_EXPRESSION_VALUE,
                                                SORT_KEY_EXPRESSION_NAME,
                                                EVENT_PREFIX_ATTRIBUTE_VALUE))
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
            } catch (InternalServerErrorException e) {
                this.logger.error(e.getMessage());
                // if there's a failure, return an empty array list rather than a partial array list
                return new ArrayList<>();
            }

            if (result == null) {
                return new ArrayList<>();
            }

            for (Map<String, AttributeValue> item : result.items()) {
                String eventEntry = item.get(SORT_KEY_NAME).s();
                long eventTimeInMilliseconds = Long.parseLong(eventEntry.split(ATTRIBUTE_DELIMITER)[EVENT_TIME_ARRAY_INDEX]);
                String nonce = eventEntry.split(ATTRIBUTE_DELIMITER)[NONCE_ARRAY_INDEX];
                Instant eventTime = Instant.ofEpochMilli(eventTimeInMilliseconds);
                Long quantity = Long.valueOf(item.get(QUANTITY_ATTRIBUTE_NAME).n());
                BillingEvent billingEvent = BillingEvent.createBillingEvent(tenantID, eventTime, quantity, nonce);
                billingEvents.add(billingEvent);
            }
        } while (!result.lastEvaluatedKey().isEmpty());
        return billingEvents;
    }

    private Map<ZonedDateTime, List<BillingEvent>> categorizeEvents(TenantConfiguration tenant, List<BillingEvent> billingEvents) {
        if (billingEvents.isEmpty()) {
            return new HashMap<>();
        }
        // Figure out the lowest and highest date (the range)
        Instant earliestBillingEvent = Collections.min(billingEvents).getEventTime();
        this.logger.info("Earliest event for tenant {} at {}",
                                    tenant.getTenantID(),
                                    earliestBillingEvent);
        Instant latestBillingEvent = Collections.max(billingEvents).getEventTime();
        this.logger.info("Latest event for tenant {} at {}",
                                    tenant.getTenantID(),
                                    latestBillingEvent);
        // Create a map with each element as a key based on the frequency (e.g. a day for a key with frequency for a day)
        Map<ZonedDateTime, List<BillingEvent>> eventCounts = new HashMap<>();
        for (BillingEvent event : billingEvents) {
            ZonedDateTime eventTime = event.getEventTime().atZone(ZoneId.of("UTC"));
            ZonedDateTime startOfEventTimePeriod = eventTime.truncatedTo(TRUNCATION_UNIT);
            ZonedDateTime startOfCurrentTimePeriod = Instant.now().atZone(ZoneId.of("UTC")).truncatedTo(TRUNCATION_UNIT);
            // Skip over this time period and future time period events because there may eventually be more events
            if (!(startOfCurrentTimePeriod.compareTo(startOfEventTimePeriod) <= 0)) {
                List<BillingEvent> eventList = eventCounts.getOrDefault(startOfEventTimePeriod, new ArrayList<>());
                eventList.add(event);
                eventCounts.put(startOfEventTimePeriod, eventList);
            }
        }
        return eventCounts;
    }

    private void initializeItem(Map<String, AttributeValue> compositeKey, ZonedDateTime time) {
        // Format the statements
        AttributeValue idempotencyKeyValue = AttributeValue.builder()
                .s(UUID.randomUUID().toString().split(UUID_DELIMITER)[SELECTED_UUID_INDEX])
                .build();
        compositeKey.put(IDEMPOTENTCY_KEY_ATTRIBUTE_NAME, idempotencyKeyValue);

        AttributeValue submittedValue = AttributeValue.builder()
                .bool(false)
                .build();
        compositeKey.put(SUBMITTED_KEY_ATTRIBUTE_NAME, submittedValue);

        String conditionalStatement = String.format("attribute_not_exists(%s)", QUANTITY_ATTRIBUTE_NAME);

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(this.tableConfig.getTableName())
                .item(compositeKey)
                .conditionExpression(conditionalStatement)
                .build();

        try {
            ddb.putItem(putItemRequest);
        } catch (ResourceNotFoundException|InternalServerErrorException e) {
            this.logger.error("{}", e.toString());
        } catch (ConditionalCheckFailedException e) {
            // Repeat the transaction and see if it works
            this.logger.error("Entry at {} already exists",
                    time.toInstant());
        }
    }

    private void putRequestsAsTransaction(Update updateRequest, List<Delete> deleteRequests) {
        List<TransactWriteItem> transaction = new ArrayList<>();
        TransactWriteItem updateTransactionItem = TransactWriteItem.builder()
                .update(updateRequest)
                .build();
        transaction.add(updateTransactionItem);
        List<TransactWriteItem> deleteRequestItems = deleteRequests.stream().map(deleteRequest -> TransactWriteItem.builder()
            .delete(deleteRequest)
            .build()).collect(Collectors.toList());
        transaction.addAll(deleteRequestItems);
        this.logger.info("Transaction contains {} actions", transaction.size());

        TransactWriteItemsRequest transactWriteItemsRequest = TransactWriteItemsRequest.builder()
                .transactItems(transaction)
                .build();

        try {
            ddb.transactWriteItems(transactWriteItemsRequest);
        } catch (ResourceNotFoundException|InternalServerErrorException|TransactionCanceledException e) {
            this.logger.error("{}", e.toString());
        }
    }

    private Long countEvents(List<BillingEvent> billingEvents) {
        this.logger.info("Counting events");
        Long eventCount = 0L;
        for (BillingEvent event : billingEvents) {
            eventCount += event.getQuantity();
        }
        return eventCount;
    }

    private Update buildUpdate(Long eventCount, Map<String, AttributeValue> compositeKey) {
        List<String> updateStatements = new ArrayList<>();
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(QUANTITY_EXPRESSION_NAME, QUANTITY_ATTRIBUTE_NAME);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        AttributeValue countByProductionCodeValue = AttributeValue.builder()
                .n(eventCount.toString())
                .build();
        expressionAttributeValues.put(ADD_TO_AGGREGATION_EXPRESSION_VALUE, countByProductionCodeValue);

        this.logger.info("Count is {}", eventCount);
        // Appended to the ADD_TO_AGGREGATION_ATTRIBUTE_VALUE for identification in the expression
        // attribute names/values. There could be more than one product code to aggregate
        String updateStatement = String.format("ADD %s %s",
                QUANTITY_EXPRESSION_NAME,
                ADD_TO_AGGREGATION_EXPRESSION_VALUE);
        updateStatements.add(updateStatement);

        return Update.builder()
                .tableName(this.tableConfig.getTableName())
                .key(compositeKey)
                .updateExpression(String.join(",", updateStatements))
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

    }

    private List<Delete> buildDeletes(List<BillingEvent> billingEvents, TenantConfiguration tenant) {
        List<Delete> deleteRequests = new ArrayList<>();
        for (BillingEvent event : billingEvents) {
            Map<String, AttributeValue> keyToDelete = new HashMap<>();
            Long eventTime = event.getEventTime().toEpochMilli();
            String nonce = event.getNonce();
            AttributeValue tenantIDValue = AttributeValue.builder()
                    .s(tenant.getTenantID())
                    .build();
            keyToDelete.put(PRIMARY_KEY_NAME, tenantIDValue);

            AttributeValue eventValue = AttributeValue.builder()
                    .s(String.format("%s%s%d%s%s",
                        EVENT_PREFIX,
                        ATTRIBUTE_DELIMITER,
                        eventTime,
                        ATTRIBUTE_DELIMITER,
                        nonce))
                    .build();

            keyToDelete.put(SORT_KEY_NAME, eventValue);

            Delete delete = Delete.builder()
                    .tableName(this.tableConfig.getTableName())
                    .key(keyToDelete)
                    .build();

            deleteRequests.add(delete);
        }
        return deleteRequests;
    }

    private void performTransaction(List<BillingEvent> billingEvents, Map<String, AttributeValue> compositeKey, ZonedDateTime time, TenantConfiguration tenant) {
        Update updateRequest = null;
        List<Delete> deleteRequests = null;

        Long eventCount = countEvents(billingEvents);
        // Initialize the item for this time slot if necessary
        this.logger.debug("Initializing item for tenant {} at time {}",
                tenant.getTenantID(),
                time.toInstant());
        // This doesn't have to be done each time; keep track of what is already initialized
        // Pass in a copy of compositeKey because initializeItem makes modifications to it
        initializeItem(new HashMap<>(compositeKey), time);
        this.logger.debug("Batched {} events, performing transaction", billingEvents.size());
        updateRequest = buildUpdate(eventCount, compositeKey);
        deleteRequests = buildDeletes(billingEvents, tenant);
        putRequestsAsTransaction(updateRequest, deleteRequests);
    }

    private void aggregateEntries(Map<ZonedDateTime, List<BillingEvent>> eventsByDate, TenantConfiguration tenant) {
        Map<String, AttributeValue> compositeKey = new HashMap<>();
        List<BillingEvent> eventsToCount = new ArrayList<>();

        for (Entry<ZonedDateTime, List<BillingEvent>> entry : eventsByDate.entrySet()) {
            ZonedDateTime time = entry.getKey();
            AttributeValue tenantIDValue = AttributeValue.builder()
                    .s(tenant.getTenantID())
                    .build();
            compositeKey.put(PRIMARY_KEY_NAME, tenantIDValue);

            AttributeValue aggregationEntryValue = AttributeValue.builder()
                    .s(formatAggregationEntry(time.toInstant().toEpochMilli()))
                    .build();
            compositeKey.put(SORT_KEY_NAME, aggregationEntryValue);

            List<BillingEvent> billingEvents = eventsByDate.get(time);

            for (BillingEvent event : billingEvents) {
                eventsToCount.add(event);
                // 24 purges, 1 update
                // Minus one because I need to leave room for the update statement
                if (eventsToCount.size() == MAXIMUM_BATCH_SIZE - 1) {
                    performTransaction(eventsToCount, compositeKey, time, tenant);
                    eventsToCount.clear();
                }
            }
            // Submit the last batch of items
            // If the number of requests lands on an increment of 25, need to make sure that no attempt is made to put
            // an empty list, which is why I'm confirming that the list isn't empty. If it isn't empty, put the remaining
            // events in the database
            if (!eventsToCount.isEmpty()) {
                performTransaction(eventsToCount, compositeKey, time, tenant);
                eventsToCount.clear();
            }
        }
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        if (this.tableConfig.getTableName().isEmpty() || this.tableConfig.getIndexName().isEmpty()) {
            return;
        }

        this.logger.info("Resolving tenant IDs in table {}", this.tableConfig.getTableName());
        List<TenantConfiguration> tenants = TenantConfiguration.getTenantConfigurations(this.tableConfig, this.ddb, this.logger);
        this.logger.info("Resolved tenant IDs in table {}", this.tableConfig.getTableName());
        if (tenants.isEmpty()) {
            this.logger.info("No tenants found");
            return;
        }
        for (TenantConfiguration tenant : tenants) {
            List<BillingEvent> billingEvents = getBillingEventsForTenant(tenant.getTenantID());
            if (billingEvents.isEmpty()) {
                this.logger.info("No events for {}", tenant.getTenantID());
                continue;
            }
            // Count the number of events - this step is necessary to make the transactions work; they need
            // to be grouped together
            Map<ZonedDateTime, List<BillingEvent>> categorizedEvents = categorizeEvents(tenant, billingEvents);
            if (categorizedEvents.isEmpty()) {
                this.logger.info("No aggregation entries for {}", tenant.getTenantID());
            } else {
                // Put those results back into the table
                aggregateEntries(categorizedEvents, tenant);
            }
        }
    }
}