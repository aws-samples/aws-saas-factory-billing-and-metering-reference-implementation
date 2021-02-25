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
package com.amazonaws.partners.saasfactory.metering.billing;

import com.amazonaws.partners.saasfactory.metering.common.TenantNotFoundException;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import com.amazonaws.partners.saasfactory.metering.common.BillingEvent;
import com.amazonaws.partners.saasfactory.metering.common.EventBridgeBillingEvent;
import com.amazonaws.partners.saasfactory.metering.common.ProcessBillingEventException;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.TenantConfiguration;

import static com.amazonaws.partners.saasfactory.metering.common.Constants.CONFIG_INDEX_NAME_ENV_VARIABLE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.QUANTITY_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.TABLE_ENV_VARIABLE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.formatEventEntry;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.formatTenantEntry;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeDynamoDBClient;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeTableConfiguration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ProcessBillingEvent implements RequestStreamHandler {

    private final DynamoDbClient ddb;
    private final Gson gson;
    private final Logger logger;
    private final TableConfiguration tableConfig;

    public ProcessBillingEvent() {
        this.ddb = initializeDynamoDBClient();
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.logger = LoggerFactory.getLogger(ProcessBillingEvent.class);
        this.tableConfig = initializeTableConfiguration(this.logger);
    }

    // This is for testing
    public ProcessBillingEvent(DynamoDbClient ddb, TableConfiguration tableConfig) {
        this.ddb = ddb;
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.logger = LoggerFactory.getLogger(ProcessBillingEvent.class);
        this.tableConfig = tableConfig;
    }

    private boolean putEvent(BillingEvent billingEvent) {
        HashMap<String,AttributeValue> item= new HashMap<>();

        AttributeValue primaryKeyValue = AttributeValue.builder()
                .s(formatTenantEntry(billingEvent.getTenantID()))
                .build();

        AttributeValue sortKeyValue = AttributeValue.builder()
                .s(formatEventEntry(billingEvent.getEventTime()))
                .build();

        AttributeValue quantityAttributeValue = AttributeValue.builder()
                .n(billingEvent.getQuantity().toString())
                .build();

        item.put(PRIMARY_KEY_NAME, primaryKeyValue);
        item.put(SORT_KEY_NAME, sortKeyValue);
        item.put(QUANTITY_ATTRIBUTE_NAME, quantityAttributeValue);

        PutItemRequest request = PutItemRequest.builder()
                .tableName(this.tableConfig.getTableName())
                .item(item)
                .build();

        try {
            this.ddb.putItem(request);
        } catch (ResourceNotFoundException e) {
            this.logger.error("Table {} does not exist", this.tableConfig.getTableName());
            return false;
        } catch (InternalServerErrorException e) {
            this.logger.error(e.getMessage());
            return false;
        }
        return true;
    }

    private boolean validateEventFields(EventBridgeBillingEvent event) {
        if (event.getDetail().getTenantID() == null) {
            return false;
        }
        if (event.getDetail().getQuantity() == null) {
            return false;
        }
        return true;
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        if (this.tableConfig.getTableName().isEmpty() || this.tableConfig.getIndexName().isEmpty()) {
            this.logger.error("{} or {} environment variable not set", TABLE_ENV_VARIABLE,CONFIG_INDEX_NAME_ENV_VARIABLE);
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        EventBridgeBillingEvent event = null;
        try {
            event = gson.fromJson(reader, EventBridgeBillingEvent.class);
        } catch (JsonSyntaxException e) {
            this.logger.error("Unable to parse JSON input");
            throw e;
        }

        if (!validateEventFields(event)) {
            this.logger.error("The fields associated with the billing event are not valid");
            throw new ProcessBillingEventException("TenantID or Quantity key not found in event");
        }

        // Verify the existence of the tenant ID
        TenantConfiguration tenant = TenantConfiguration.getTenantConfiguration(
                event.getDetail().getTenantID(),
                this.tableConfig,
                this.ddb,
                this.logger);

        if (tenant.isEmpty()) {
            throw new TenantNotFoundException(String.format(
                    "Tenant with ID %s not found",
                    event.getDetail().getTenantID()));
        }

        this.logger.info("Found tenant ID {}", event.getDetail().getTenantID());

        BillingEvent billingEvent = BillingEvent.createBillingEvent(event);
        if (billingEvent == null) {
            this.logger.error("Billing event not created because a component of the billing event was missing.");
            throw new ProcessBillingEventException("Billing event not created because a component of the billing event was missing.");
        }
        this.logger.debug("Billing event time is: {}", event.getTime());
        boolean result = putEvent(billingEvent);
        if (result) {
            this.logger.info("{} | {} | {}",
                    billingEvent.getTenantID(),
                    billingEvent.getEventTime(),
                    billingEvent.getQuantity());
        } else {
            this.logger.error("{} | {} | {}",
                    billingEvent.getTenantID(),
                    billingEvent.getEventTime(),
                    billingEvent.getQuantity());
            throw new ProcessBillingEventException("Failure to put item into DyanmoDB");
        }
    }
}