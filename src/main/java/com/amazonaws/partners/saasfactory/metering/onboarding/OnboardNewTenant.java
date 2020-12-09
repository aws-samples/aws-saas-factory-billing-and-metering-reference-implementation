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
package com.amazonaws.partners.saasfactory.metering.onboarding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.OnboardingEvent;
import com.amazonaws.partners.saasfactory.metering.common.EventBridgeOnboardTenantEvent;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.ATTRIBUTE_DELIMITER;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.CONFIG_SORT_KEY_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EXTERNAL_SUBSCRIPTION_IDENTIFIER_ATTRIBUTE_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_VALUE;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.PRIMARY_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.SORT_KEY_NAME;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.TENANT_PREFIX;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeDynamoDBClient;
import static com.amazonaws.partners.saasfactory.metering.common.Constants.initializeTableConfiguration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class OnboardNewTenant implements RequestStreamHandler {

    private final DynamoDbClient ddb;
    private final Gson gson;
    private final Logger logger;
    private final TableConfiguration tableConfig;

    public OnboardNewTenant() {
        this.ddb = initializeDynamoDBClient();
        this.logger = LoggerFactory.getLogger(OnboardNewTenant.class);
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.tableConfig = initializeTableConfiguration(this.logger);
    }

    // Used for testing, need to inject the mock DDB client
    public OnboardNewTenant(DynamoDbClient ddb, TableConfiguration tableConfig) {
        this.ddb = ddb;
        this.logger = LoggerFactory.getLogger(OnboardNewTenant.class);
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.tableConfig = tableConfig;
    }

    private UpdateItemRequest buildUpdateStatement(OnboardingEvent onboardingEvent) {
        HashMap<String, AttributeValue> compositeKey = new HashMap<>();

        AttributeValue primaryKeyValue = AttributeValue.builder()
                .s(String.format("%s%s%s",
                                    TENANT_PREFIX,
                                    ATTRIBUTE_DELIMITER,
                                    onboardingEvent.getTenantID()))
                .build();

        compositeKey.put(PRIMARY_KEY_NAME, primaryKeyValue);

        AttributeValue sortKeyValue = AttributeValue.builder()
                .s(CONFIG_SORT_KEY_VALUE)
                .build();
        compositeKey.put(SORT_KEY_NAME, sortKeyValue);

        HashMap<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_NAME, EXTERNAL_SUBSCRIPTION_IDENTIFIER_ATTRIBUTE_NAME);

        HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        AttributeValue externalProductCodeValue = AttributeValue.builder()
                .s(onboardingEvent.getExternalSubscriptionIdentifier())
                .build();

        expressionAttributeValues.put(EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_VALUE, externalProductCodeValue);

        String updateStatement = String.format("SET %s = %s",
                EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_NAME,
                EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_VALUE);

        return UpdateItemRequest.builder()
                .tableName(this.tableConfig.getTableName())
                .key(compositeKey)
                .updateExpression(updateStatement)
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

    }

    private void putTenant(OnboardingEvent onboardingEvent) {
        // This needs to be an update; there may already be an existing value in the subscription mapping attribute
        UpdateItemRequest subscriptionMappingUpdate = buildUpdateStatement(onboardingEvent);

        try {
            this.ddb.updateItem(subscriptionMappingUpdate);
        } catch (ResourceNotFoundException|InternalServerErrorException|TransactionCanceledException e) {
            this.logger.error("{}", e.toString());
            throw e;
        }
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        if (this.tableConfig.getTableName().isEmpty() || this.tableConfig.getIndexName().isEmpty()) {
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        EventBridgeOnboardTenantEvent event = gson.fromJson(reader, EventBridgeOnboardTenantEvent.class);

        // Create onboarding event
        OnboardingEvent onboardingEvent = OnboardingEvent.createOnboardingEvent(event);
        if (onboardingEvent == null) {
            if (event.getDetail().getTenantID() == null) {
                this.logger.error("Failed to create a new tenant because tenant ID unspecified");
            }
            if (event.getDetail().getExternalSubscriptionIdentifier() == null) {
                this.logger.error("Failed to create a new tenant because external subscription identifier unspecified");
            }
            return;
        }

        // Put the onboarding event into DynamoDB
        putTenant(onboardingEvent);
        this.logger.info("Created tenant with ID {}", onboardingEvent.getTenantID());
    }

}
