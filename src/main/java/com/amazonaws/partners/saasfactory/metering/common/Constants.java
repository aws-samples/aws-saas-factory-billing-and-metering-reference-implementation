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

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.InvalidParameterException;
import software.amazon.awssdk.services.secretsmanager.model.InvalidRequestException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public final class Constants {

    private Constants() {}

    public static final ChronoUnit TRUNCATION_UNIT = ChronoUnit.MINUTES;
    public static final Integer EVENT_TIME_ARRAY_INDEX = 1;
    public static final Integer MAXIMUM_BATCH_SIZE = 25;
    public static final Integer NONCE_ARRAY_INDEX = 2;
    public static final Integer PERIOD_START_ARRAY_LOCATION = 2;
    public static final Integer SELECTED_UUID_INDEX = 4;
    public static final String ADD_TO_AGGREGATION_EXPRESSION_VALUE = ":aggregationValue";
    public static final String AGGREGATION_ENTRY_PREFIX = "AGGREGATE";
    public static final String AGGREGATION_EXPRESSION_VALUE = ":aggregate";
    public static final String ATTRIBUTE_DELIMITER = "#";
    public static final String CONFIG_EXPRESSION_NAME = "#configurationAttributeName";
    public static final String CONFIG_EXPRESSION_VALUE = ":config";
    public static final String CONFIG_INDEX_NAME_ENV_VARIABLE = "DYNAMODB_CONFIG_INDEX_NAME";
    public static final String CONFIG_SORT_KEY_VALUE = "CONFIG";
    public static final String EVENT_PREFIX = "EVENT";
    public static final String EVENT_PREFIX_ATTRIBUTE_VALUE = ":event";
    public static final String IDEMPOTENTCY_KEY_ATTRIBUTE_NAME = "idempotency_key";
    public static final String KEY_SUBMITTED_EXPRESSION_VALUE = ":confirmPublished";
    public static final String PRIMARY_KEY_EXPRESSION_NAME = "#datatype";
    public static final String PRIMARY_KEY_NAME = "data_type";
    public static final String QUANTITY_ATTRIBUTE_NAME = "quantity";
    public static final String QUANTITY_EXPRESSION_NAME = "#quantityName";
    public static final String SORT_KEY_EXPRESSION_NAME = "#subtype";
    public static final String SORT_KEY_NAME = "sub_type";
    public static final String STRIPE_IDEMPOTENCY_REPLAYED = "idempotent-replayed";
    public static final String STRIPE_SECRET_ARN_ENV_VARIABLE = "STRIPE_SECRET_ARN";
    public static final String SUBMITTED_KEY_ATTRIBUTE_NAME = "published_to_billing_provider";
    public static final String SUBMITTED_KEY_EXPRESSION_NAME = "#publishName";
    public static final String EXTERNAL_SUBSCRIPTION_IDENTIFIER_ATTRIBUTE_NAME = "external_subscription_identifier";
    public static final String EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_NAME = "#external_subscription_identifier";
    public static final String EXTERNAL_SUBSCRIPTION_IDENTIFIER_EXPRESSION_VALUE = ":externalSubscriptionIdentifier";
    public static final String TABLE_ENV_VARIABLE = "DYNAMODB_TABLE_NAME";
    public static final String TENANT_ID_EXPRESSION_VALUE = ":tenantID";
    public static final String TENANT_PREFIX = "TENANT";
    public static final String UUID_DELIMITER = "-";

    public static String getEnvVariable(String envVariableName, Logger logger) {
        String envVariableValue = System.getenv(envVariableName);
        if (envVariableValue == null) {
            logger.error("Environment variable {} not present", envVariableName);
            return "";
        }
        logger.debug("Resolved {} to {}", envVariableName, envVariableValue);
        return envVariableValue;
    }

    public static String formatAggregationEntry(long aggregationTime) {
        return String.format("%s%s%s%s%d",
                AGGREGATION_ENTRY_PREFIX,
                ATTRIBUTE_DELIMITER,
                TRUNCATION_UNIT.toString().toUpperCase(),
                ATTRIBUTE_DELIMITER,
                aggregationTime);
    }

    public static String formatTenantEntry(String tenantID) {
        return String.format("%s%s%s",
                TENANT_PREFIX,
                ATTRIBUTE_DELIMITER,
                tenantID);
    }

    public static String formatEventEntry(Instant timeOfEvent) {
        return String.format("%s%s%d%s%s",
                EVENT_PREFIX,
                ATTRIBUTE_DELIMITER,
                timeOfEvent.toEpochMilli(),
                ATTRIBUTE_DELIMITER,
                UUID.randomUUID().toString().split("-")[SELECTED_UUID_INDEX]);
    }

    public static DynamoDbClient initializeDynamoDBClient() {
        return DynamoDbClient.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    public static TableConfiguration initializeTableConfiguration(Logger logger) {
        return new TableConfiguration(
                getEnvVariable(TABLE_ENV_VARIABLE, logger),
                getEnvVariable(CONFIG_INDEX_NAME_ENV_VARIABLE, logger)
        );
    }

    public static BillingProviderConfiguration initializeBillingProviderConfiguration(Logger logger) {
        SecretsManagerClient sm = SecretsManagerClient.builder().build();
        String secretArn = getEnvVariable(STRIPE_SECRET_ARN_ENV_VARIABLE, logger);

        GetSecretValueRequest request = GetSecretValueRequest.builder()
                .secretId(secretArn)
                .build();
        GetSecretValueResponse result = null;
        try {
            result = sm.getSecretValue(request);
        } catch (ResourceNotFoundException |InvalidRequestException|InvalidParameterException e) {
            logger.error(e.getMessage());
        }
        if (result == null) {
            return new BillingProviderConfiguration();
        }

        return new BillingProviderConfiguration(
                result.secretString()
        );
    }
}
