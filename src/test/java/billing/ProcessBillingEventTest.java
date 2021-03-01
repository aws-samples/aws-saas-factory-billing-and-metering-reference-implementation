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
package billing;

import com.amazonaws.partners.saasfactory.metering.common.ProcessBillingEventException;
import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import com.amazonaws.partners.saasfactory.metering.billing.ProcessBillingEvent;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ProcessBillingEventTest {

    private static DynamoDbClient client;
    private static final String tableName = "TestTenantConfigurationTable";
    private static final String indexName = "TestTenantConfigurationIndex";
    private static final String external_subscription_identifier = "si_000000000000";
    // This variable is required to run the tests even if it isn't used within the test
    private static Logger logger;

    @BeforeAll
    public static void initDynamoDBLocal() {
        client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .region(Region.US_WEST_2)
                .build();

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName("data_type")
                                .keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement.builder()
                                .attributeName("sub_type")
                                .keyType(KeyType.RANGE)
                                .build())
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName("data_type")
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("sub_type")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                                .indexName(indexName)
                                .keySchema(
                                        KeySchemaElement.builder()
                                                .attributeName("sub_type")
                                                .keyType(KeyType.HASH)
                                                .build(),
                                        KeySchemaElement.builder()
                                                .attributeName("data_type")
                                                .keyType(KeyType.RANGE)
                                                .build())
                                .projection(
                                        Projection.builder()
                                                .projectionType(ProjectionType.ALL)
                                                .build())
                                .provisionedThroughput(
                                        ProvisionedThroughput.builder()
                                                .readCapacityUnits((long) 0)
                                                .writeCapacityUnits((long) 0)
                                                .build())
                                .build())
                .build();

        CreateTableResponse response = client.createTable(request);

        // Create a sample tenant
        HashMap<String, AttributeValue> item = new HashMap<>();
        item.put("data_type", AttributeValue.builder()
                .s(String.format("TENANT#Tenant%d", 0))
                .build());
        item.put("sub_type", AttributeValue.builder()
                .s("CONFIG")
                .build());
        item.put("external_subscription_identifier", AttributeValue.builder()
                .s(external_subscription_identifier)
                .build());
        item.put("closing_invoice_time", AttributeValue.builder()
                .s(Instant.now().toString())
                .build());
        PutItemRequest tenantRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        client.putItem(tenantRequest);

        logger = LoggerFactory.getLogger(ProcessBillingEventTest.class);
    }

    @Test
    public void shouldProcessBillingEvent() {
        String onboardingJSON = "{ \"detail\": { \"TenantID\": \"Tenant0\", \"Quantity\": 5 }}";
        InputStream inputStream = new ByteArrayInputStream(onboardingJSON.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();
        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        ProcessBillingEvent processBillingEvent = new ProcessBillingEvent(client, tableConfig);
        Context context = null;
        processBillingEvent.handleRequest(inputStream, outputStream, context);

        HashMap<String, String> expressionNames = new HashMap<>();
        String dataTypeExpressionName = "#dataTypeColumn";
        expressionNames.put(dataTypeExpressionName, "data_type");
        String subTypeExpressionName = "#subTypeColumn";
        expressionNames.put(subTypeExpressionName, "sub_type");

        HashMap<String, AttributeValue> expressionValues = new HashMap<>();
        String tenantIDExpressionValue = ":tenantID";
        String tenantID = "TENANT#Tenant0";
        expressionValues.put(tenantIDExpressionValue,
                AttributeValue.builder()
                        .s(tenantID)
                        .build());
        String eventPrefixExpressionValue = ":event";
        String eventPrefix = "EVENT";
        expressionValues.put(eventPrefixExpressionValue,
                AttributeValue.builder()
                        .s(eventPrefix)
                        .build());

        QueryResponse response = null;
        do {
            QueryRequest request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression(
                            String.format("%s = %s and begins_with(%s, %s)",
                                    dataTypeExpressionName,
                                    tenantIDExpressionValue,
                                    subTypeExpressionName,
                                    eventPrefixExpressionValue
                            ))
                    .expressionAttributeNames(expressionNames)
                    .expressionAttributeValues(expressionValues)
                    .build();

            if (response != null && !response.lastEvaluatedKey().isEmpty()) {
                request = request.toBuilder()
                        .exclusiveStartKey(response.lastEvaluatedKey())
                        .build();
            }

            response = client.query(request);

        } while (!response.lastEvaluatedKey().isEmpty());

        // There should only be one result here.
        assertEquals(1, response.items().size());
        assertEquals(response.items().get(0).get("data_type").s(), tenantID);

        String expectedEventEntry = String.format("%s%s%s%s%s",
                "EVENT",
                "#",
                "[0-9]+",
                "#",
                "[a-z0-9]{12}");
        Pattern expectedEventEntryPattern = Pattern.compile(expectedEventEntry);
        Matcher entryMatch = expectedEventEntryPattern.matcher(response.items().get(0).get("sub_type").s());
        assertEquals(entryMatch.find(), true);

        assertEquals(response.items().get(0).get("quantity").n(), "5");
    }

    @Test
    void shouldThrowProcessBillingEventExceptionOnBadQuantityKey() {
        String onboardingJSON = "{ \"detail\": { \"TenantID\": \"Tenant0\", \"invalid_key\": 5 }}";
        InputStream inputStream = new ByteArrayInputStream(onboardingJSON.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();
        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        ProcessBillingEvent processBillingEvent = new ProcessBillingEvent(client, tableConfig);
        Context context = null;
        assertThrows(ProcessBillingEventException.class, () -> {
            processBillingEvent.handleRequest(inputStream, outputStream, context);
        });
    }

    @Test
    void shouldThrowProcessBillingEventExceptionOnBadTenantIDKey() {
        String onboardingJSON = "{ \"detail\": { \"invalid_key\": \"Tenant0\", \"Quantity\": 5 }}";
        InputStream inputStream = new ByteArrayInputStream(onboardingJSON.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();
        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        ProcessBillingEvent processBillingEvent = new ProcessBillingEvent(client, tableConfig);
        Context context = null;
        assertThrows(ProcessBillingEventException.class, () -> {
            processBillingEvent.handleRequest(inputStream, outputStream, context);
        });
    }

    @Test
    void shouldThrowJsonSyntaxExceptionExceptionOnBadQuantityValue() {
        String onboardingJSON = "{ \"detail\": { \"TenantID\": \"Tenant0\", \"Quantity\": \"Five\" }}";
        InputStream inputStream = new ByteArrayInputStream(onboardingJSON.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();
        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        ProcessBillingEvent processBillingEvent = new ProcessBillingEvent(client, tableConfig);
        Context context = null;
        assertThrows(JsonSyntaxException.class, () -> {
            processBillingEvent.handleRequest(inputStream, outputStream, context);
        });
    }

    @Test
    void shouldThrowJsonSyntaxExceptionOnInvalidJson() {
        String onboardingJSON = "invalid_json";
        InputStream inputStream = new ByteArrayInputStream(onboardingJSON.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();
        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        ProcessBillingEvent processBillingEvent = new ProcessBillingEvent(client, tableConfig);
        Context context = null;
        assertThrows(JsonSyntaxException.class, () -> {
            processBillingEvent.handleRequest(inputStream, outputStream, context);
        });
    }

    @AfterAll
    public static void cleanUpDynamoDB() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();

        client.deleteTable(request);
    }
}
