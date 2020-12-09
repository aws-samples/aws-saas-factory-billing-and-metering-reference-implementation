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
package aggregation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import com.amazonaws.partners.saasfactory.metering.aggregation.BillingEventAggregation;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class BillingEventAggregationTest {
    private static DynamoDbClient client;
    private static final String tableName = "TestBillingAggregationTable";
    private static final String indexName = "TestBillingAggregationIndex";
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
        PutItemRequest tenantRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        client.putItem(tenantRequest);

        logger = LoggerFactory.getLogger(BillingEventAggregationTest.class);

    }

    @Test
    void shouldAggregateBillingEvents() {
        // Why start with this time rather than the current time? There's logic in
        // the aggregation algorithm that ignores events that occur in the current minute.
        // If you used Instant.now, these events would be missed and the test would fail.
        long aggregationTime = 1577836800000L;
        // Create sample events
        for (int i = 0; i < 10; i++) {
            HashMap<String, AttributeValue> item = new HashMap<>();
            item.put("data_type", AttributeValue.builder()
                    .s(String.format("TENANT#Tenant%d", 0))
                    .build());
            item.put("sub_type", AttributeValue.builder()
                    .s(String.format("EVENT#%d#%s",
                            aggregationTime,
                            UUID.randomUUID().toString().split("-")[4]))
                    .build());
            item.put("quantity", AttributeValue.builder()
                    .n("1")
                    .build());

            PutItemRequest tenantRequest = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            client.putItem(tenantRequest);
            aggregationTime += 1;
        }

        InputStream inputStream = new ByteArrayInputStream("".getBytes());
        OutputStream outputStream = new ByteArrayOutputStream();

        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        BillingEventAggregation billingAggregator = new BillingEventAggregation(client, tableConfig);
        Context context = null;
        billingAggregator.handleRequest(inputStream, outputStream, context);

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
        String aggregatePrefixExpressionValue = ":aggregate";
        String aggregatePrefix = "AGGREGATE";
        expressionValues.put(aggregatePrefixExpressionValue,
                AttributeValue.builder()
                        .s(aggregatePrefix)
                        .build());

        QueryResponse response = null;
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        do {
            QueryRequest request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression(
                            String.format("%s = %s and begins_with(%s, %s)",
                                    dataTypeExpressionName,
                                    tenantIDExpressionValue,
                                    subTypeExpressionName,
                                    aggregatePrefixExpressionValue
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
            items.addAll(response.items());

        } while (!response.lastEvaluatedKey().isEmpty());

        assertFalse(items.isEmpty());

        // There could be more than one entry if the events were put in when a minute cut over; verify both
        int totalQuantity = 0;
        for (Map<String, AttributeValue> item : response.items()) {
            assertEquals(item.get("data_type").s(), tenantID);

            String expectedEventEntry = String.format("%s%s%s%s%s",
                    aggregatePrefix,
                    "#",
                    "MINUTES",
                    "#",
                    "[a-z0-9]{12}");
            Pattern expectedEventEntryPattern = Pattern.compile(expectedEventEntry);
            Matcher entryMatch = expectedEventEntryPattern.matcher(item.get("sub_type").s());
            assertTrue(entryMatch.find());

            totalQuantity += Integer.parseInt(item.get("quantity").n());
        }
        assertEquals(totalQuantity, 10);
    }


    @AfterAll
    public static void cleanUpDynamoDB() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();

        client.deleteTable(request);
    }
}
