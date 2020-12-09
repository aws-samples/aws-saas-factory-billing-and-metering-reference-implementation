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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import com.amazonaws.partners.saasfactory.metering.aggregation.StripeBillingPublish;
import com.amazonaws.partners.saasfactory.metering.common.BillingProviderConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.UUID;

class StripeBillingPublishTest {

    private static DynamoDbClient client;
    private static final String tableName = "TestStripeBillingPublishTable";
    private static final String indexName = "TestStripeBillingPublishIndex";
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
    void shouldPublishAggregateEntries() {
        // Put a sample aggregation event
        long aggregationTimePeriod = 1577836800000L;
        HashMap<String, AttributeValue> item = new HashMap<>();
        item.put("data_type", AttributeValue.builder()
                .s(String.format("TENANT#Tenant%d", 0))
                .build());
        item.put("sub_type", AttributeValue.builder()
                .s(String.format("AGGREGATE#MINUTES#%d",
                        aggregationTimePeriod))
                .build());
        item.put("quantity", AttributeValue.builder()
                .n("10")
                .build());
        item.put("published_to_billing_provider", AttributeValue.builder()
                .bool(false)
                .build());
        item.put("idempotency_key", AttributeValue.builder()
                .s(UUID.randomUUID().toString().split("-")[4])
                .build());

        PutItemRequest aggregationRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        client.putItem(aggregationRequest);

        InputStream inputStream = new ByteArrayInputStream("".getBytes());
        OutputStream outputStream = new ByteArrayOutputStream();

        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        String testApiKey = "sk_test_123";
        String stripeOverrideUrl = "http://localhost:12111/";
        BillingProviderConfiguration billingConfig = new BillingProviderConfiguration(testApiKey);
        StripeBillingPublish stripeBillingPublish = new StripeBillingPublish(client,
                tableConfig,
                billingConfig,
                stripeOverrideUrl);
        Context context = null;
        stripeBillingPublish.handleRequest(inputStream, outputStream, context);

        // Not much more can be done beyond checking that the published_to_billing_provider
        // attribute is set to true because the stripe-mock doesn't retain state

        HashMap<String, AttributeValue> key = new HashMap<>();

        key.put("data_type", AttributeValue.builder()
                .s(String.format("TENANT#Tenant%d", 0))
                .build());
        key.put("sub_type", AttributeValue.builder()
                .s(String.format("AGGREGATE#MINUTES#%d",
                        aggregationTimePeriod))
                .build());

        GetItemRequest getItemRequest = GetItemRequest.builder()
                .key(key)
                .tableName(tableName)
                .build();

        GetItemResponse getItemResponse = client.getItem(getItemRequest);

        assertTrue(getItemResponse.item().get("published_to_billing_provider").bool());

    }

    @AfterAll
    public static void cleanUpDynamoDB() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();

        client.deleteTable(request);
    }
}
