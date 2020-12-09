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
package common;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import com.amazonaws.partners.saasfactory.metering.common.TenantConfiguration;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;

import java.net.URI;
import java.util.List;
import java.util.HashMap;

class TenantConfigurationTest {

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

        logger = LoggerFactory.getLogger(TenantConfigurationTest.class);
    }

    public void loadRandomTenants(int numberOfTenants) {
        for (int i = 0; i < numberOfTenants; i++) {
            HashMap<String, AttributeValue> item = new HashMap<>();
            item.put("data_type", AttributeValue.builder()
                    .s(String.format("TENANT#Tenant%d", i))
                    .build());
            item.put("sub_type", AttributeValue.builder()
                    .s("CONFIG")
                    .build());
            item.put("external_subscription_identifier", AttributeValue.builder()
                    .s(external_subscription_identifier)
                    .build());
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            PutItemResponse response = client.putItem(request);
        }
    }

    public void deleteTenants(int numberOfTenants) {
        for (int i = 0; i < numberOfTenants; i++) {
            HashMap<String, AttributeValue> key = new HashMap<>();
            key.put("data_type", AttributeValue.builder()
                    .s(String.format("TENANT#Tenant%d", i))
                    .build());
            key.put("sub_type", AttributeValue.builder()
                    .s("CONFIG")
                    .build());
            DeleteItemRequest request = DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build();
            DeleteItemResponse response = client.deleteItem(request);
        }

    }

    @Test
    void shouldReturnTenantConfigurationsLessThan25() {
        int numberOfTenants = 25;
        loadRandomTenants(numberOfTenants);
        List<TenantConfiguration> tenants = TenantConfiguration.getTenantConfigurations(
                new TableConfiguration(tableName, indexName),
                client,
                logger);
        assertEquals(tenants.size(), numberOfTenants);
    }

    @Test
    void shouldReturnTenantConfigurationsMoreThan25() {
        int numberOfTenants = 100;
        loadRandomTenants(numberOfTenants);
        List<TenantConfiguration> tenants = TenantConfiguration.getTenantConfigurations(
                new TableConfiguration(tableName, indexName),
                client,
                logger);
        assertEquals(tenants.size(), numberOfTenants);
    }

    @Test
    void shouldReturnTenantConfiguration() {
        int numberOfTenants = 1;
        String tenantIdentifier = "Tenant0";
        loadRandomTenants(numberOfTenants);
        TenantConfiguration tenant = TenantConfiguration.getTenantConfiguration(
                tenantIdentifier,
                new TableConfiguration(tableName, indexName),
                client,
                logger);
        assertEquals(external_subscription_identifier, tenant.getExternalSubscriptionIdentifier());
        assertEquals(tenantIdentifier, tenant.getTenantID());
    }

    @AfterAll
    public static void cleanUpDynamoDB() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();

        client.deleteTable(request);
    }
}
