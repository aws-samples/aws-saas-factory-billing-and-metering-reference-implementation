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
package onboarding;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import com.amazonaws.partners.saasfactory.metering.onboarding.OnboardNewTenant;
import com.amazonaws.partners.saasfactory.metering.common.TableConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

class OnboardNewTenantTest {

    private static DynamoDbClient client;
    private static final String tableName = "TestTenantOnboardingTable";
    private static final String indexName = "TestTenantOnboardingIndex";
    private static final String tenantID = "Tenant0";
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

        logger = LoggerFactory.getLogger(OnboardNewTenantTest.class);
    }

    @Test
    void shouldAddNewTenant() {
        String onboardingJSON = String.format(
                "{ \"detail\": { \"TenantID\": \"%s\", \"ExternalSubscriptionIdentifier\": \"%s\" }}",
                tenantID,
                external_subscription_identifier);
        InputStream inputStream = new ByteArrayInputStream(onboardingJSON.getBytes(StandardCharsets.UTF_8));
        OutputStream outputStream = new ByteArrayOutputStream();
        TableConfiguration tableConfig = new TableConfiguration(tableName, indexName);
        OnboardNewTenant onboardNewTenant = new OnboardNewTenant(client, tableConfig);
        Context context = null;
        onboardNewTenant.handleRequest(inputStream, outputStream, context);

        HashMap<String, AttributeValue> key = new HashMap<>();
        String data_type = "TENANT#Tenant0";
        String sub_type = "CONFIG";

        key.put("data_type", AttributeValue.builder()
                .s(data_type)
                .build());
        key.put("sub_type", AttributeValue.builder()
                .s(sub_type)
                .build());

        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();

        GetItemResponse response = client.getItem(request);

        assertEquals(data_type, response.item().get("data_type").s());
        assertEquals(sub_type, response.item().get("sub_type").s());
        assertEquals(external_subscription_identifier, response.item().get("external_subscription_identifier").s());
    }

    @AfterAll
    public static void cleanUpDynamoDB() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();

        client.deleteTable(request);
    }
}
