# Reference Billing/Metering Service

This project contains a reference implementation for a serverless billing/metering service.

## Requirements

* Java, version 11 or higher
* Maven
* AWS SAM (https://aws.amazon.com/serverless/sam/)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

## Building

Build the SAM application by running the following command:

```
$ sam build
```

You can ignore the warnings about the JVM using a major version higher than 11 if you are
using a Java compiler version higher than 11. The pom file tells the compiler to build for a version
of Java that is compatible with Lambda.

## Testing

There is Maven lifecycle that defines tests of major functionality locally. In order to
test the OnboardNewTenant, ProcessBillingEvent, BillingEventAggregation and StripeBillingPublish, the
[DynamoDB Local](https://hub.docker.com/r/amazon/dynamodb-local/) container needs to be running in Docker
and listening on port 8000:

```shell script
$ docker run -p 8000:8000 amazon/dynamodb-local:latest
```

For the StripeBillingPublish test, the [stripe-mock container](https://hub.docker.com/r/stripemock/stripe-mock) needs
to be run according to the Docker usage instructions [here](https://github.com/stripe/stripe-mock)

The tests for TenantConfiguration and Constants do not require any external dependencies.

## Deploying

In order to deploy the application, run the following command:

```shell script
$ sam deploy --guided
```

## Initial Configuration

### Tenant Configuration

Tenants are configured in two places: within Stripe and within this application. Tenant configuration in Stripe must
happen before configuring a tenant in this application. There are several components that need to be created within
Stripe:
* Stripe Product (https://stripe.com/docs/api/products)
* Stripe Price (https://stripe.com/docs/api/prices)
* Stripe Customer (https://stripe.com/docs/api/customers)
* Stripe Subscription (https://stripe.com/docs/api/subscriptions)

The connection between the String Customer and Stripe Subscription creates the subscription item ID, an identifier
prefixed with "si\_". Please refer to the Stripe documentation for more information about how to create
these resources.

Once the subscription item ID exists in Stripe, the tenant can be onboarded into this application. This is done
through an event on the associated EventBridge. See the "Usage and Function" section below.

### Stripe API Key

**Do not use a production API key.**

The Stripe API key is stored within Secrets Manager. The Cloudformation stack creates an empty secret.
Access the secret through the Secrets Manager console and paste in a **restricted testing API key** with
the following permissions:

* Invoices - Read
* Subscriptions - Read
* Usage Records - Write

More information about how to create a restricted key can be found [here](https://stripe.com/docs/keys#limit-access).

Do not paste it in JSON format; the application expects to find a single string with the API key.

**Do not use a production API key.**

## Usage and Function

First, tenants need to be onboarded to this system. This is done by placing an event onto the EventBridge. The 
event is in the following format:

```json
{
  "TenantID": "Tenant0",
  "ExternalSubscriptionIdentifier": "si_00000000"
}
```

Where "TenantID" is some way to identify a tenant, and where "ExternalSubscriptionIdentifier" is the identifier 
associated with the subscription in the billing provider. In the case of Stripe Billing, this identifier will be 
the subscription ID, prefixed with "si\_".

This can be placed onto the EventBridge with the AWS CLI or with one of the AWS SDKs. Here is an example using the
AWS CLI:

```shell script
$ aws events put-events --entries file://exampleOnboardingEvent.json
```

The contents of exampleOnboardingEvent.json should be similar to the following:
```json
[
  {
    "Detail": "{ \"TenantID\": \"Tenant0\", \"ExternalSubscriptionIdentifier\": \"si_00000000\" }",
    "DetailType": "ONBOARD",
    "EventBusName": "BillingEventBridge",
    "Source": "command-line-test"
  }
]
```

Where the value of the EventBusName key is the name of the EventBridge associated with the SAM application. This is
BillingEventBridge by default.

Once a tenant is onboarded, place billing events onto the same EventBridge. The event is in the following format:

```json
{ 
  "TenantID": "Tenant0",
  "Quantity": 5
}
```

The billing event is in the following format:
* TenantID: the ID of the tenant that owns the billing event
* Quantity: the number of billing events that occurred

This can be placed onto the EventBridge with the AWS CLI or with one of the AWS SDKs. Here is an example using the
AWS CLI:

```shell script
$ aws events put-events --entries file://exampleBillingEvents.json
```

The contents of exampleBillingEvents.json should be similar to the following:
```json
[
  {
    "Detail": "{ \"TenantID\": \"Tenant0\", \"Quantity\": 5 }",
    "DetailType": "BILLING",
    "EventBusName": "BillingEventBridge",
    "Source": "command-line-test"
  },
  {
    "Detail": "{ \"TenantID\": \"Tenant0\", \"Quantity\": 10 }",
    "DetailType": "BILLING",
    "EventBusName": "BillingEventBridge",
    "Source": "command-line-test"
  }
]
```

After placing the event onto the EventBridge, a Lambda function processes the event and places it into a DynamoDB
table.

Where the value of the EventBusName key is the name of the EventBridge associated with the SAM application. This is
BillingEventBridge by default.

At a rate set by the user of the application, a Cloudwatch Scheduled Event runs a Step Function State Machine that 
aggregates the events in the DynamoDB table and publishes them to Stripe Billing.
