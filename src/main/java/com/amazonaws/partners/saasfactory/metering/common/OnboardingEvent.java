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

public class OnboardingEvent {

    private final String tenantID;
    private final String externalSubscriptionIdentifier;

    private OnboardingEvent(String tenantID,
                            String externalSubscriptionIdentifier) {
        this.tenantID = tenantID;
        this.externalSubscriptionIdentifier = externalSubscriptionIdentifier;
    }

    public static OnboardingEvent createOnboardingEvent(EventBridgeOnboardTenantEvent event) {
        String tenantID = event.getDetail().getTenantID();
        if (tenantID == null) {
            return null;
        }

        String externalSubscriptionIdentifier = event.getDetail().getExternalSubscriptionIdentifier();
        if (externalSubscriptionIdentifier == null) {
            return null;
        }

        return new OnboardingEvent(tenantID,
                                   externalSubscriptionIdentifier);

    }

    public String getTenantID() {
        return this.tenantID;
    }

    public String getExternalSubscriptionIdentifier() {
        return this.externalSubscriptionIdentifier;
    }
}
