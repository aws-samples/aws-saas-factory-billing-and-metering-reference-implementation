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

import com.google.gson.annotations.SerializedName;

public class EventBridgeEvent {

    private String version;
    private String id;
    @SerializedName("detail-type")
    private String detail_type;
    private String source;
    private String account;
    private String time;
    private String region;
    private String[] resources;

    public EventBridgeEvent() {
        // This constructor is empty because this class is a container for an event
        // from EventBridge. This class is used for transforming JSON into a
        // Java object
    }

    public String getVersion() {
        return version;
    }

    public String getId() {
        return id;
    }

    public String getDetail_type() {
        return detail_type;
    }

    public String getSource() {
        return source;
    }

    public String getAccount() {
        return account;
    }

    public String getTime() {
        return time;
    }

    public String getRegion() {
        return region;
    }

    public String[] getResources() {
        return resources;
    }

}
