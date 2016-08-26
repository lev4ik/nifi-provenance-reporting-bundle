/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.joeyfrazee.nifi.reporting;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import javax.json.JsonObject;

@Tags({"http", "provenance"})
@CapabilityDescription("A provenance reporting task that posts to an HTTP server")
public class HttpProvenanceReporter extends AbstractProvenanceReporter {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    public static final PropertyDescriptor URL = new PropertyDescriptor
            .Builder().name("URL")
            .displayName("URL")
            .description("The URL to post to")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private AtomicReference<OkHttpClient> client = new AtomicReference<OkHttpClient>();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(URL);
        return descriptors;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        client.set(new OkHttpClient());
    }

    private OkHttpClient getHttpClient() {
        return client.get();
    }

    private void post(String json, String url) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
            .url(url)
            .post(body)
            .build();
        Response response = getHttpClient().newCall(request).execute();
        getLogger().info("{} {} {}", new Object[]{Integer.valueOf(response.code()), response.message(), response.body().string()});
    }

    public void indexEvent(final JsonObject event, final ReportingContext context) throws IOException {
        final String url = context.getProperty(URL).getValue();
        post(event.toString(), url);
    }

    public void indexEvents(final ArrayList<JsonObject> events, final ReportingContext context) throws IOException {
        for (JsonObject event: events){
            indexEvent(event, context);
        }
    }
}
