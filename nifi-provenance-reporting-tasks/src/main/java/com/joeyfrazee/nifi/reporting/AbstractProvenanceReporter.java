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
import java.text.SimpleDateFormat;
import java.util.*;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;

@Stateful(scopes = Scope.CLUSTER, description = "After querying the "
        + "provenance repository, the last seen event id is stored so "
        + "reporting can persist across restarts of the reporting task or "
        + "NiFi. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation.")
public abstract class AbstractProvenanceReporter extends AbstractReportingTask {

    private static final String LAST_EVENT_ID_KEY = "lastEventId";
    private static final String TIMESTAMP_FORMAT = "YYYY-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final PropertyDescriptor REPORT_IN_BATCH_MODE = new PropertyDescriptor
            .Builder().name("Report in Batch Mode")
            .displayName("Report in Batch Mode")
            .description("Makes the underlying reporting to be in batches instead of per event")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor
            .Builder().name("Page Size")
            .displayName("Page Size")
            .description("Page size for scrolling through the provenance repository")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private volatile long firstEventId = -1L;

    protected List<PropertyDescriptor> descriptors;

    private Map<String,String> createComponentMap(final ProcessGroupStatus status) {
        final Map<String,String> componentMap = new HashMap<>();

        if (status != null) {
            componentMap.put(status.getId(), status.getName());

            for (final ProcessorStatus procStatus : status.getProcessorStatus()) {
                componentMap.put(procStatus.getId(), procStatus.getName());
            }

            for (final PortStatus portStatus : status.getInputPortStatus()) {
                componentMap.put(portStatus.getId(), portStatus.getName());
            }

            for (final PortStatus portStatus : status.getOutputPortStatus()) {
                componentMap.put(portStatus.getId(), portStatus.getName());
            }

            for (final RemoteProcessGroupStatus rpgStatus : status.getRemoteProcessGroupStatus()) {
                componentMap.put(rpgStatus.getId(), rpgStatus.getName());
            }

            for (final ProcessGroupStatus childGroup : status.getProcessGroupStatus()) {
                componentMap.put(childGroup.getId(), childGroup.getName());
            }
        }

        return componentMap;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PAGE_SIZE);
        descriptors.add(REPORT_IN_BATCH_MODE);
        return descriptors;
    }

    public abstract void indexEvent(final JsonObject event, final ReportingContext context) throws IOException;

    public abstract void indexEvents(final ArrayList<JsonObject> events, final ReportingContext context) throws IOException;

    private void processEvents(ReportingContext context, JsonBuilderFactory factory, JsonObjectBuilder builder, Map<String, String> componentMap, List<ProvenanceEventRecord> events) throws  IOException{
        final SimpleDateFormat df = new SimpleDateFormat (TIMESTAMP_FORMAT);

        if (context.getProperty(REPORT_IN_BATCH_MODE).asBoolean()){
            ArrayList<JsonObject> jsonEvents = new ArrayList<>();
            for (ProvenanceEventRecord e : events) {
                final String componentName = componentMap.get(e.getComponentId());
                jsonEvents.add(EventSerializer.serialize(factory, builder, e, df, componentName));
            }
            indexEvents(jsonEvents, context);
        }
        else {
            for (ProvenanceEventRecord e : events) {
                final String componentName = componentMap.get(e.getComponentId());
                JsonObject event = EventSerializer.serialize(factory, builder, e, df, componentName);
                indexEvent(event, context);
            }
        }
    }

    private boolean setFirstEventIdForCurrentRun(ReportingContext context, Long currMaxId) {
        if (firstEventId < 0) {
            try{
                String value = StateManagementUtilities.getStateEntry(LAST_EVENT_ID_KEY, context.getStateManager());
                if (value != null) {
                    firstEventId = Long.parseLong(value);
                }
                else {
                    firstEventId = currMaxId;
                    try {
                        StateManagementUtilities.setStateEntry(LAST_EVENT_ID_KEY, context.getStateManager(), String.valueOf(firstEventId));
                    } catch (final IOException ioe) {
                        getLogger().error("Failed to retrieve State Manager, while saving, from context due to: " + ioe.getMessage(), ioe);
                        return false;
                    }
                }
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve State Manager, while getting, from context due to: " + ioe.getMessage(), ioe);
                return false;
            }

            if(currMaxId < firstEventId){
                getLogger().warn("Current provenance max id is {} which is less than what was stored in state as the last queried event, which was {}. This means the provenance restarted its "+
                        "ids. Restarting querying from the beginning.", new Object[]{currMaxId, firstEventId});
                firstEventId = -1;
            }
        }
        return true;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final ProcessGroupStatus procGroupStatus = context.getEventAccess().getControllerStatus();
        final Map<String,String> componentMap = createComponentMap(procGroupStatus);

        Long currMaxId = context.getEventAccess().getProvenanceRepository().getMaxEventId();

        if(currMaxId == null) {
            getLogger().debug("No events to send because no events have been created yet.");
            return;
        }

        if (!setFirstEventIdForCurrentRun(context, currMaxId)) return;

        if (currMaxId == firstEventId) {
            getLogger().debug("No events to send due to the current max id being equal to the last id that was queried.");
            return;
        }

        List<ProvenanceEventRecord> events;
        try {
            events = context.getEventAccess().getProvenanceEvents(firstEventId, context.getProperty(PAGE_SIZE).asInteger());
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve Provenance Events from repository due to: " + ioe.getMessage(), ioe);
            return;
        }

        while(events != null && !events.isEmpty()) {
            try {
                processEvents(context, factory, builder, componentMap, events);
            }
            catch (IOException ioe){
                getLogger().error("Failed to index Event, with batch mode set to {" + context.getProperty(REPORT_IN_BATCH_MODE).asBoolean() + "} due to: " + ioe.getMessage(), ioe);
                return;
            }

            final ProvenanceEventRecord lastEvent = events.get(events.size() - 1);

            try {
                StateManagementUtilities.setStateEntry(LAST_EVENT_ID_KEY, context.getStateManager(), String.valueOf(lastEvent.getEventId()));
                firstEventId = lastEvent.getEventId() + 1;
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve State Manager, while saving, from context due to: " + ioe.getMessage(), ioe);
                return;
            }

            try {
                events = context.getEventAccess().getProvenanceEvents(firstEventId, context.getProperty(PAGE_SIZE).asInteger());
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve Provenance Events from repository due to: " + ioe.getMessage(), ioe);
                return;
            }
        }
    }
}
