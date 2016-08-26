package com.joeyfrazee.nifi.reporting;

import org.apache.nifi.provenance.ProvenanceEventRecord;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;

/**
 * Created by lev on 8/25/16.
 */
public class EventSerializer {
    public static JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder, final ProvenanceEventRecord event, final SimpleDateFormat df, final String componentName) {

        addField(builder, "@timestamp", df.format(event.getEventTime()));

        addField(builder, "event_id", event.getEventId());
        addField(builder, "event_type", event.getEventType().name());
        addField(builder, "event_time", df.format(event.getEventTime()));
        addField(builder, "entry_date", df.format(event.getFlowFileEntryDate()));
        addField(builder, "lineage_start_date", df.format(event.getLineageStartDate()));

        addField(builder, "event_duration_millis", event.getEventDuration());
        addField(builder, "event_duration_seconds", event.getEventDuration() / 1000);

        addField(builder, "file_size", event.getFileSize());
        addField(builder, "previous_file_size", event.getPreviousFileSize());

        addField(builder, "component_id", event.getComponentId());
        addField(builder, "component_type", event.getComponentType());
        addField(builder, "component_name", componentName);

        addField(builder, "source_system_id", event.getSourceSystemFlowFileIdentifier());

        addField(builder, "flow_file_id", event.getFlowFileUuid());

        addField(builder, factory, "parent_ids", event.getParentUuids());

        addField(builder, factory, "child_ids", event.getChildUuids());

        addField(builder, "details", event.getDetails());

        addField(builder, "relationship", event.getRelationship());

        addField(builder, "source_queue_id", event.getSourceQueueIdentifier());

        addField(builder, factory, "updated_attributes", event.getUpdatedAttributes());
        addField(builder, factory, "previous_attributes", event.getPreviousAttributes());


        return builder.build();
    }

    private static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Map<String, String> values) {
        if (values == null) {
            return;
        }

        final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
        for (final Map.Entry<String, String> entry : values.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }

            mapBuilder.add(entry.getKey().replace('.','_'), entry.getValue());
        }

        builder.add(key.replace('.','_'), mapBuilder);
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final Long value) {
        if (value != null) {
            builder.add(key.replace('.','_'), value.longValue());
        }
    }

    private static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Collection<String> values) {
        if (values == null) {
            return;
        }

        builder.add(key.replace('.','_'), createJsonArray(factory, values));
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final String value) {
        if (value == null) {
            return;
        }

        builder.add(key.replace('.','_'), value);
    }

    private static JsonArrayBuilder createJsonArray(JsonBuilderFactory factory, final Collection<String> values) {
        final JsonArrayBuilder builder = factory.createArrayBuilder();
        for (final String value : values) {
            if (value != null) {
                builder.add(value);
            }
        }
        return builder;
    }

}
