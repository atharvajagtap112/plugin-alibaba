package io.kestra.plugin.alibaba.sls;

import com.aliyun.openservices.log.request.GetLogsRequest;
import com.aliyun.openservices.log.response.GetLogsResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.alibaba.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query logs from Alibaba Cloud Simple Log Service (SLS)",
    description = "Queries a Logstore within a project over a specified time range, supporting fetch and store output modes."
)
@Plugin(
    examples = {
        @Example(
            title = "Query error logs from an SLS logstore",
            full = true,
            code = """
                id: sls_query
                namespace: company.team

                tasks:
                  - id: query_logs
                    type: io.kestra.plugin.alibaba.sls.Query
                    accessKeyId: "{{ secret('ALIBABA_ACCESS_KEY_ID') }}"
                    accessKeySecret: "{{ secret('ALIBABA_ACCESS_KEY_SECRET') }}"
                    region: cn-hangzhou
                    project: ops-logs
                    logstore: app-events
                    query: "level: ERROR"
                    fetchType: FETCH
                """
        )
    }
)
public class Query extends AbstractConnection implements RunnableTask<Query.Output> {
    @NotNull
    @Schema(title = "Project", description = "The SLS project name.")
    @PluginProperty(group = "main")
    private Property<String> project;

    @NotNull
    @Schema(title = "Logstore", description = "The SLS Logstore name.")
    @PluginProperty(group = "main")
    private Property<String> logstore;

    @Schema(title = "Query", description = "Search query or SQL analysis statement. Defaults to empty string.")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<String> query = Property.ofValue("");

    @Schema(title = "Topic", description = "The log topic filter. Defaults to empty string.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> topic = Property.ofValue("");

    @Schema(title = "From", description = "Start of time range as ISO instant or epoch seconds. Defaults to 15 minutes ago.")
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(title = "To", description = "End of time range as ISO instant or epoch seconds. Defaults to current time.")
    @PluginProperty(group = "main")
    private Property<String> to;

    @Builder.Default
    @Schema(title = "Offset", description = "Non-negative line offset for pagination; defaults to 0.")
    @PluginProperty(group = "processing")
    private Property<Integer> offset = Property.ofValue(0);

    @Builder.Default
    @Schema(title = "Lines", description = "Number of log lines to return, between 1 and 100; defaults to 100.")
    @PluginProperty(group = "processing")
    private Property<Integer> lines = Property.ofValue(100);

    @Builder.Default
    @Schema(title = "Reverse", description = "Sort in reverse chronological order; defaults to false.")
    @PluginProperty(group = "processing")
    private Property<Boolean> reverse = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Result handling mode", description = "FETCH, FETCH_ONE, STORE, or NONE. Defaults to FETCH.")
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rProject = required(runContext, project, "project");
        var rLogstore = required(runContext, logstore, "logstore");
        var rQuery = runContext.render(query).as(String.class).orElse("");
        var rTopic = runContext.render(topic).as(String.class).orElse("");
        var rOffset = runContext.render(offset).as(Integer.class).orElse(0);
        var rLines = runContext.render(lines).as(Integer.class).orElse(100);
        if (rOffset < 0 || rLines < 1 || rLines > 100) {
            throw new IllegalArgumentException("`offset` must be non-negative and `lines` must be between 1 and 100");
        }
        var rReverse = runContext.render(reverse).as(Boolean.class).orElse(false);
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        var toEpoch = parseEpochSecond(runContext.render(this.to).as(String.class).orElse(null), Math.toIntExact(Instant.now().getEpochSecond()));
        var fromEpoch = parseEpochSecond(runContext.render(this.from).as(String.class).orElse(null), toEpoch - 900);

        if (fromEpoch >= toEpoch) {
            throw new IllegalArgumentException("`from` must be earlier than `to`");
        }
        var request = new GetLogsRequest(rProject, rLogstore, fromEpoch, toEpoch, rTopic, rQuery, rOffset, rLines, rReverse);

        var start = Instant.now();
        GetLogsResponse response;
        var client = slsClient(runContext);
        try {
            response = client.GetLogs(request);
        } finally {
            client.shutdown();
        }
        if (!response.IsCompleted()) {
            throw new IllegalStateException("SLS query is incomplete; retry the query before using its results");
        }
        var end = Instant.now();
        runContext.metric(Timer.of("duration", Duration.between(start, end)));

        var queriedLogs = response.GetLogs();
        var rows = new ArrayList<Map<String, Object>>(queriedLogs.size());

        for (var qLog : queriedLogs) {
            var item = qLog.GetLogItem();
            var map = new LinkedHashMap<String, Object>();
            map.put("__time__", Integer.toUnsignedLong(item.GetTime()));
            map.put("__source__", qLog.GetSource());
            for (var content : item.GetLogContents()) {
                map.put(content.GetKey(), content.GetValue());
            }
            rows.add(map);
        }

        var count = (long) rows.size();
        runContext.metric(Counter.of("total.rows", count));

        var builder = Output.builder()
            .count(count);

        if (FetchType.NONE.equals(rFetchType)) {
            return builder.build();
        }

        if (FetchType.FETCH.equals(rFetchType)) {
            builder.logs(rows);
        } else if (FetchType.FETCH_ONE.equals(rFetchType)) {
            builder.log(rows.isEmpty() ? Collections.emptyMap() : rows.getFirst());
        } else if (FetchType.STORE.equals(rFetchType)) {
            var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
            try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                Long lineCount = FileSerde.writeAll(output, Flux.fromIterable(rows)).block();
                output.flush();
                var uri = runContext.storage().putFile(tempFile);
                builder.uri(uri).count(lineCount);
            }
        }

        return builder.build();
    }

    private int parseEpochSecond(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Math.toIntExact(Long.parseLong(value));
        } catch (NumberFormatException e) {
            try {
                return Math.toIntExact(Instant.parse(value).getEpochSecond());
            } catch (RuntimeException ex) {
                throw new IllegalArgumentException("Invalid SLS time: '" + value + "'; expected ISO instant or epoch seconds", ex);
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Fetched logs as a list of key-value maps", description = "Only populated if fetchType is FETCH")
        private List<Map<String, Object>> logs;

        @Schema(title = "First fetched log item as a key-value map", description = "Only populated if fetchType is FETCH_ONE")
        private Map<String, Object> log;

        @Schema(title = "Total number of log records returned")
        private Long count;

        @Schema(title = "URI of the stored results in internal storage", description = "Only populated if fetchType is STORE")
        private URI uri;
    }
}
