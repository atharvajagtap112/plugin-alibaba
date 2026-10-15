package io.kestra.plugin.alibaba.sls;

import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.request.GetCursorRequest;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.alibaba.AbstractConnection;
import io.kestra.plugin.alibaba.AlibabaConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new logs in Alibaba Cloud Simple Log Service (SLS)",
    description = "Consumes new SLS log groups using shard cursors persisted in namespace KV. Late-arriving logs are consumed by ingestion position, independently of their event timestamps."
)
@Plugin(
    examples = {
        @Example(
            title = "React to error logs in SLS",
            full = true,
            code = """
                id: sls_on_errors
                namespace: company.team

                triggers:
                  - id: on_errors
                    type: io.kestra.plugin.alibaba.sls.Trigger
                    accessKeyId: "{{ secret('ALIBABA_ACCESS_KEY_ID') }}"
                    accessKeySecret: "{{ secret('ALIBABA_ACCESS_KEY_SECRET') }}"
                    region: cn-hangzhou
                    project: ops-logs
                    logstore: app-events
                    query: "* | where level == 'ERROR'"
                    fetchType: STORE
                    interval: PT1M

                tasks:
                  - id: alert
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.count }} error logs received"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output>, AlibabaConnectionInterface {
    @Builder.Default
    @Schema(title = "Polling interval", description = "Interval between query runs; defaults to 60 seconds.")
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @NotNull
    @Schema(title = "The AccessKey ID")
    @PluginProperty(group = "connection")
    protected Property<String> accessKeyId;

    @NotNull
    @Schema(title = "The AccessKey Secret")
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<String> accessKeySecret;

    @Schema(title = "Security Token")
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<String> securityToken;

    @Schema(title = "Region", description = "Region used to derive the endpoint when endpointOverride is absent.")
    @PluginProperty(group = "connection")
    protected Property<String> region;

    @Schema(title = "Endpoint override")
    @PluginProperty(group = "connection")
    protected Property<String> endpointOverride;

    @NotNull
    @Schema(title = "Project", description = "The SLS project name.")
    @PluginProperty(group = "main")
    private Property<String> project;

    @NotNull
    @Schema(title = "Logstore", description = "The SLS Logstore name.")
    @PluginProperty(group = "main")
    private Property<String> logstore;

    @Builder.Default
    @Schema(title = "Query", description = "SPL consumption filter, such as * | where level == 'ERROR'. Defaults to empty. Index search syntax and SQL analysis are not supported.")
    @PluginProperty(group = "main")
    private Property<String> query = Property.ofValue("");

    @Builder.Default
    @Schema(title = "Topic", description = "Topic filter.")
    @PluginProperty(group = "advanced")
    private Property<String> topic = Property.ofValue("");

    @Builder.Default
    @Schema(title = "Initial lookback duration", description = "Lookback duration when no prior cursor exists in the KV store; defaults to 5 minutes.")
    @PluginProperty(group = "processing")
    private Property<Duration> lookback = Property.ofValue(Duration.ofMinutes(5));

    @Builder.Default
    @Schema(title = "Result handling mode", description = "FETCH or STORE; defaults to FETCH. FETCH accumulates the complete matching backlog in memory and execution variables. Use STORE for high-volume logstores, long lookback windows, or catch-up after downtime; STORE writes results incrementally to internal storage.")
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var rProject = AbstractConnection.required(runContext, project, "project");
        var rLogstore = AbstractConnection.required(runContext, logstore, "logstore");
        var rQuery = runContext.render(query).as(String.class).orElse("");
        var rTopic = runContext.render(topic).as(String.class).orElse("");
        var rLookback = runContext.render(lookback).as(Duration.class).orElse(Duration.ofMinutes(5));
        if (rLookback.isNegative()) {
            throw new IllegalArgumentException("`lookback` must be non-negative");
        }
        var mode = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        if (mode != FetchType.FETCH && mode != FetchType.STORE) {
            throw new IllegalArgumentException("Trigger `fetchType` must be FETCH or STORE");
        }
        var key = cursorKey(context.getFlowId(), getId(), rProject, rLogstore);
        var persisted = runContext.namespaceKv(context.getNamespace()).getValue(key);
        var cursors = new LinkedHashMap<String, String>();
        if (persisted.isPresent()) {
            var state = JacksonMapper.ofJson().readValue((byte[]) persisted.get().value(),
                new TypeReference<Map<String, String>>() {});
            if (state == null || state.entrySet().stream().anyMatch(entry -> entry.getKey() == null
                || entry.getKey().isBlank() || entry.getValue() == null || entry.getValue().isBlank())) {
                throw new IllegalArgumentException("Persisted SLS shard cursors must contain non-blank shard IDs and cursors");
            }
            cursors.putAll(state);
        }
        var rows = mode == FetchType.FETCH ? new ArrayList<Map<String, Object>>() : null;
        var file = mode == FetchType.STORE ? runContext.workingDir().createTempFile(".ion").toFile() : null;
        var count = 0L;
        var client = AbstractConnection.slsClient(runContext, this);
        try (var output = file == null ? null : new BufferedOutputStream(new FileOutputStream(file), FileSerde.BUFFER_SIZE)) {
            for (var shard : client.ListShard(rProject, rLogstore).GetShards()) {
                var shardId = shard.GetShardId();
                var shardKey = String.valueOf(shardId);
                var cursor = cursors.get(shardKey);
                if (cursor == null) {
                    cursor = client.GetCursor(new GetCursorRequest(rProject, rLogstore, shardId,
                        Instant.now().minus(rLookback).getEpochSecond())).GetCursor();
                }
                var end = client.GetCursor(rProject, rLogstore, shardId, Consts.CursorMode.END).GetCursor();
                while (!cursor.equals(end)) {
                    var request = new PullLogsRequest(rProject, rLogstore, shardId, 100, cursor, end, rQuery);
                    var response = client.pullLogs(request);
                    var next = response.getNextCursor();
                    if (next == null || next.isBlank() || next.equals(cursor)) {
                        throw new IllegalStateException("SLS shard " + shardId + " cursor did not advance; retry this poll");
                    }
                    for (var data : response.getLogGroups()) {
                        var group = data.GetLogGroup();
                        if (!rTopic.isEmpty() && !rTopic.equals(group.getTopic())) {
                            continue;
                        }
                        for (var log : group.getLogsList()) {
                            var row = new LinkedHashMap<String, Object>();
                            row.put("__time__", Integer.toUnsignedLong(log.getTime()));
                            row.put("__source__", group.getSource());
                            for (var content : log.getContentsList()) {
                                row.put(content.getKey(), content.getValue());
                            }
                            if (output == null) {
                                rows.add(row);
                            } else {
                                FileSerde.write(output, row);
                            }
                            count++;
                        }
                    }
                    cursor = next;
                }
                cursors.put(shardKey, cursor);
            }
        } finally {
            client.shutdown();
        }
        var builder = Query.Output.builder().count(count);
        if (file != null && count > 0) {
            builder.uri(runContext.storage().putFile(file));
        } else {
            builder.logs(rows);
        }
        var execution = count == 0 ? Optional.<Execution>empty()
            : Optional.of(TriggerService.generateExecution(this, conditionContext, context, builder.build()));
        runContext.namespaceKv(context.getNamespace()).put(key,
            new KVValueAndMetadata(new KVMetadata("SLS shard cursors", (Duration) null), JacksonMapper.ofJson().writeValueAsBytes(cursors)));
        return execution;
    }

    public static String cursorKey(String flowId, String triggerId, String project, String logstore) {
        return "sls_cursor_" + segment(flowId) + segment(triggerId) + segment(project) + segment(logstore);
    }

    private static String segment(String value) {
        return value.length() + "_" + value + "_";
    }
}
