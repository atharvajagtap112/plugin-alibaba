package io.kestra.plugin.alibaba.maxcompute;

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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on MaxCompute query results",
    description = "Periodically executes a MaxCompute SQL query and triggers a flow execution when rows are returned. Supports persisting watermarks in the namespace KV store to prevent duplicate processing."
)
@Plugin(
    examples = {
        @Example(
            title = "Poll for new records in MaxCompute",
            full = true,
            code = """
                id: maxcompute_listen
                namespace: company.team

                tasks:
                  - id: log_rows
                    type: io.kestra.plugin.core.log.Log
                    message: "Found {{ trigger.size }} new rows"

                triggers:
                  - id: watch_orders
                    type: io.kestra.plugin.alibaba.maxcompute.Trigger
                    accessKeyId: "{{ secret('ALIBABA_ACCESS_KEY_ID') }}"
                    accessKeySecret: "{{ secret('ALIBABA_ACCESS_KEY_SECRET') }}"
                    region: cn-shanghai
                    project: analytics
                    sql: "SELECT * FROM orders WHERE id > '{{ trigger.watermark }}' ORDER BY id ASC LIMIT 50"
                    watermarkField: id
                    initialWatermark: "0"
                    interval: PT5M
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output>, AlibabaConnectionInterface {
    @Builder.Default
    @Schema(
        title = "Polling interval",
        description = "Interval between query runs; defaults to 60 seconds."
    )
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
    @Schema(title = "Project", description = "MaxCompute project name.")
    @PluginProperty(group = "main")
    private Property<String> project;

    @NotNull
    @Schema(title = "SQL query", description = "Query executed on each poll. May reference {{ trigger.watermark }}, escaped for a single-quoted SQL literal. Use an appropriate initialWatermark and filter strictly greater than the watermark.")
    @PluginProperty(group = "main")
    private Property<String> sql;

    @NotNull
    @Schema(title = "Watermark column", description = "Required monotonically increasing column. Numeric values compare numerically; ISO instants compare chronologically; other strings compare lexicographically. Order ascending and avoid splitting equal watermark values across batches.")
    @PluginProperty(group = "processing")
    private Property<String> watermarkField;

    @Builder.Default
    @Schema(title = "Result handling mode", description = "FETCH or STORE; defaults to FETCH.")
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Builder.Default
    @Schema(title = "Initial watermark", description = "Value exposed before the first poll; defaults to empty string. For numeric IDs, set a lower bound such as 0.")
    @PluginProperty(group = "processing")
    private Property<String> initialWatermark = Property.ofValue("");

    @Schema(title = "Tunnel endpoint override")
    @PluginProperty(group = "advanced")
    private Property<String> tunnelEndpoint;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var namespace = context.getNamespace();
        var key = watermarkKey(context.getFlowId(), getId());
        var field = AbstractConnection.required(runContext, watermarkField, "watermarkField");
        var mode = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        if (mode != FetchType.FETCH && mode != FetchType.STORE) {
            throw new IllegalArgumentException("Trigger `fetchType` must be FETCH or STORE");
        }
        var persisted = runContext.namespaceKv(namespace).getValue(key);
        Watermark previous = persisted.isPresent()
            ? JacksonMapper.ofJson().readValue((byte[]) persisted.get().value(), Watermark.class) : null;
        if (persisted.isPresent() && previous == null) {
            throw new IllegalArgumentException("Persisted MaxCompute watermark must not be null");
        }
        var value = previous == null ? runContext.render(initialWatermark).as(String.class).orElse("") : previous.value();
        var escaped = escapeWatermark(value);
        var variables = Map.<String, Object>of("watermark", escaped, "trigger", Map.of("watermark", escaped));
        var renderedSql = runContext.render(sql).as(String.class, variables).filter(sql -> !sql.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("`sql` is required"));
        var task = Query.builder().id(getId()).type(Query.class.getName())
            .accessKeyId(accessKeyId).accessKeySecret(accessKeySecret).securityToken(securityToken)
            .region(region).endpointOverride(endpointOverride).project(project)
            .sql(Property.ofValue(renderedSql)).fetchType(fetchType).tunnelEndpoint(tunnelEndpoint).build();
        var output = task.run(runContext);
        if (output.getSize() == null || output.getSize() == 0) {
            return Optional.empty();
        }
        var max = new AtomicReference<>(previous);
        Consumer<Map<?, ?>> track = row -> {
            var next = Watermark.of(row.get(field));
            if (max.get() == null || next.compareTo(max.get()) > 0) {
                max.set(next);
            }
        };
        if (mode == FetchType.STORE) {
            try (var input = runContext.storage().getFile(output.getUri())) {
                FileSerde.read(input, row -> {
                    if (!(row instanceof Map<?, ?> map)) {
                        throw new IllegalArgumentException("Stored MaxCompute result must contain rows");
                    }
                    track.accept(map);
                });
            }
        } else {
            output.getRows().forEach(track);
        }
        if (max.get().equals(previous)) {
            return Optional.empty();
        }
        var execution = TriggerService.generateExecution(this, conditionContext, context, output);
        runContext.namespaceKv(namespace).put(key, new KVValueAndMetadata(new KVMetadata("MaxCompute watermark", (Duration) null),
            JacksonMapper.ofJson().writeValueAsBytes(max.get())));
        return Optional.of(execution);
    }

    public static String watermarkKey(String flowId, String triggerId) {
        return "maxcompute_watermark_" + flowId.length() + "_" + flowId + "_" + triggerId.length() + "_" + triggerId;
    }

    static String escapeWatermark(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t").replace("\0", "\\0");
    }

    public record Watermark(String type, String value) implements Comparable<Watermark> {
        public Watermark {
            if (type == null || value == null) {
                throw new IllegalArgumentException("Watermark type and value must not be null");
            }
            switch (type) {
                case "number" -> new BigDecimal(value);
                case "instant" -> Instant.parse(value);
                case "string" -> { }
                default -> throw new IllegalArgumentException("Unsupported persisted watermark type: " + type);
            }
        }

        static Watermark of(Object value) {
            if (value == null) {
                throw new IllegalArgumentException("Every row must have a non-null `watermarkField`");
            }
            if (value instanceof Number number) {
                return new Watermark("number", new BigDecimal(number.toString()).toPlainString());
            }
            if (value instanceof String string) {
                try {
                    return new Watermark("instant", Instant.parse(string).toString());
                } catch (DateTimeParseException e) {
                    return new Watermark("string", string);
                }
            }
            throw new IllegalArgumentException("Unsupported watermark type: " + value.getClass().getName());
        }

        @Override
        public int compareTo(Watermark other) {
            if (!type.equals(other.type)) {
                throw new IllegalArgumentException("Watermark column type changed from " + other.type + " to " + type);
            }
            return switch (type) {
                case "number" -> new BigDecimal(value).compareTo(new BigDecimal(other.value));
                case "instant" -> Instant.parse(value).compareTo(Instant.parse(other.value));
                case "string" -> value.compareTo(other.value);
                default -> throw new IllegalArgumentException("Unsupported persisted watermark type: " + type);
            };
        }
    }
}
