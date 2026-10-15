package io.kestra.plugin.alibaba.sls;

import com.aliyun.openservices.log.common.LogItem;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.alibaba.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push logs to Alibaba Cloud Simple Log Service (SLS)",
    description = "Writes log records to an SLS Logstore from an inline list of maps, a single map, or a file in Kestra internal storage."
)
@Plugin(
    examples = {
        @Example(
            title = "Push inline logs to SLS",
            full = true,
            code = """
                id: sls_push
                namespace: company.team

                tasks:
                  - id: push_logs
                    type: io.kestra.plugin.alibaba.sls.Push
                    accessKeyId: "{{ secret('ALIBABA_ACCESS_KEY_ID') }}"
                    accessKeySecret: "{{ secret('ALIBABA_ACCESS_KEY_SECRET') }}"
                    region: cn-hangzhou
                    project: ops-logs
                    logstore: app-events
                    from:
                      - level: INFO
                        message: Flow finished
                        flow: "{{ flow.id }}"
                """
        )
    }
)
public class Push extends AbstractConnection implements RunnableTask<Push.Output>, Data.From {
    @NotNull
    @Schema(title = "Project", description = "The SLS project name.")
    @PluginProperty(group = "main")
    private Property<String> project;

    @NotNull
    @Schema(title = "Logstore", description = "The SLS Logstore name.")
    @PluginProperty(group = "main")
    private Property<String> logstore;

    @Schema(title = "Topic", description = "The log topic.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> topic = Property.ofValue("");

    @Schema(title = "Source", description = "The log source. Defaults to 'kestra'.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> source = Property.ofValue("kestra");

    @NotNull
    @Schema(
        title = "Source data",
        description = "Inline map, list of maps, JSON string, or URI of an Ion file in Kestra internal storage."
    )
    @PluginProperty(group = "main")
    private Object from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rProject = required(runContext, project, "project");
        var rLogstore = required(runContext, logstore, "logstore");
        var rTopic = runContext.render(topic).as(String.class).orElse("");
        var rSource = runContext.render(source).as(String.class).orElse("kestra");
        if (from == null) {
            throw new IllegalArgumentException("`from` is required");
        }
        var client = slsClient(runContext);
        var count = new AtomicLong();
        var recordIndex = new AtomicLong();
        try {
            Data.from(from).read(runContext)
                .map(record -> logItem(record, recordIndex.incrementAndGet()))
                .buffer(4096)
                .concatMap(batch -> Mono.fromCallable(() -> {
                    client.PutLogs(rProject, rLogstore, rTopic, batch, rSource);
                    count.addAndGet(batch.size());
                    return batch.size();
                }), 1)
                .blockLast();
        } finally {
            client.shutdown();
        }
        runContext.metric(Counter.of("pushed.logs", count.get()));
        return Output.builder().count(count.get()).build();
    }

    private LogItem logItem(Map<String, Object> record, long index) {
        var field = record.containsKey("__time__") ? "__time__" : record.containsKey("timestamp") ? "timestamp" : null;
        var time = Instant.now().getEpochSecond();
        if (field != null) {
            try {
                time = field.equals("__time__") ? Long.parseLong(String.valueOf(record.get(field)))
                    : Instant.parse(String.valueOf(record.get(field))).getEpochSecond();
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Record " + index + " has invalid `" + field + "`", e);
            }
        }
        if (time < 0 || time > 0xffffffffL) {
            throw new IllegalArgumentException("Record " + index + " timestamp must fit SLS unsigned 32-bit epoch seconds");
        }
        var item = new LogItem((int) time);
        for (var entry : record.entrySet()) {
            if (!entry.getKey().equals("__time__")) {
                item.PushBack(entry.getKey(), entry.getValue() == null ? "" : String.valueOf(entry.getValue()));
            }
        }
        return item;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The total number of log records pushed")
        private final Long count;
    }
}
