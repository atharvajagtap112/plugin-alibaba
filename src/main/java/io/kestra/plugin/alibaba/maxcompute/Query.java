package io.kestra.plugin.alibaba.maxcompute;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.AbstractChar;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.task.SQLTask;
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
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a MaxCompute SQL query",
    description = "Runs a SQL statement on Alibaba Cloud MaxCompute and retrieves or stores the result set. Supports cancellation via instance termination."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a MaxCompute query and store the result",
            full = true,
            code = """
                id: maxcompute_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.alibaba.maxcompute.Query
                    accessKeyId: "{{ secret('ALIBABA_ACCESS_KEY_ID') }}"
                    accessKeySecret: "{{ secret('ALIBABA_ACCESS_KEY_SECRET') }}"
                    region: cn-shanghai
                    project: analytics
                    sql: |
                      SELECT region, SUM(amount) AS total
                      FROM orders
                      WHERE ds = '{{ now() | date('yyyyMMdd') }}'
                      GROUP BY region
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Execute a query and fetch rows",
            full = true,
            code = """
                id: maxcompute_fetch
                namespace: company.team

                tasks:
                  - id: fetch_data
                    type: io.kestra.plugin.alibaba.maxcompute.Query
                    accessKeyId: "{{ secret('ALIBABA_ACCESS_KEY_ID') }}"
                    accessKeySecret: "{{ secret('ALIBABA_ACCESS_KEY_SECRET') }}"
                    region: cn-hangzhou
                    project: my_project
                    sql: "SELECT id, name FROM users LIMIT 100"
                    fetchType: FETCH
                """
        )
    }
)
public class Query extends AbstractConnection implements RunnableTask<Query.Output> {
    @NotNull
    @Schema(
        title = "Project",
        description = "The MaxCompute project name."
    )
    @PluginProperty(group = "main")
    private Property<String> project;

    @NotNull
    @Schema(
        title = "SQL query",
        description = "The SQL query to execute."
    )
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Builder.Default
    @Schema(
        title = "Result handling mode",
        description = "FETCH_ONE, FETCH, STORE, or NONE. Defaults to NONE."
    )
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Schema(
        title = "Tunnel endpoint override",
        description = "Custom tunnel endpoint URL. If omitted, default tunnel routing is used."
    )
    @PluginProperty(group = "advanced")
    private Property<String> tunnelEndpoint;

    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicBoolean isKilled = new AtomicBoolean();

    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicReference<Runnable> killable = new AtomicReference<>();

    @Override
    public Output run(RunContext runContext) throws Exception {
        checkKilled();
        var rProject = required(runContext, project, "project");
        var rSql = required(runContext, sql, "sql");
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.NONE);
        var rEndpoint = renderedEndpoint(runContext);
        if (rEndpoint == null || rEndpoint.isBlank()) {
            rEndpoint = "https://service." + renderedRegion(runContext) + ".maxcompute.aliyun.com/api";
        }
        var token = renderedSecurityToken(runContext);
        Account account = token == null || token.isBlank()
            ? new AliyunAccount(renderedAccessKeyId(runContext), renderedAccessKeySecret(runContext))
            : new StsAccount(renderedAccessKeyId(runContext), renderedAccessKeySecret(runContext), token);
        var odps = new Odps(account);
        odps.setEndpoint(rEndpoint);
        odps.setDefaultProject(rProject);
        var tunnel = runContext.render(tunnelEndpoint).as(String.class).orElse(null);
        var taskName = "kestra_" + UUID.randomUUID().toString().replace("-", "");
        var start = Instant.now();
        var instance = SQLTask.run(odps, rProject, rSql, taskName, null, null);
        Runnable stop = () -> {
            try {
                if (!instance.isTerminated()) {
                    instance.stop();
                }
            } catch (Exception e) {
                runContext.logger().warn("Could not stop MaxCompute instance '{}'", instance.getId(), e);
            }
        };
        killable.set(stop);
        if (isKilled.get()) {
            stopInstance();
        }
        try {
            checkKilled();
            instance.waitForSuccess();
            checkKilled();
            runContext.metric(Timer.of("duration", Duration.between(start, Instant.now())));
            var builder = Output.builder().instanceId(instance.getId());
            if (rFetchType == FetchType.NONE) {
                return builder.size(0L).build();
            }
            var records = SQLTask.getResultSet(instance, taskName, rFetchType == FetchType.FETCH_ONE ? 1L : null,
                false, tunnel == null || tunnel.isBlank() ? null : URI.create(tunnel));
            var rows = rFetchType == FetchType.STORE ? null : new ArrayList<Map<String, Object>>();
            var tempFile = rFetchType == FetchType.STORE ? runContext.workingDir().createTempFile(".ion").toFile() : null;
            var count = 0L;
            try (var output = tempFile == null ? null : new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                for (var record : records) {
                    checkKilled();
                    var row = new LinkedHashMap<String, Object>();
                    for (var i = 0; i < record.getColumnCount(); i++) {
                        var column = record.getColumns()[i];
                        var value = column.getTypeInfo().getOdpsType() == OdpsType.STRING
                            ? record.getString(i) : record.get(i);
                        if (value instanceof AbstractChar<?> text) {
                            value = text.getValue();
                        } else if (value instanceof Binary binary) {
                            value = binary.data();
                        } else if (value instanceof java.sql.Date date) {
                            value = date.toLocalDate().toString();
                        } else if (value instanceof Date date) {
                            value = date.toInstant().toString();
                        }
                        row.put(column.getName(), value);
                    }
                    if (output == null) {
                        rows.add(row);
                    } else {
                        FileSerde.write(output, row);
                    }
                    count++;
                }
            }
            runContext.metric(Counter.of("total.rows", count));
            builder.size(count);
            if (rFetchType == FetchType.STORE) {
                builder.uri(runContext.storage().putFile(tempFile));
            } else if (rFetchType == FetchType.FETCH) {
                builder.rows(rows);
            } else {
                builder.row(rows.isEmpty() ? Collections.emptyMap() : rows.getFirst());
            }
            return builder.build();
        } finally {
            killable.compareAndSet(stop, null);
        }
    }

    private void checkKilled() {
        if (isKilled.get()) {
            throw new CancellationException("MaxCompute query was cancelled");
        }
    }

    private void stopInstance() {
        var stop = killable.getAndSet(null);
        if (stop != null) {
            stop.run();
        }
    }

    @Override
    public void kill() {
        if (isKilled.compareAndSet(false, true)) {
            stopInstance();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The MaxCompute instance ID")
        private String instanceId;

        @Schema(title = "List containing fetched rows", description = "Only populated if fetchType is FETCH")
        private List<Map<String, Object>> rows;

        @Schema(title = "Map containing the first row", description = "Only populated if fetchType is FETCH_ONE")
        private Map<String, Object> row;

        @Schema(title = "The total number of rows returned")
        private Long size;

        @Schema(title = "The URI of the stored result in internal storage", description = "Only populated if fetchType is STORE")
        private URI uri;
    }
}
