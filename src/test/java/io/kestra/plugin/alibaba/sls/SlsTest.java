package io.kestra.plugin.alibaba.sls;

import com.aliyun.openservices.log.common.Logs;
import com.aliyun.openservices.log.util.LZ4Encoder;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class SlsTest {
    @Inject
    private RunContextFactory runContextFactory;
    private static WireMockServer server;

    @BeforeAll
    static void start() {
        server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.start();
    }

    @AfterAll
    static void stop() {
        server.stop();
    }

    @BeforeEach
    void reset() {
        server.resetAll();
    }

    private String endpoint() {
        return "http://localhost:" + server.port();
    }

    private Query.QueryBuilder<?, ?> query(FetchType mode) {
        return Query.builder().id("query").type(Query.class.getName())
            .accessKeyId(Property.ofValue("ak")).accessKeySecret(Property.ofValue("sk"))
            .endpointOverride(Property.ofValue(endpoint())).project(Property.ofValue("test-project"))
            .logstore(Property.ofValue("test-logstore")).fetchType(Property.ofValue(mode));
    }

    private Push.PushBuilder<?, ?> push(Object from) {
        return Push.builder().id("push").type(Push.class.getName())
            .accessKeyId(Property.ofValue("ak")).accessKeySecret(Property.ofValue("sk"))
            .endpointOverride(Property.ofValue(endpoint())).project(Property.ofValue("test-project"))
            .logstore(Property.ofValue("test-logstore")).from(from);
    }

    private void stubQuery(String progress) {
        server.stubFor(post(urlPathEqualTo("/logstores/test-logstore/logs"))
            .willReturn(aResponse().withHeader("Content-Type", "application/json")
                .withHeader("x-log-count", "2").withHeader("x-log-progress", progress)
                .withBody("""
                    {"meta":{"progress":"%s"},"data":[
                    {"__time__":"1609459200","__source__":"host","message":"First error"},
                    {"__time__":"1609459201","__source__":"host","message":"Second error"}]}
                    """.formatted(progress))));
    }

    @Test
    void queryAllFetchModesAgainstRealEndpoint() throws Exception {
        stubQuery("Complete");
        var rc = runContextFactory.of(Map.of());
        assertEquals("First error", query(FetchType.FETCH).build().run(rc).getLogs().getFirst().get("message"));
        assertEquals("First error", query(FetchType.FETCH_ONE).build().run(rc).getLog().get("message"));
        assertEquals(2L, query(FetchType.NONE).build().run(rc).getCount());
        var stored = query(FetchType.STORE).build().run(rc);
        var rows = new ArrayList<Object>();
        try (var input = rc.storage().getFile(stored.getUri())) {
            FileSerde.read(input, rows::add);
        }
        assertEquals(2, rows.size());
        server.verify(4, postRequestedFor(urlPathEqualTo("/logstores/test-logstore/logs"))
            .withHeader("Host", equalTo("localhost:" + server.port())));
    }

    @Test
    void invalidBoundsTimesAndIncompleteQueriesFail() {
        var rc = runContextFactory.of(Map.of());
        assertThrows(IllegalArgumentException.class, () -> query(FetchType.FETCH).lines(Property.ofValue(101)).build().run(rc));
        assertThrows(IllegalArgumentException.class, () -> query(FetchType.FETCH).lines(Property.ofValue(0)).build().run(rc));
        assertThrows(IllegalArgumentException.class, () -> query(FetchType.FETCH).offset(Property.ofValue(-1)).build().run(rc));
        assertThrows(IllegalArgumentException.class, () -> query(FetchType.FETCH).lines(Property.ofExpression("{{ 101 }}")).build().run(rc));
        assertThrows(IllegalArgumentException.class, () -> query(FetchType.FETCH).offset(Property.ofExpression("{{ -1 }}")).build().run(rc));
        assertEquals(0, server.getAllServeEvents().size());
        assertThrows(IllegalArgumentException.class, () -> query(FetchType.FETCH).from(Property.ofValue("invalid")).build().run(rc));
        stubQuery("Incomplete");
        assertThrows(IllegalStateException.class, () -> query(FetchType.FETCH).build().run(rc));
    }

    private void stubPush() {
        server.stubFor(post(urlPathEqualTo("/logstores/test-logstore/shards/lb"))
            .willReturn(aResponse().withHeader("x-log-requestid", "req-123")));
    }

    @Test
    void pushInlineAndStoredIonInBoundedBatches() throws Exception {
        stubPush();
        var rc = runContextFactory.of(Map.of());
        var records = IntStream.range(0, 4097).mapToObj(i -> Map.<String, Object>of("message", "item " + i)).toList();
        assertEquals(4097L, push(records).build().run(rc).getCount());
        server.verify(2, postRequestedFor(urlPathEqualTo("/logstores/test-logstore/shards/lb")));
        var file = rc.workingDir().createTempFile(".ion").toFile();
        try (var output = new FileOutputStream(file)) {
            FileSerde.write(output, Map.of("message", "from storage"));
        }
        var uri = rc.storage().putFile(file);
        assertEquals(1L, push(uri.toString()).build().run(rc).getCount());
    }

    @Test
    void pushRejectsInvalidTimestampAndPreservesUnsignedEpoch() throws Exception {
        stubPush();
        var rc = runContextFactory.of(Map.of());
        var error = assertThrows(IllegalArgumentException.class,
            () -> push(List.of(Map.of("__time__", "bad"))).build().run(rc));
        assertTrue(error.getMessage().contains("Record 1"));
        assertThrows(IllegalArgumentException.class, () -> push(List.of(Map.of("__time__", "4294967296"))).build().run(rc));
        assertEquals(1L, push(List.of(Map.of("__time__", "2200000000", "message", "future"))).build().run(rc).getCount());
    }

    private Trigger.TriggerBuilder<?, ?> trigger(FetchType mode, String id) {
        return Trigger.builder().id(id).type(Trigger.class.getName())
            .accessKeyId(Property.ofValue("ak")).accessKeySecret(Property.ofValue("sk"))
            .endpointOverride(Property.ofValue(endpoint())).project(Property.ofValue("test-project"))
            .logstore(Property.ofValue("test-logstore")).query(Property.ofValue("* | where level == 'ERROR'"))
            .fetchType(Property.ofValue(mode));
    }

    private void stubShards() {
        server.stubFor(get(urlPathEqualTo("/logstores/test-logstore/shards"))
            .willReturn(aResponse().withBody("[{\"shardID\":0,\"status\":\"readwrite\",\"inclusiveBeginKey\":\"0\",\"exclusiveEndKey\":\"f\",\"createTime\":1}]")));
        server.stubFor(get(urlPathEqualTo("/logstores/test-logstore/shards/0"))
            .withQueryParam("type", equalTo("cursor")).willReturn(aResponse().withBody("{\"cursor\":\"start\"}")));
        server.stubFor(get(urlPathEqualTo("/logstores/test-logstore/shards/0"))
            .withQueryParam("type", equalTo("cursor")).withQueryParam("from", equalTo("end"))
            .willReturn(aResponse().withBody("{\"cursor\":\"end\"}")));
    }

    private void stubPage(String cursor, String next, int logs) throws Exception {
        var group = Logs.LogGroup.newBuilder().setSource("host").setTopic("errors");
        for (var i = 0; i < logs; i++) {
            group.addLogs(Logs.Log.newBuilder().setTime(1)
                .addContents(Logs.Log.Content.newBuilder().setKey("message").setValue("late log " + i)));
        }
        var raw = Logs.LogGroupList.newBuilder().addLogGroupList(group).build().toByteArray();
        server.stubFor(get(urlPathEqualTo("/logstores/test-logstore/shards/0"))
            .withQueryParam("type", equalTo("log")).withQueryParam("cursor", equalTo(cursor))
            .withQueryParam("end_cursor", equalTo("end")).withQueryParam("query", equalTo("* | where level == 'ERROR'"))
            .willReturn(aResponse().withHeader("x-log-bodyrawsize", String.valueOf(raw.length))
                .withHeader("x-log-count", "1").withHeader("x-log-cursor", next)
                .withHeader("x-log-compresstype", "lz4").withBody(LZ4Encoder.compressToLhLz4Chunk(raw))));
    }

    @Test
    void realTriggerConsumesAllPagesAndPersistsShardCursorsForFetchAndStore() throws Exception {
        stubShards();
        stubPage("start", "middle", 101);
        stubPage("middle", "end", 2);
        for (var mode : List.of(FetchType.FETCH, FetchType.STORE)) {
            var id = "trigger-" + UUID.randomUUID();
            var task = trigger(mode, id).build();
            var ctx = TriggerContext.builder().tenantId("main").namespace("company.team").flowId("sls_flow").triggerId(id).build();
            var rc = runContextFactory.of(Map.of("flow", Map.of("namespace", ctx.getNamespace(), "id", ctx.getFlowId(), "tenantId", "main")));
            runContextFactory.initializer().forScheduler((DefaultRunContext) rc, ctx, task);
            var cond = ConditionContext.builder().runContext(rc)
                .flow(Flow.builder().id(ctx.getFlowId()).namespace(ctx.getNamespace()).revision(1).build()).build();
            var first = task.evaluate(cond, ctx).orElseThrow();
            assertEquals(103L, ((Number) first.getTrigger().getVariables().get("count")).longValue());
            var state = rc.namespaceKv(ctx.getNamespace()).getValue(Trigger.cursorKey(ctx.getFlowId(), id, "test-project", "test-logstore")).orElseThrow();
            assertEquals("end", JacksonMapper.ofJson().readValue((byte[]) state.value(), Map.class).get("0"));
            assertFalse(task.evaluate(cond, ctx).isPresent());
        }
        server.verify(4, getRequestedFor(urlPathEqualTo("/logstores/test-logstore/shards/0")).withQueryParam("type", equalTo("log")));
    }

    @Test
    void failedPageDoesNotCommitCursor() throws Exception {
        stubShards();
        stubPage("start", "start", 1);
        var id = "failed-" + UUID.randomUUID();
        var task = trigger(FetchType.FETCH, id).build();
        var ctx = TriggerContext.builder().tenantId("main").namespace("company.team").flowId("sls_flow").triggerId(id).build();
        var rc = runContextFactory.of(Map.of("flow", Map.of("namespace", ctx.getNamespace(), "id", ctx.getFlowId(), "tenantId", "main")));
        runContextFactory.initializer().forScheduler((DefaultRunContext) rc, ctx, task);
        var cond = ConditionContext.builder().runContext(rc).flow(Flow.builder().id(ctx.getFlowId()).namespace(ctx.getNamespace()).revision(1).build()).build();
        assertThrows(IllegalStateException.class, () -> task.evaluate(cond, ctx));
        assertFalse(rc.namespaceKv(ctx.getNamespace()).getValue(Trigger.cursorKey(ctx.getFlowId(), id, "test-project", "test-logstore")).isPresent());
    }

    @Test
    void corruptStateFailsBeforeCallingService() throws Exception {
        var id = "corrupt-" + UUID.randomUUID();
        var task = trigger(FetchType.FETCH, id).build();
        var ctx = TriggerContext.builder().tenantId("main").namespace("company.team").flowId("state_test").triggerId(id).build();
        var rc = runContextFactory.of(Map.of("flow", Map.of("namespace", ctx.getNamespace(), "id", ctx.getFlowId(), "tenantId", "main")));
        runContextFactory.initializer().forScheduler((DefaultRunContext) rc, ctx, task);
        var cond = ConditionContext.builder().runContext(rc)
            .flow(Flow.builder().id(ctx.getFlowId()).namespace(ctx.getNamespace()).revision(1).build()).build();
        for (var state : List.of("invalid JSON", "null", "{\"0\":null}", "{\"0\":\" \"}", "{\"\":\"end\"}")) {
            rc.namespaceKv(ctx.getNamespace()).put(Trigger.cursorKey(ctx.getFlowId(), id, "test-project", "test-logstore"),
                new KVValueAndMetadata(new KVMetadata("corrupted", (Duration) null), state.getBytes(StandardCharsets.UTF_8)));
            assertThrows(Exception.class, () -> task.evaluate(cond, ctx), state);
        }
        assertEquals(0, server.getAllServeEvents().size());
    }

    @Test
    void cursorKeysCannotCollideOnUnderscores() {
        assertNotEquals(Trigger.cursorKey("a_b", "c", "p", "l"), Trigger.cursorKey("a", "b_c", "p", "l"));
    }
}
