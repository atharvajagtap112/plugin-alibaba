package io.kestra.plugin.alibaba.mns.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class Message implements io.kestra.core.models.tasks.Output {
    @Schema(
        title = "The message data",
        description = "A string or a structured value (map, list) serialized to JSON before sending. Inline messages are rendered once by Kestra, rows read from a `kestra://` file are sent as is."
    )
    @PluginProperty(group = "main")
    @NotNull
    private Object data;

    @Schema(
        title = "Delay in seconds",
        description = "How long the message stays invisible after it is sent, from 0 to 604800 seconds."
    )
    @PluginProperty(group = "advanced")
    private Integer delaySeconds;

    @Schema(
        title = "Priority",
        description = "From 1 (highest) to 16 (lowest), defaults to 8."
    )
    @PluginProperty(group = "advanced")
    private Integer priority;

    public com.aliyun.mns.model.Message toMns() throws JsonProcessingException {
        var message = new com.aliyun.mns.model.Message();
        message.setMessageBodyAsRawString(body());
        if (delaySeconds != null) {
            message.setDelaySeconds(delaySeconds);
        }
        if (priority != null) {
            message.setPriority(priority);
        }
        return message;
    }

    private String body() throws JsonProcessingException {
        if (data == null) {
            throw new IllegalArgumentException("Every message needs a `data` value: check that each row of the `from` file has a `data` field");
        }
        if (data instanceof String s) {
            return s;
        }
        return JacksonMapper.ofJson(false).writeValueAsString(data);
    }
}
