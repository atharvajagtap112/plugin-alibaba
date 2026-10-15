package io.kestra.plugin.alibaba;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.http.client.ClientException;
import com.aliyun.openservices.log.http.client.ServiceException;
import com.aliyun.openservices.log.http.comm.DefaultServiceClient;
import com.aliyun.openservices.log.http.comm.RequestMessage;
import com.aliyun.openservices.log.http.comm.ResponseMessage;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task implements AlibabaConnectionInterface {
    @Schema(title = "AccessKey ID", description = "The AccessKey ID used to authenticate against Alibaba Cloud.")
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> accessKeyId;

    @Schema(title = "AccessKey secret", description = "The AccessKey secret paired with the AccessKey ID.")
    @NotNull
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> accessKeySecret;

    @Schema(title = "Security token", description = "Optional Security Token Service (STS) token, required when using temporary credentials.")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> securityToken;

    @Schema(title = "Region", description = "The Alibaba Cloud region ID, for example `cn-hangzhou`. The service endpoint is derived from it unless `endpointOverride` is set.")
    @PluginProperty(group = "connection")
    protected Property<String> region;

    @Schema(title = "Endpoint override", description = "Custom http or https endpoint URL. When set, it takes precedence over the endpoint derived from `region`.")
    @PluginProperty(group = "connection")
    protected Property<String> endpointOverride;

    public static String required(RunContext runContext, Property<String> property, String name) throws IllegalVariableEvaluationException {
        return runContext.render(property).as(String.class).filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("`" + name + "` is required"));
    }

    protected String renderedAccessKeyId(RunContext runContext) throws IllegalVariableEvaluationException {
        return required(runContext, accessKeyId, "accessKeyId");
    }

    protected String renderedAccessKeySecret(RunContext runContext) throws IllegalVariableEvaluationException {
        return required(runContext, accessKeySecret, "accessKeySecret");
    }

    protected String renderedSecurityToken(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(securityToken).as(String.class).orElse(null);
    }

    protected String renderedRegion(RunContext runContext) throws IllegalVariableEvaluationException {
        return required(runContext, region, "region");
    }

    protected String renderedEndpoint(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(endpointOverride).as(String.class).orElse(null);
    }

    protected Client slsClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return slsClient(runContext, this);
    }

    public static Client slsClient(RunContext runContext, AlibabaConnectionInterface connection) throws IllegalVariableEvaluationException {
        var endpoint = runContext.render(connection.getEndpointOverride()).as(String.class).filter(value -> !value.isBlank())
            .orElseGet(() -> null);
        var custom = endpoint != null;
        if (!custom) {
            endpoint = "https://" + required(runContext, connection.getRegion(), "region") + ".log.aliyuncs.com";
        } else if (!endpoint.contains("://")) {
            endpoint = "https://" + endpoint;
        }
        var uri = URI.create(endpoint);
        if (uri.getHost() == null || !("https".equals(uri.getScheme()) || "http".equals(uri.getScheme()))
            || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null
            || !(uri.getPath().isEmpty() || uri.getPath().equals("/"))) {
            throw new IllegalArgumentException("`endpointOverride` must be an http or https service endpoint");
        }
        var configuration = new ClientConfiguration();
        var transport = new DefaultServiceClient(configuration) {
            @Override
            public ResponseMessage sendRequest(RequestMessage request, String charset) throws ServiceException, ClientException {
                if (custom) {
                    request.setEndpoint(uri);
                    request.getHeaders().put("Host", uri.getAuthority());
                }
                return super.sendRequest(request, charset);
            }
        };
        try {
            // The SDK validates hostnames without ports; custom endpoints are applied by its transport above.
            var client = new Client(uri.getScheme() + "://" + uri.getHost(),
                required(runContext, connection.getAccessKeyId(), "accessKeyId"),
                required(runContext, connection.getAccessKeySecret(), "accessKeySecret"), transport);
            var token = runContext.render(connection.getSecurityToken()).as(String.class).orElse(null);
            if (token != null && !token.isBlank()) {
                client.setSecurityToken(token);
            }
            return client;
        } catch (RuntimeException | IllegalVariableEvaluationException e) {
            transport.shutdown();
            throw e;
        }
    }
}
