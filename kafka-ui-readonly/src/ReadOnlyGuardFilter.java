/* SPDX-License-Identifier: Apache-2.0
 * Local modification for Kafka UI v0.7.2: enforce read-only at the HTTP boundary.
 */
package com.provectus.kafka.ui.config;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.PathContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(-50) // After Spring Security (-100), before controller dispatch.
public final class ReadOnlyGuardFilter implements WebFilter {
  private final boolean forceReadOnly;
  private final ClustersProperties properties;

  public ReadOnlyGuardFilter(@Value("${ui.read-only:false}") boolean forceReadOnly,
                            ClustersProperties properties) {
    this.forceReadOnly = forceReadOnly;
    this.properties = properties;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    List<String> segments = exchange.getRequest().getPath().pathWithinApplication().elements()
        .stream().filter(e -> e instanceof PathContainer.PathSegment)
        .map(e -> ((PathContainer.PathSegment) e).valueToMatch()).toList();
    boolean api = !segments.isEmpty() && "api".equals(segments.get(0));
    HttpMethod method = exchange.getRequest().getMethod();
    if (api && segments.size() == 2 && "readonly-policy".equals(segments.get(1))
        && HttpMethod.GET.equals(method)) {
      return respond(exchange, HttpStatus.OK, "{\"readOnly\":" + forceReadOnly + "}");
    }
    boolean safeMethod = HttpMethod.GET.equals(method) || HttpMethod.HEAD.equals(method)
        || HttpMethod.OPTIONS.equals(method);
    if (api && !safeMethod && (forceReadOnly || readOnlyCluster(segments))) {
      return respond(exchange, HttpStatus.FORBIDDEN,
          "{\"code\":\"UI_READ_ONLY\",\"message\":\"Management writes are disabled\"}");
    }
    return chain.filter(exchange);
  }

  private boolean readOnlyCluster(List<String> segments) {
    if (segments.size() < 3 || !"clusters".equals(segments.get(1))) {
      return false;
    }
    String name = segments.get(2);
    return properties.getClusters() != null && properties.getClusters().stream()
        .anyMatch(c -> name.equals(c.getName()) && c.isReadOnly());
  }

  private Mono<Void> respond(ServerWebExchange exchange, HttpStatus status, String body) {
    exchange.getResponse().setStatusCode(status);
    exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
    exchange.getResponse().getHeaders().setCacheControl("no-store");
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    return exchange.getResponse().writeWith(Mono.just(
        exchange.getResponse().bufferFactory().wrap(bytes)));
  }
}
