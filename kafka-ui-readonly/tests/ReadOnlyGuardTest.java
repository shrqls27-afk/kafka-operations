/* SPDX-License-Identifier: Apache-2.0 */
import com.provectus.kafka.ui.config.ClustersProperties;
import com.provectus.kafka.ui.config.ReadOnlyGuardFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

public class ReadOnlyGuardTest {
  private static int passed;
  private static void check(ReadOnlyGuardFilter guard, String method, String path,
                            boolean forbidden, String context) {
    var builder = MockServerHttpRequest.method(HttpMethod.valueOf(method), java.net.URI.create(path));
    if (!context.isEmpty()) builder.contextPath(context);
    var exchange = MockServerWebExchange.from(builder);
    AtomicBoolean reached = new AtomicBoolean();
    guard.filter(exchange, e -> {
      reached.set(true);
      e.getResponse().setStatusCode(HttpStatus.ACCEPTED);
      return e.getResponse().setComplete();
    }).block(Duration.ofSeconds(5));
    if (forbidden) {
      if (reached.get() || exchange.getResponse().getStatusCode() != HttpStatus.FORBIDDEN)
        throw new AssertionError(method + " " + path + " escaped guard");
      if (!exchange.getResponse().getBodyAsString().block().contains("UI_READ_ONLY"))
        throw new AssertionError("Missing explicit error code");
    } else if (!reached.get()) {
      throw new AssertionError(method + " " + path + " unexpectedly blocked");
    }
    passed++;
  }
  public static void main(String[] args) throws Exception {
    ClustersProperties properties = new ClustersProperties();
    ClustersProperties.Cluster ro = new ClustersProperties.Cluster();
    ro.setName("locked"); ro.setReadOnly(true);
    ClustersProperties.Cluster rw = new ClustersProperties.Cluster();
    rw.setName("writable"); rw.setReadOnly(false);
    properties.setClusters(List.of(ro, rw));
    ReadOnlyGuardFilter global = new ReadOnlyGuardFilter(true, properties);
    ReadOnlyGuardFilter scoped = new ReadOnlyGuardFilter(false, properties);
    for (String line : Files.readAllLines(Path.of(args[0]))) {
      String[] parts = line.split(" ", 2);
      String method = parts[0];
      String path = parts[1].replace("{clusterName}", "locked")
          .replaceAll("\\{[^}]+\\}", "sample");
      boolean write = !List.of("GET", "HEAD", "OPTIONS").contains(method);
      check(global, method, path, write, "");
      check(scoped, method, path, write && path.startsWith("/api/clusters/locked/"), "");
      check(scoped, method, path.replace("/locked/", "/writable/"), false, "");
    }
    for (String method : List.of("POST", "PUT", "PATCH", "DELETE", "TRACE")) {
      check(global, method, "/ui/api/clusters/locked/topics", true, "/ui");
      check(scoped, method, "/api/clusters/%6cocked/topics", true, "");
      check(global, method, "/%61pi/clusters/locked/topics", true, "");
      check(global, method, "/api;param=value/config", true, "");
    }
    for (String method : List.of("GET", "HEAD", "OPTIONS")) {
      check(global, method, "/api/clusters/locked/topics", false, "");
    }
    check(global, "POST", "/auth", false, "");
    check(global, "GET", "/assets/example.js", false, "");
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/readonly-policy"));
    global.filter(exchange, e -> {throw new AssertionError("Policy not handled");}).block();
    if (!exchange.getResponse().getBodyAsString().block().equals("{\"readOnly\":true}"))
      throw new AssertionError("Wrong policy");
    passed++;
    System.out.println("READONLY_GUARD_CHECKS_PASSED=" + passed);
  }
}
