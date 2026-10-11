package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

/** Bounded Trino statement protocol. Query errors never echo SQL or server payload. */
public final class TrinoSqlClient implements GoldPublisher.Sql {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final URI server;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    public TrinoSqlClient(URI server) {
        if (!Set.of("http", "https").contains(server.getScheme()) || server.getHost() == null
                || server.getUserInfo() != null || server.getQuery() != null || server.getFragment() != null
                || !(server.getPath().isEmpty() || server.getPath().equals("/")))
            throw new IllegalArgumentException("SAFE_TRINO_ENDPOINT_REQUIRED");
        this.server = server;
    }
    @Override public GoldPublisher.Result execute(String sql) throws Exception {
        URI next = server.resolve("/v1/statement");
        var request = HttpRequest.newBuilder(next).timeout(Duration.ofSeconds(90))
                .header("X-Trino-User", "etl-gold-verifier").header("X-Trino-Time-Zone", "UTC")
                .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
                .POST(HttpRequest.BodyPublishers.ofString(sql)).build();
        List<JsonNode> rows = new ArrayList<>(), columns = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        for (int page = 0; page < 1000; page++) {
            var response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] bytes;
            try (var body = response.body()) { bytes = body.readNBytes(1_048_577); }
            if (response.statusCode() != 200 || bytes.length > 1_048_576) throw new IOException("TRINO_RESPONSE_INVALID");
            var reply = JSON.readTree(bytes);
            if (reply.has("error")) throw new IOException("TRINO_QUERY_FAILED");
            if (reply.has("columns")) { columns.clear(); reply.path("columns").forEach(columns::add); }
            reply.path("data").forEach(rows::add);
            if (rows.size() > 512) throw new IOException("TRINO_RESULT_LIMIT");
            if (!reply.has("nextUri")) return new GoldPublisher.Result(List.copyOf(columns), List.copyOf(rows));
            next = URI.create(reply.path("nextUri").asText());
            if (!Objects.equals(server.getHost(), next.getHost()) || server.getPort() != next.getPort()
                    || !server.getScheme().equals(next.getScheme()) || next.getUserInfo() != null
                    || !next.getPath().startsWith("/v1/statement/")) throw new IOException("TRINO_NEXT_URI_REJECTED");
            if (System.nanoTime() > deadline) throw new IOException("TRINO_QUERY_OUTCOME_UNKNOWN");
            request = HttpRequest.newBuilder(next).timeout(Duration.ofSeconds(90)).header("X-Trino-User", "etl-gold-verifier")
                    .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
                    .header("X-Trino-Time-Zone", "UTC").GET().build();
        }
        throw new IOException("TRINO_QUERY_OUTCOME_UNKNOWN");
    }
}
