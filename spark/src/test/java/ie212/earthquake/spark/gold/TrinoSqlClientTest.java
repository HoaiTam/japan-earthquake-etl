package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TrinoSqlClientTest {
    @Test void lostPostResponseIsUnknownAndNeverResubmitted() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0); var calls=new AtomicInteger();
        server.createContext("/v1/statement",exchange->{
            calls.incrementAndGet(); exchange.getRequestBody().readAllBytes();
            // The server accepted the POST, then vanished before a complete response.
            exchange.sendResponseHeaders(200,1024);
            exchange.getResponseBody().write("{".getBytes(StandardCharsets.UTF_8)); exchange.close();
        });server.start();
        try {
            var client=new TrinoSqlClient(URI.create("http://127.0.0.1:"+server.getAddress().getPort()));
            var error=assertThrows(Exception.class,()->client.execute("INSERT private_payload"));
            assertEquals("TRINO_QUERY_OUTCOME_UNKNOWN method=POST page=0 query_id=unknown retry=disabled reason=TRANSPORT query_budget_seconds=120",error.getMessage());
            assertNull(error.getCause()); assertEquals(1,calls.get());
        } finally {server.stop(0);}
    }
    @Test void lostPageRetainsSafeQueryIdWithoutRestartingPost() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0); var posts=new AtomicInteger(); var gets=new AtomicInteger();
        String endpoint="http://127.0.0.1:"+server.getAddress().getPort();
        server.createContext("/v1/statement",exchange->{
            exchange.getRequestBody().readAllBytes();
            if("POST".equals(exchange.getRequestMethod())) {
                posts.incrementAndGet(); byte[] bytes=("{\"id\":\"20261025_010203_00001_abc12\",\"nextUri\":\""+endpoint+"/v1/statement/q/1\"}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);
            } else {gets.incrementAndGet(); exchange.sendResponseHeaders(200,1024);exchange.getResponseBody().write(123);}
            exchange.close();
        });server.start();
        try {
            var error=assertThrows(Exception.class,()->new TrinoSqlClient(URI.create(endpoint)).execute("CREATE VIEW private_payload"));
            assertEquals("TRINO_QUERY_OUTCOME_UNKNOWN method=GET page=1 query_id=20261025_010203_00001_abc12 retry=disabled reason=TRANSPORT query_budget_seconds=120",error.getMessage());
            assertEquals(1,posts.get());assertEquals(1,gets.get());
        } finally {server.stop(0);}
    }
    @Test void malformedJsonCannotEchoPayloadInParserException() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/v1/statement",exchange->{byte[] bytes="{private_payload".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});server.start();
        try {var error=assertThrows(Exception.class,()->new TrinoSqlClient(URI.create("http://127.0.0.1:"+server.getAddress().getPort())).execute("SELECT private_payload"));
            assertEquals("TRINO_RESPONSE_INVALID",error.getMessage());assertNull(error.getCause());
        } finally {server.stop(0);}
    }
    @Test void statementPagesAndErrorPayloadRedaction() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var calls=new AtomicInteger();
        String endpoint="http://127.0.0.1:"+server.getAddress().getPort();
        server.createContext("/v1/statement",exchange->{
            assertEquals("PARAMETRIC_DATETIME",exchange.getRequestHeaders().getFirst("X-Trino-Client-Capabilities"));
            int call=calls.getAndIncrement();
            String data=call==0?"{\"nextUri\":\""+endpoint+"/v1/statement/q/1\"}":
                call==1?"{\"columns\":[{\"name\":\"n\",\"type\":\"bigint\"}],\"data\":[[16]]}":
                "{\"error\":{\"message\":\"private source payload\"}}";
            byte[] bytes=data.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);
            try(var out=exchange.getResponseBody()){out.write(bytes);}
        });server.start();
        try{
            var client=new TrinoSqlClient(URI.create(endpoint));var result=client.execute("SELECT count(*)");
            assertEquals(16,result.rows().get(0).get(0).asLong());assertEquals("bigint",result.columns().get(0).path("type").asText());
            var error=assertThrows(Exception.class,()->client.execute("SELECT 1"));
            assertEquals("TRINO_QUERY_FAILED",error.getMessage());
        }finally{server.stop(0);}
    }
    @Test void temporalPrecisionAndTimeZoneRemainVisible() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/v1/statement",exchange->{
            boolean capable="PARAMETRIC_DATETIME".equals(exchange.getRequestHeaders().getFirst("X-Trino-Client-Capabilities"));
            String utc=capable?"timestamp(6) with time zone":"timestamp with time zone";
            String jst=capable?"timestamp(6)":"timestamp";
            byte[] data=("{\"columns\":[{\"name\":\"utc\",\"type\":\""+utc+"\"},{\"name\":\"jst\",\"type\":\""+jst+"\"}]}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200,data.length);try(var out=exchange.getResponseBody()){out.write(data);}
        });server.start();
        try {var result=new TrinoSqlClient(URI.create("http://127.0.0.1:"+server.getAddress().getPort())).execute("SELECT temporal_fields LIMIT 0");
            assertEquals("timestamp(6) with time zone",result.columns().get(0).path("type").asText());
            assertEquals("timestamp(6)",result.columns().get(1).path("type").asText());
        }finally{server.stop(0);}
    }
    @Test void foreignNextUriIsNeverFollowed() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/v1/statement",exchange->{byte[] data="{\"nextUri\":\"http://example.invalid/v1/statement/q\"}".getBytes();
            exchange.sendResponseHeaders(200,data.length);try(var out=exchange.getResponseBody()){out.write(data);}});server.start();
        try{var client=new TrinoSqlClient(URI.create("http://127.0.0.1:"+server.getAddress().getPort()));
            assertEquals("TRINO_NEXT_URI_REJECTED",assertThrows(Exception.class,()->client.execute("SELECT 1")).getMessage());
        }finally{server.stop(0);}
        assertThrows(Exception.class,()->new TrinoSqlClient(URI.create("http://user:secret@localhost:8080")));
    }
    @Test void researchSelectCanKeepPollingAfterTwoMinutesWithoutResubmitting() throws Exception {
        deadlineTrial("SELECT count(*) FROM pinned_snapshot",true,180,true,300);
    }
    @Test void defaultSelectAndResearchWritesKeepTwoMinuteBudget() throws Exception {
        deadlineTrial("SELECT count(*) FROM pinned_snapshot",false,180,false,120);
        for(String sql:java.util.List.of("INSERT INTO publication_status SELECT 1","CREATE VIEW serving AS SELECT 1",
                "WITH input AS (SELECT 1) SELECT * FROM input","DELETE FROM publication_status"))
            deadlineTrial(sql,true,180,false,120);
    }
    @Test void researchSelectStillHasHardFiveMinuteBudget() throws Exception {
        deadlineTrial("SELECT count(*) FROM pinned_snapshot",true,301,false,300);
    }
    private void deadlineTrial(String sql,boolean research,int elapsedSeconds,boolean success,int budget) throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        var posts=new AtomicInteger();var gets=new AtomicInteger();var ticks=new AtomicInteger();
        String endpoint="http://127.0.0.1:"+server.getAddress().getPort();
        server.createContext("/v1/statement",exchange->{
            exchange.getRequestBody().readAllBytes();String data;
            if("POST".equals(exchange.getRequestMethod())) {posts.incrementAndGet();data="{\"id\":\"20261027_010203_00001_abc12\",\"nextUri\":\""+endpoint+"/v1/statement/q/1\"}";}
            else {gets.incrementAndGet();data="{\"data\":[[0]]}";}
            byte[] bytes=data.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);
            try(var out=exchange.getResponseBody()){out.write(bytes);}
        });server.start();
        try {
            var client=new TrinoSqlClient(URI.create(endpoint),research,()->ticks.getAndIncrement()<2?0:java.time.Duration.ofSeconds(elapsedSeconds).toNanos());
            if(success)assertEquals(0,client.execute(sql).rows().get(0).get(0).asInt());
            else {
                var error=assertThrows(Exception.class,()->client.execute(sql));
                assertTrue(error.getMessage().contains("reason=QUERY_DEADLINE query_budget_seconds="+budget));
                assertFalse(error.getMessage().contains(sql));assertNull(error.getCause());
            }
            assertEquals(1,posts.get());assertEquals(success?1:0,gets.get());
        } finally {server.stop(0);}
    }
}
