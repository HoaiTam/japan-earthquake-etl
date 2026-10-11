package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TrinoSqlClientTest {
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
}
