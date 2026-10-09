package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.*;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class ResultBundleContractTest {
    static final ObjectMapper JSON=new ObjectMapper();
    static final Path FIXTURES=Path.of("src/test/resources/ml/results");
    static final ResultBundleContract VALIDATOR=new ResultBundleContract();
    static final List<ResultBundleValidator.Candidate> CANDIDATES=List.of(
        new ResultBundleValidator.Candidate("main","main","MAINSHOCK"),
        new ResultBundleValidator.Candidate("main","post","POST"));
    static ResultBundleValidator.Context context(String algorithm,String previous) {
        return new ResultBundleValidator.Context("ds_fixture","EXPORTED","exp_fixture",algorithm,"1",CANDIDATES,previous);
    }
    static MessageType schema(String name) {
        StringBuilder text=new StringBuilder("message result { ");
        for (JsonNode field:ResultBundleContract.contract().path("schemas").path(name).path("fields")) {
            String repetition=field.get("nullable").asBoolean()?"optional":"required";
            String type=field.get("type").asText(), key=field.get("name").asText();
            if (type.equals("array<string>")) {
                text.append(repetition).append(" group ").append(key).append(" (LIST) { repeated group list { required binary element (UTF8); } } ");
                continue;
            }
            String physical=switch(type) {
                case "string","json_string"->"binary"; case "boolean"->"boolean";
                case "integer"->"int32"; case "long","timestamp_utc"->"int64"; case "double"->"double";
                default->throw new IllegalArgumentException(type);
            };
            text.append(repetition).append(' ').append(physical).append(' ').append(key);
            if (type.equals("string") || type.equals("json_string")) text.append(" (UTF8)");
            if (type.equals("timestamp_utc")) text.append(" (TIMESTAMP(MILLIS,true))");
            text.append("; ");
        }
        return MessageTypeParser.parseMessageType(text.append("}").toString());
    }
    static OutputFile output(Path path) {
        return new OutputFile() {
            public PositionOutputStream create(long hint)throws IOException { return stream(Files.newOutputStream(path,StandardOpenOption.CREATE_NEW)); }
            public PositionOutputStream createOrOverwrite(long hint)throws IOException { return stream(Files.newOutputStream(path)); }
            public boolean supportsBlockSize(){return false;} public long defaultBlockSize(){return 0;}
            private PositionOutputStream stream(OutputStream out) {
                return new PositionOutputStream(){ long position;
                    public long getPos(){return position;} public void write(int b)throws IOException{out.write(b);position++;}
                    public void write(byte[] b,int offset,int length)throws IOException{out.write(b,offset,length);position+=length;}
                    public void close()throws IOException{out.close();}
                };
            }
        };
    }
    static InputFile input(Path path) {
        return new InputFile() {
            public long getLength()throws IOException{return Files.size(path);}
            public SeekableInputStream newStream()throws IOException {
                RandomAccessFile file=new RandomAccessFile(path.toFile(),"r");
                return new DelegatingSeekableInputStream(new InputStream(){
                    public int read()throws IOException{return file.read();}
                    public int read(byte[] bytes,int offset,int length)throws IOException{return file.read(bytes,offset,length);}
                    public void close()throws IOException{file.close();}
                }) {
                    public long getPos()throws IOException{return file.getFilePointer();}
                    public void seek(long position)throws IOException{file.seek(position);}
                };
            }
        };
    }
    static void writeParquet(Path path,String artifact,List<JsonNode> rows)throws Exception {
        MessageType type=schema(artifact); SimpleGroupFactory factory=new SimpleGroupFactory(type);
        try(var writer=ExampleParquetWriter.builder(output(path)).withType(type).withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE).withConf(new Configuration(false)).build()) {
            for(JsonNode row:rows) {
                Group group=factory.newGroup();
                for(JsonNode field:ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
                    String key=field.get("name").asText(), kind=field.get("type").asText(); JsonNode value=row.get(key);
                    if(value==null || value.isNull())continue;
                    switch(kind) {
                        case "string","json_string"->group.add(key,value.asText());
                        case "timestamp_utc"->group.add(key,Instant.parse(value.asText()).toEpochMilli());
                        case "boolean"->group.add(key,value.asBoolean()); case "integer"->group.add(key,value.asInt());
                        case "long"->group.add(key,value.asLong()); case "double"->group.add(key,value.asDouble());
                        case "array<string>"->{Group list=group.addGroup(key); for(JsonNode item:value)list.addGroup("list").add("element",item.asText());}
                    }
                }
                writer.write(group);
            }
        }
    }
    static List<JsonNode> readParquet(Path path,String artifact)throws Exception {
        List<JsonNode> rows=new ArrayList<>();
        try(var reader=ParquetFileReader.open(input(path))) {
            MessageType type=reader.getFooter().getFileMetaData().getSchema();
            if(!type.equals(schema(artifact)))throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
            var pages=reader.readNextRowGroup();
            while(pages!=null) {
                var records=new ColumnIOFactory().getColumnIO(type).getRecordReader(pages,new GroupRecordConverter(type));
                for(long i=0;i<pages.getRowCount();i++) {
                    Group group=records.read(); ObjectNode row=JSON.createObjectNode();
                    for(JsonNode field:ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
                        String key=field.get("name").asText(),kind=field.get("type").asText();
                        if(group.getFieldRepetitionCount(key)==0){row.putNull(key);continue;}
                        switch(kind) {
                            case "string","json_string"->row.put(key,group.getBinary(key,0).toStringUsingUTF8());
                            case "timestamp_utc"->row.put(key,Instant.ofEpochMilli(group.getLong(key,0)).toString());
                            case "boolean"->row.put(key,group.getBoolean(key,0));case "integer"->row.put(key,group.getInteger(key,0));
                            case "long"->row.put(key,group.getLong(key,0));case "double"->row.put(key,group.getDouble(key,0));
                            case "array<string>"->{var array=row.putArray(key);Group list=group.getGroup(key,0);
                                for(int j=0;j<list.getFieldRepetitionCount("list");j++)array.add(list.getGroup("list",j).getBinary("element",0).toStringUsingUTF8());}
                        }
                    }
                    rows.add(row);
                }
                pages=reader.readNextRowGroup();
            }
        }
        return rows;
    }
    static ResultBundleValidator.DecodedBundle load(String name)throws Exception {
        Path dir=FIXTURES.resolve(name); Map<String,byte[]> artifacts=new HashMap<>();
        for(JsonNode file:ResultBundleContract.contract().get("required_artifacts")) artifacts.put(file.asText(),Files.readAllBytes(dir.resolve(file.asText())));
        return new ResultBundleValidator.DecodedBundle(JSON.readTree(dir.resolve("_SUCCESS.json").toFile()),artifacts,
            readParquet(dir.resolve("memberships.parquet"),"memberships.parquet"),readParquet(dir.resolve("sequence_summary.parquet"),"sequence_summary.parquet"),true);
    }
    static ObjectNode base(String artifact) {
        ObjectNode row=JSON.createObjectNode();
        for(JsonNode field:ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
            String name=field.get("name").asText();
            if(field.get("nullable").asBoolean())row.putNull(name);
            else switch(field.get("type").asText()) {
                case "string","json_string"->row.put(name,"fixture"); case "timestamp_utc"->row.put(name,"2024-01-01T00:00:00Z");
                case "boolean"->row.put(name,false); case "long","integer"->row.put(name,0L); case "double"->row.put(name,0.0);
            }
        }
        row.put("schema_version","1.0").put("experiment_run_id","exp_fixture").put("dataset_id","ds_fixture")
            .put("mainshock_event_id","main").put("algorithm_name","WINDOW").put("model_config_version","1");
        return row;
    }
    static void fixture(String name,String invalid)throws Exception {
        Path dir=FIXTURES.resolve(name); Files.createDirectories(dir);
        ObjectNode main=base("memberships.parquet");
        main.put("candidate_event_id","main").put("event_role_candidate","MAINSHOCK").put("is_sequence_member",true)
            .put("multi_sequence_status","UNIQUE").put("resolution_rank",1);
        ObjectNode post=main.deepCopy();post.put("candidate_event_id","post").put("event_role_candidate","POST");
        List<JsonNode> members=new ArrayList<>(List.of(main,post));
        if(invalid.equals("duplicate"))members.add(post.deepCopy());
        if(invalid.equals("unknown"))post.put("candidate_event_id","unknown");
        if(invalid.equals("schema"))post.put("schema_version","2.0");
        ObjectNode summary=base("sequence_summary.parquet");
        summary.put("window_result_status","SUCCESS").put("productive_sequence_detected",true)
            .put("candidate_count",2L).put("sequence_member_count",2L).put("aftershock_count",1L).put("foreshock_candidate_count",0L);
        summary.putArray("metric_reason_codes").add("METRIC_NOT_APPLICABLE").add("METRIC_RUNTIME_UNAVAILABLE");
        boolean hdb=invalid.startsWith("hdb");
        if(hdb) {
            for(JsonNode item:members) {
                ObjectNode row=(ObjectNode)item;row.put("algorithm_name","HDBSCAN_GLOBAL").put("cluster_id",0L).put("is_noise",false)
                    .put("is_mainshock_cluster_member",true).put("membership_probability",0.9);
            }
            summary.put("algorithm_name","HDBSCAN_GLOBAL").put("cluster_count",1).put("noise_count",0L).put("noise_ratio",0.0).put("mean_membership_probability",0.9);
        }
        if(invalid.equals("hdb-noise")) {
            main.put("cluster_id",-1L).put("is_noise",true).put("membership_probability",0.0);
            for(JsonNode item:members)((ObjectNode)item).put("is_sequence_member",false).put("is_mainshock_cluster_member",false).put("multi_sequence_status","NOT_MEMBER").putNull("resolution_rank");
            summary.put("window_result_status","FAILED").put("failure_reason_code","EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
                .putNull("productive_sequence_detected").putNull("sequence_member_count").putNull("aftershock_count").putNull("foreshock_candidate_count").putNull("mean_membership_probability");
        }
        writeParquet(dir.resolve("memberships.parquet"),"memberships.parquet",members);
        writeParquet(dir.resolve("sequence_summary.parquet"),"sequence_summary.parquet",List.of(summary));
        ObjectNode metadata=JSON.createObjectNode().put("dataset_id","ds_fixture").put("experiment_run_id","exp_fixture")
            .put("algorithm_name",hdb?"HDBSCAN_GLOBAL":"WINDOW").put("model_config_version","1")
            .put("algorithm_version","fixture-1").put("package_version","fixture-1").put("code_version","fixture-1");
        Files.writeString(dir.resolve("experiment_config.json"),metadata.toPrettyString()+"\n");
        Files.writeString(dir.resolve("experiment_metrics.json"),metadata.deepCopy().put("membership_row_count",members.size()).put("summary_row_count",1L).toPrettyString()+"\n");
        Files.writeString(dir.resolve("requirements-lock.txt"),"# synthetic fixture; no experiment executed\n");
        ObjectNode success=metadata.deepCopy().put("schema_version","1.0").put("algorithm_version","fixture-1").put("package_version","fixture-1")
            .put("membership_row_count",members.size()).put("summary_row_count",1L).put("status","COMPLETED")
            .put("completed_at_utc","2024-01-01T00:00:00Z").put("artifact_uri","s3://fixture/results/exp_fixture/");
        ObjectNode checks=success.putObject("checksums");
        for(JsonNode file:ResultBundleContract.contract().get("required_artifacts"))checks.put(file.asText(),ResultBundleContract.sha256(Files.readAllBytes(dir.resolve(file.asText()))));
        if(invalid.equals("checksum"))checks.put("memberships.parquet","0".repeat(64));
        Files.writeString(dir.resolve("_SUCCESS.json"),success.toPrettyString()+"\n");
    }
    @BeforeAll static void generateWhenRequested()throws Exception {
        if(Boolean.getBoolean("generateMlFixtures"))for(String name:List.of("success","checksum","schema","duplicate","unknown","hdb-success","hdb-noise"))fixture(name,name);
    }
    @Test void validParquetFixtureIsOnlyResultReadyAndRerunIdempotent()throws Exception {
        var bundle=load("success");var receipt=VALIDATOR.validate(bundle,context("WINDOW",null));
        assertEquals("RESULT_READY",receipt.nextStatus());
        assertEquals(receipt,VALIDATOR.validate(bundle,context("WINDOW",receipt.bundleSha256())));
    }
    @Test void persistedInvalidFixturesReject()throws Exception {
        for(var item:Map.of("checksum","IMP_CHECKSUM_MISMATCH","schema","IMP_SCHEMA_MISMATCH","duplicate","IMP_DUPLICATE_GRAIN","unknown","IMP_UNKNOWN_EVENT_ID").entrySet()) {
            var error=assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(load(item.getKey()),context("WINDOW",null)));
            assertEquals(item.getValue(),error.getMessage());
        }
    }
    @Test void runReuseAndUnexportedDatasetRejected()throws Exception {
        var bundle=load("success");
        assertEquals("IMP_EXPERIMENT_RUN_REUSED",assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(bundle,context("WINDOW","other"))).getMessage());
        assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(bundle,new ResultBundleValidator.Context("ds_fixture","BUILDING","exp_fixture","WINDOW","1",CANDIDATES,null)));
    }
    @Test void probabilityNoiseRoleCountsAndSchemaReject()throws Exception {
        for(String field:List.of("membership_probability","cluster_id","event_role_candidate","multi_sequence_status","normalized_distance")) {
            var source=load("success"); List<JsonNode> rows=new ArrayList<>(source.memberships());ObjectNode row=rows.get(1).deepCopy();
            switch(field) {
                case "membership_probability"->row.put(field,1.2);case "cluster_id"->row.put(field,-1L);
                case "event_role_candidate"->row.put(field,"PRE");case "multi_sequence_status"->row.put(field,"INVALID");
                case "normalized_distance"->row.put(field,Double.NaN);
            }
            rows.set(1,row);
            assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(new ResultBundleValidator.DecodedBundle(source.success(),source.artifacts(),rows,source.summaries(),true),context("WINDOW",null)));
        }
        var source=load("success");ObjectNode success=source.success().deepCopy();success.put("summary_row_count",0);
        assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(new ResultBundleValidator.DecodedBundle(success,source.artifacts(),source.memberships(),source.summaries(),true),context("WINDOW",null)));
        assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(new ResultBundleValidator.DecodedBundle(source.success(),source.artifacts(),source.memberships(),source.summaries(),false),context("WINDOW",null)));
    }
    @Test void lifecycleRequiresVerifiedCommitAndScientificReview() {
        ResultBundleContract.assertTransition("TRAINING_EXTERNAL","RESULT_READY",false,null,null);
        ResultBundleContract.assertTransition("IMPORT_VALIDATING","CANDIDATE",false,1L,2L);
        assertThrows(IllegalArgumentException.class,()->ResultBundleContract.assertTransition("IMPORT_VALIDATING","CANDIDATE",false,null,null));
        assertThrows(IllegalArgumentException.class,()->ResultBundleContract.assertTransition("RESULT_READY","APPROVED",true,1L,2L));
        assertThrows(IllegalArgumentException.class,()->ResultBundleContract.assertTransition("CANDIDATE","APPROVED",false,1L,2L));
        ResultBundleContract.assertTransition("CANDIDATE","APPROVED",true,1L,2L);
        assertThrows(IllegalArgumentException.class,()->ResultBundleContract.assertTransition("REJECTED","RESULT_READY",true,1L,2L));
    }
    @Test void unsafeUriAndMetadataRejected()throws Exception {
        var source=load("success");ObjectNode success=source.success().deepCopy();success.put("artifact_uri","/home/person/results");
        assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(new ResultBundleValidator.DecodedBundle(success,source.artifacts(),source.memberships(),source.summaries(),true),context("WINDOW",null)));
    }
    @Test void hdbscanProbabilityAndMainshockNoiseFixtures()throws Exception {
        for(String name:List.of("hdb-success","hdb-noise")) assertEquals("RESULT_READY",VALIDATOR.validate(load(name),context("HDBSCAN_GLOBAL",null)).nextStatus());
        var source=load("hdb-success");
        for(Double probability:Arrays.asList(-0.1,1.1,Double.NaN,null)) {
            List<JsonNode> rows=new ArrayList<>(source.memberships());ObjectNode row=rows.get(1).deepCopy();
            if(probability==null)row.putNull("membership_probability");else row.put("membership_probability",probability);rows.set(1,row);
            assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(new ResultBundleValidator.DecodedBundle(source.success(),source.artifacts(),rows,source.summaries(),true),context("HDBSCAN_GLOBAL",null)));
        }
        var noise=load("hdb-noise");List<JsonNode> rows=new ArrayList<>(noise.memberships());ObjectNode post=rows.get(1).deepCopy();
        post.put("is_mainshock_cluster_member",true);rows.set(1,post);
        assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(new ResultBundleValidator.DecodedBundle(noise.success(),noise.artifacts(),rows,noise.summaries(),true),context("HDBSCAN_GLOBAL",null)));
    }
}
