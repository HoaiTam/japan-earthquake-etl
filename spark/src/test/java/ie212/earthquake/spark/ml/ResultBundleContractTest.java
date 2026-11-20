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
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ResultBundleContractTest {
    static final ObjectMapper JSON=new ObjectMapper();
    static final Path FIXTURES=Path.of("src/test/resources/ml/results");
    static final ResultBundleContract VALIDATOR=new ResultBundleContract();
    static String fixtureJson(JsonNode node) { return node.toPrettyString().replace("\r\n","\n")+"\n"; }
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
            if(!type.equals(schema(artifact)) && !type.getFields().equals(schema(artifact).getFields()))throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
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
        Files.writeString(dir.resolve("experiment_config.json"),fixtureJson(metadata));
        Files.writeString(dir.resolve("experiment_metrics.json"),fixtureJson(metadata.deepCopy().put("membership_row_count",members.size()).put("summary_row_count",1L)));
        Files.writeString(dir.resolve("requirements-lock.txt"),"# synthetic fixture; no experiment executed\n");
        ObjectNode success=metadata.deepCopy().put("schema_version","1.0").put("algorithm_version","fixture-1").put("package_version","fixture-1")
            .put("membership_row_count",members.size()).put("summary_row_count",1L).put("status","COMPLETED")
            .put("completed_at_utc","2024-01-01T00:00:00Z").put("artifact_uri","s3://fixture/results/exp_fixture/");
        ObjectNode checks=success.putObject("checksums");
        for(JsonNode file:ResultBundleContract.contract().get("required_artifacts"))checks.put(file.asText(),ResultBundleContract.sha256(Files.readAllBytes(dir.resolve(file.asText()))));
        if(invalid.equals("checksum"))checks.put("memberships.parquet","0".repeat(64));
        Files.writeString(dir.resolve("_SUCCESS.json"),fixtureJson(success));
    }
    @BeforeAll static void generateWhenRequested()throws Exception {
        if(Boolean.getBoolean("generateMlFixtures"))for(String name:List.of("success","checksum","schema","duplicate","unknown","hdb-success","hdb-noise"))fixture(name,name);
    }
    @Test void validParquetFixtureIsOnlyResultReadyAndRerunIdempotent()throws Exception {
        var bundle=load("success");var receipt=VALIDATOR.validate(bundle,context("WINDOW",null));
        assertEquals("RESULT_READY",receipt.nextStatus());
        assertEquals(receipt,VALIDATOR.validate(bundle,context("WINDOW",receipt.bundleSha256())));
    }
    @Test void collectionResultMustBindTheExactTrustedShardInCheckedConfig() throws Exception {
        var source=load("success");var scope=new ResultBundleValidator.CollectionScope(ResearchCollectionExportBundle.VERSION,"col_"+"1".repeat(64),"shard_00000");
        var ctx=new ResultBundleValidator.Context("ds_fixture","EXPORTED","exp_fixture","WINDOW","1",CANDIDATES,null,null,null,scope);
        assertEquals("IMP_COLLECTION_SCOPE_MISMATCH",assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(source,ctx)).getMessage());
        var artifacts=new HashMap<>(source.artifacts());var config=(ObjectNode)JSON.readTree(artifacts.get("experiment_config.json"));
        config.set("collection_scope",scope.toJson());artifacts.put("experiment_config.json",FeatureExportBundle.bytes(config));
        var success=(ObjectNode)source.success().deepCopy();((ObjectNode)success.path("checksums")).put("experiment_config.json",ResultBundleContract.sha256(artifacts.get("experiment_config.json")));
        var bundle=new ResultBundleValidator.DecodedBundle(success,artifacts,source.memberships(),source.summaries(),true);
        assertEquals("RESULT_READY",VALIDATOR.validate(bundle,ctx).nextStatus());
        assertEquals("IMP_COLLECTION_SCOPE_MISMATCH",assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(bundle,context("WINDOW",null))).getMessage());
        var foreign=new ResultBundleValidator.Context("ds_fixture","EXPORTED","exp_fixture","WINDOW","1",CANDIDATES,null,null,null,
                new ResultBundleValidator.CollectionScope(ResearchCollectionExportBundle.VERSION,scope.collectionId(),"shard_00001"));
        assertEquals("IMP_COLLECTION_SCOPE_MISMATCH",assertThrows(IllegalArgumentException.class,()->VALIDATOR.validate(bundle,foreign)).getMessage());
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
    @Test void pythonWriterBundleIsReadSuccessfullyByMlResultBundleReader() throws Exception {
        Path root = Files.exists(Path.of("notebooks")) ? Path.of(".") : Path.of("..");
        Path harnessScript = root.resolve("notebooks/experiment_harness.py").normalize();
        Assumptions.assumeTrue(Files.exists(harnessScript), "Harness script must exist at " + harnessScript);

        String pythonExe = null;
        for (String cmd : List.of("python", "python3", "py")) {
            try {
                var p = new ProcessBuilder(cmd, "-c", "import pyarrow, notebooks.experiment_harness").directory(root.toFile()).start();
                if (p.waitFor() == 0) {
                    pythonExe = cmd;
                    break;
                }
            } catch (Exception ignored) {}
        }
        Assumptions.assumeTrue(pythonExe != null, "Python environment with pyarrow and experiment_harness must be available");

        Path tempDir = Files.createTempDirectory("mli03-py-writer-");
        String code = "import shutil, sys\n"
            + "from pathlib import Path\n"
            + "import notebooks.experiment_harness as harness\n"
            + "out_dir = Path(sys.argv[1])\n"
            + "bundle = {'mode': harness.MODE_OFFICIAL, 'source_dataset_id': 'ds_fixture', 'feature_version': 'v1', 'scaling_config_version': 'v1', 'manifest': {'dataset_id': 'ds_fixture'}}\n"
            + "candidates = [{'candidate_event_id': 'main', 'delta_time_hours': 0.0}, {'candidate_event_id': 'post', 'delta_time_hours': 1.0}]\n"
            + "mem_rows = harness.build_membership_rows(experiment_run_id='exp_fixture', dataset_id='ds_fixture', mainshock_event_id='main', candidate_records=candidates, algorithm_name='WINDOW', model_config_version='1')\n"
            + "sum_row = harness.build_sequence_summary_row(experiment_run_id='exp_fixture', dataset_id='ds_fixture', mainshock_event_id='main', candidate_count=2, algorithm_name='WINDOW', model_config_version='1', membership_rows=mem_rows)\n"
            + "metrics = {'dataset_id': 'ds_fixture', 'experiment_run_id': 'exp_fixture', 'algorithm_name': 'WINDOW', 'model_config_version': '1', 'algorithm_version': '1.0.0', 'package_version': '1.0.0', 'code_version': 'v1', 'membership_row_count': 2, 'summary_row_count': 1}\n"
            + "harness.write_experiment_result(output_dir=out_dir, bundle=bundle, algorithm_name='WINDOW', algorithm_version='1.0.0', model_config_version='1', model_config={'param': 'test'}, memberships=mem_rows, summaries=[sum_row], metrics=metrics, mode=harness.MODE_OFFICIAL, random_seed=42)\n";

        var proc = new ProcessBuilder(pythonExe, "-c", code, tempDir.toAbsolutePath().toString())
            .directory(root.toFile())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
        assertTrue(proc.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, proc.exitValue());
        var bundle = MlResultBundleReader.read(tempDir);
        assertNotNull(bundle);
        assertEquals(2, bundle.memberships().size());
        assertEquals(1, bundle.summaries().size());
        assertEquals("COMPLETED", bundle.success().path("status").asText());
        String runId = bundle.success().path("experiment_run_id").asText();
        var context = new ResultBundleValidator.Context("ds_fixture", "EXPORTED", runId, "WINDOW", "1", CANDIDATES, null);
        var receipt = VALIDATOR.validate(bundle, context);
        assertEquals("RESULT_READY", receipt.nextStatus());

        // 1. Idempotent Retry: Writing retry of the same run into tempDir preserves checksums and passes validation
        String retryCode = "import sys\n"
            + "from pathlib import Path\n"
            + "import notebooks.experiment_harness as harness\n"
            + "out_dir = Path(sys.argv[1])\n"
            + "bundle = {'mode': harness.MODE_OFFICIAL, 'source_dataset_id': 'ds_fixture', 'feature_version': 'v1', 'scaling_config_version': 'v1', 'manifest': {'dataset_id': 'ds_fixture'}}\n"
            + "candidates = [{'candidate_event_id': 'main', 'delta_time_hours': 0.0}, {'candidate_event_id': 'post', 'delta_time_hours': 1.0}]\n"
            + "mem_rows = harness.build_membership_rows(experiment_run_id='" + runId + "', dataset_id='ds_fixture', mainshock_event_id='main', candidate_records=candidates, algorithm_name='WINDOW', model_config_version='1')\n"
            + "sum_row = harness.build_sequence_summary_row(experiment_run_id='" + runId + "', dataset_id='ds_fixture', mainshock_event_id='main', candidate_count=2, algorithm_name='WINDOW', model_config_version='1', membership_rows=mem_rows)\n"
            + "metrics = {'dataset_id': 'ds_fixture', 'experiment_run_id': '" + runId + "', 'algorithm_name': 'WINDOW', 'model_config_version': '1', 'algorithm_version': '1.0.0', 'package_version': '1.0.0', 'code_version': 'v1', 'membership_row_count': 2, 'summary_row_count': 1}\n"
            + "harness.write_experiment_result(output_dir=out_dir, bundle=bundle, algorithm_name='WINDOW', algorithm_version='1.0.0', model_config_version='1', model_config={'param': 'test'}, memberships=mem_rows, summaries=[sum_row], metrics=metrics, mode=harness.MODE_OFFICIAL, random_seed=42, experiment_run_id='" + runId + "', is_retry=True)\n";

        var procRetry = new ProcessBuilder(pythonExe, "-c", retryCode, tempDir.toAbsolutePath().toString())
            .directory(root.toFile())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
        assertTrue(procRetry.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, procRetry.exitValue());

        var retryBundle = MlResultBundleReader.read(tempDir);
        var retryContext = new ResultBundleValidator.Context("ds_fixture", "EXPORTED", runId, "WINDOW", "1", CANDIDATES, receipt.bundleSha256());
        var retryReceipt = VALIDATOR.validate(retryBundle, retryContext);
        assertEquals("RESULT_READY", retryReceipt.nextStatus());
        assertEquals(receipt.bundleSha256(), retryReceipt.bundleSha256());

        // 2. Conflict: Writing into completed directory without retry permission is rejected
        String conflictCode = "import sys\n"
            + "from pathlib import Path\n"
            + "import notebooks.experiment_harness as harness\n"
            + "out_dir = Path(sys.argv[1])\n"
            + "bundle = {'mode': harness.MODE_OFFICIAL, 'source_dataset_id': 'ds_fixture', 'feature_version': 'v1', 'scaling_config_version': 'v1', 'manifest': {'dataset_id': 'ds_fixture'}}\n"
            + "candidates = [{'candidate_event_id': 'main', 'delta_time_hours': 0.0}]\n"
            + "mem_rows = harness.build_membership_rows(experiment_run_id='conflict_run', dataset_id='ds_fixture', mainshock_event_id='main', candidate_records=candidates, algorithm_name='WINDOW', model_config_version='1')\n"
            + "sum_row = harness.build_sequence_summary_row(experiment_run_id='conflict_run', dataset_id='ds_fixture', mainshock_event_id='main', candidate_count=1, algorithm_name='WINDOW', model_config_version='1', membership_rows=mem_rows)\n"
            + "metrics = {'dataset_id': 'ds_fixture', 'experiment_run_id': 'conflict_run', 'algorithm_name': 'WINDOW', 'model_config_version': '1', 'algorithm_version': '1.0.0', 'package_version': '1.0.0', 'code_version': 'v1', 'membership_row_count': 1, 'summary_row_count': 1}\n"
            + "harness.write_experiment_result(output_dir=out_dir, bundle=bundle, algorithm_name='WINDOW', algorithm_version='1.0.0', model_config_version='1', model_config={'param': 'test'}, memberships=mem_rows, summaries=[sum_row], metrics=metrics, mode=harness.MODE_OFFICIAL, is_retry=False)\n";

        var procConflict = new ProcessBuilder(pythonExe, "-c", conflictCode, tempDir.toAbsolutePath().toString())
            .directory(root.toFile())
            .start();
        assertTrue(procConflict.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
        assertNotEquals(0, procConflict.exitValue());
    }

    private static ObjectNode collectionRegistry(Path root, String datasetId, String collectionId,
            Map<String, List<ResultBundleValidator.Candidate>> sourceShards) throws Exception {
        String exportRef = "exports/" + datasetId + "/";
        Files.createDirectories(root.resolve(exportRef));
        var manifest = JSON.createObjectNode().put("dataset_id", datasetId).put("dataset_status", "EXPORTED")
            .put("export_version", ResearchCollectionExportBundle.VERSION)
            .put("registry_snapshot_id", "101").put("candidate_snapshot_id", "102").put("mainshock_snapshot_id", "103");
        var plan = manifest.putObject("collection_plan").put("collection_id", collectionId).putArray("shards");
        var entries = manifest.putObject("collection_shards");
        var exportChecksums = new StringBuilder();
        for (var shard : new TreeMap<>(sourceShards).entrySet()) {
            String shardId = shard.getKey();
            var lineage = JSON.createObjectNode().put("dataset_id", datasetId)
                .put("collection_id", collectionId).put("shard_id", shardId);
            var candidates = lineage.putArray("candidates");
            Map<String, Long> counts = new TreeMap<>();
            for (var source : shard.getValue()) {
                candidates.addObject().put("mainshock_event_id", source.mainshockId())
                    .put("candidate_event_id", source.eventId()).put("role", source.role());
                counts.merge(source.mainshockId(), 1L, Long::sum);
            }
            String lineageRef = exportRef + shardId + ".json";
            var bytes = FeatureExportBundle.bytes(lineage);
            Files.write(root.resolve(lineageRef), bytes);
            String sha = TrustedDatasetResolver.sha256(bytes);
            exportChecksums.append(sha).append("  ").append(shardId).append(".json\n");
            entries.putObject(shardId).put("lineage_ref", lineageRef).put("lineage_sha256", sha)
                .put("mainshock_count", counts.size()).put("feature_row_count", shard.getValue().size());
            var mainshocks = plan.addObject().put("shard_id", shardId).putArray("mainshocks");
            counts.forEach((id, count) -> mainshocks.addObject().put("mainshock_event_id", id)
                .put("candidate_row_count", count));
        }
        String checksumsRef = exportRef + "export-checksums.sha256";
        Path checksums = Files.writeString(root.resolve(checksumsRef), exportChecksums);
        manifest.put("export_manifest_ref", checksumsRef)
            .put("export_manifest_sha256", TrustedDatasetResolver.sha256(Files.readAllBytes(checksums)));
        Path path = root.resolve("registry/" + datasetId + ".json");
        Files.createDirectories(path.getParent());
        byte[] bytes = FeatureExportBundle.bytes(manifest);
        Files.write(path, bytes);
        return JSON.createObjectNode().put("dataset_id", datasetId).put("collection_id", collectionId)
            .put("dataset_manifest_ref", "registry/" + datasetId + ".json")
            .put("dataset_manifest_sha256", TrustedDatasetResolver.sha256(bytes));
    }

    @Test void collectionImportAcceptsOnlyMatchingSiblingShards(@TempDir Path temp) throws Exception {
        Path root = temp.toRealPath();
        Path fullDir = root.resolve("window");
        Path shard0Dir = root.resolve("window_shards").resolve("shard_00000");
        Path shard1Dir = root.resolve("window_shards").resolve("shard_00001");
        Files.createDirectories(fullDir);
        Files.createDirectories(shard0Dir);
        Files.createDirectories(shard1Dir);

        String dsId = "ds_fixture";
        String collectionId = "col_" + "a".repeat(64);
        var fullCandidates = List.of(
            new ResultBundleValidator.Candidate("main-0", "main-0", "MAINSHOCK"),
            new ResultBundleValidator.Candidate("main-0", "post-0", "POST"),
            new ResultBundleValidator.Candidate("main-1", "main-1", "MAINSHOCK"),
            new ResultBundleValidator.Candidate("main-1", "post-1", "POST")
        );
        // Registry inputs are fixed before any result artifact is written or decoded.
        Path registry = root.resolve("trusted");
        var request = collectionRegistry(registry, dsId, collectionId, Map.of(
            "shard_00000", fullCandidates.subList(0, 2), "shard_00001", fullCandidates.subList(2, 4)))
            .put("algorithm_name", "WINDOW").put("model_config_version", "1");

        java.util.function.BiConsumer<Path, List<String>> writeCleanBundle = (dir, midsAndScope) -> {
            try {
                boolean isShard = midsAndScope.size() == 2 && midsAndScope.get(1).startsWith("shard_");
                List<String> mids = isShard ? List.of(midsAndScope.get(0)) : midsAndScope;
                String shardId = isShard ? midsAndScope.get(1) : null;
                String runId = isShard ? "exp_fixture_" + shardId : "exp_fixture_full";

                List<JsonNode> members = new ArrayList<>();
                List<JsonNode> summaries = new ArrayList<>();
                for (String mid : mids) {
                    String pid = mid.equals("main-0") ? "post-0" : "post-1";
                    ObjectNode m = base("memberships.parquet");
                    m.put("dataset_id", dsId).put("experiment_run_id", runId).put("mainshock_event_id", mid)
                        .put("candidate_event_id", mid).put("event_role_candidate", "MAINSHOCK")
                        .put("is_sequence_member", true).put("multi_sequence_status", "UNIQUE").put("resolution_rank", 1);
                    ObjectNode p = m.deepCopy();
                    p.put("candidate_event_id", pid).put("event_role_candidate", "POST");
                    members.add(m);
                    members.add(p);

                    ObjectNode s = base("sequence_summary.parquet");
                    s.put("dataset_id", dsId).put("experiment_run_id", runId).put("mainshock_event_id", mid)
                        .put("window_result_status", "SUCCESS").put("productive_sequence_detected", true)
                        .put("candidate_count", 2L).put("sequence_member_count", 2L)
                        .put("aftershock_count", 1L).put("foreshock_candidate_count", 0L);
                    s.putArray("metric_reason_codes").add("METRIC_NOT_APPLICABLE");
                    summaries.add(s);
                }

                writeParquet(dir.resolve("memberships.parquet"), "memberships.parquet", members);
                writeParquet(dir.resolve("sequence_summary.parquet"), "sequence_summary.parquet", summaries);
                ObjectNode cfg = JSON.createObjectNode().put("dataset_id", dsId).put("experiment_run_id", runId)
                    .put("algorithm_name", "WINDOW").put("model_config_version", "1")
                    .put("algorithm_version", "1.0.0").put("package_version", "1.0.0").put("code_version", "v1");
                if (isShard) {
                    cfg.set("collection_scope", new ResultBundleValidator.CollectionScope(
                        ResearchCollectionExportBundle.VERSION, collectionId, shardId).toJson());
                }
                Files.writeString(dir.resolve("experiment_config.json"), fixtureJson(cfg));
                Files.writeString(dir.resolve("experiment_metrics.json"), fixtureJson(
                    JSON.createObjectNode().put("dataset_id", dsId).put("experiment_run_id", runId)
                        .put("algorithm_name", "WINDOW").put("model_config_version", "1")
                        .put("membership_row_count", (long) members.size()).put("summary_row_count", (long) summaries.size())));
                Files.writeString(dir.resolve("requirements-lock.txt"), "# verified sibling bundle\n");
                ObjectNode succ = JSON.createObjectNode().put("schema_version", "1.0")
                    .put("dataset_id", dsId).put("experiment_run_id", runId)
                    .put("algorithm_name", "WINDOW").put("model_config_version", "1")
                    .put("algorithm_version", "1.0.0").put("package_version", "1.0.0").put("code_version", "v1")
                    .put("membership_row_count", (long) members.size()).put("summary_row_count", (long) summaries.size())
                    .put("status", "COMPLETED").put("completed_at_utc", "2024-01-01T00:00:00Z")
                    .put("artifact_uri", "s3://fixture/results/" + runId + "/");
                ObjectNode chks = succ.putObject("checksums");
                for (JsonNode file : ResultBundleContract.contract().get("required_artifacts")) {
                    chks.put(file.asText(), ResultBundleContract.sha256(Files.readAllBytes(dir.resolve(file.asText()))));
                }
                Files.writeString(dir.resolve("_SUCCESS.json"), fixtureJson(succ));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };

        writeCleanBundle.accept(fullDir, List.of("main-0", "main-1"));
        writeCleanBundle.accept(shard0Dir, List.of("main-0", "shard_00000"));
        writeCleanBundle.accept(shard1Dir, List.of("main-1", "shard_00001"));

        var fullDecoded = MlResultBundleReader.read(fullDir);
        assertEquals(4, fullDecoded.memberships().size());
        var fullRequest = request.deepCopy().put("experiment_run_id", "exp_fixture_full");
        assertEquals("IMP_REQUEST_FIELD_REQUIRED", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(fullRequest, registry, null)).getMessage());
        fullRequest.put("collection_shard_id", "shard_00000");
        var fullAsShard = TrustedDatasetResolver.resolve(fullRequest, registry, null);
        assertEquals("IMP_COLLECTION_SCOPE_MISMATCH", assertThrows(IllegalArgumentException.class,
            () -> VALIDATOR.validate(fullDecoded, fullAsShard)).getMessage());

        for (var shardDir : List.of(shard0Dir, shard1Dir)) {
            String shardId = shardDir.getFileName().toString();
            var shardRequest = request.deepCopy().put("collection_shard_id", shardId)
                .put("experiment_run_id", "exp_fixture_" + shardId);
            var context = TrustedDatasetResolver.resolve(shardRequest, registry, null);
            var decoded = MlResultBundleReader.read(shardDir);
            assertEquals("RESULT_READY", VALIDATOR.validate(decoded, context).nextStatus());

            shardRequest.put("collection_shard_id", shardId.equals("shard_00000") ? "shard_00001" : "shard_00000");
            var otherShard = TrustedDatasetResolver.resolve(shardRequest, registry, null);
            assertEquals("IMP_COLLECTION_SCOPE_MISMATCH", assertThrows(IllegalArgumentException.class,
                () -> VALIDATOR.validate(decoded, otherShard)).getMessage());
            shardRequest.put("collection_shard_id", "shard_00002");
            assertEquals("IMP_COLLECTION_SCOPE_REQUIRED", assertThrows(IllegalArgumentException.class,
                () -> TrustedDatasetResolver.resolve(shardRequest, registry, null)).getMessage());
        }

        // Legacy nested full/shards/ layout must be rejected by MlResultBundleReader
        Files.createDirectories(fullDir.resolve("shards/shard_00000"));
        Files.writeString(fullDir.resolve("shards/shard_00000/_SUCCESS.json"), "{}\n");
        assertEquals("IMP_UNSAFE_ARTIFACT_METADATA",
            assertThrows(IllegalArgumentException.class, () -> MlResultBundleReader.read(fullDir)).getMessage());
    }

    @Test void officialPythonBaselinesValidateEachShardThroughTrustedRegistry(@TempDir Path temp) throws Exception {
        Path root = Files.exists(Path.of("notebooks")) ? Path.of(".") : Path.of("..");
        Assumptions.assumeTrue(Files.exists(root.resolve("notebooks/baseline_models.py")), "baseline_models.py must exist");

        String pythonExe = null;
        for (String cmd : List.of("python", "python3", "py")) {
            try {
                var p = new ProcessBuilder(cmd, "-c", "import pyarrow, sklearn, notebooks.baseline_models, tests.test_research_collection_loader")
                    .directory(root.toFile()).start();
                if (p.waitFor() == 0) {
                    pythonExe = cmd;
                    break;
                }
            } catch (Exception ignored) {}
        }
        Assumptions.assumeTrue(pythonExe != null, "Python with pyarrow, sklearn, and baseline_models required");

        Path workDir = temp.toRealPath();
        String pyCode = "import importlib.util, sys\n"
            + "from pathlib import Path\n"
            + "work = Path(sys.argv[1])\n"
            + "spec_c = importlib.util.spec_from_file_location('col_fx', 'tests/test_research_collection_loader.py')\n"
            + "col_mod = importlib.util.module_from_spec(spec_c); spec_c.loader.exec_module(col_mod)\n"
            + "spec_r = importlib.util.spec_from_file_location('exp02_r', 'scripts/run-exp02-official.py')\n"
            + "runner = importlib.util.module_from_spec(spec_r); spec_r.loader.exec_module(runner)\n"
            + "repro = work / 'repro'; pilot = work / 'pilot'; out = work / 'out'\n"
            + "repro.mkdir(); pilot.mkdir()\n"
            + "pins = col_mod.fixture(repro, pilot)\n"
            + "runner.execute_official_baselines(bundle_dir=repro, output_dir=out, output_json=work / 'report.json', manifest_sha256=pins['manifest_sha256'], checksums_sha256=pins['checksums_sha256'], expected_handoff_sha256=None)\n";

        Path runnerLog = workDir.resolve("runner.log");
        var proc = new ProcessBuilder(pythonExe, "-c", pyCode, workDir.toAbsolutePath().toString())
            .directory(root.toFile()).redirectErrorStream(true).redirectOutput(runnerLog.toFile()).start();
        if (!proc.waitFor(60, java.util.concurrent.TimeUnit.SECONDS)) {
            proc.destroyForcibly();
            fail("Fixture baseline runner exceeded its time budget");
        }
        assertEquals(0, proc.exitValue(), () -> "Fixture baseline runner failed; inspect " + runnerLog);
        JsonNode report = JSON.readTree(Files.readAllBytes(workDir.resolve("report.json")));
        // Independent expectations from tests.test_research_collection_loader.fixture and its pilot seed.
        String post = "evt_" + "2".repeat(32);
        Path registry = workDir.resolve("trusted");
        var request = collectionRegistry(registry, "ds_" + "1".repeat(64), "col_" + "2".repeat(64), Map.of(
            "shard_00000", List.of(new ResultBundleValidator.Candidate("main-0", "main-0", "MAINSHOCK"),
                new ResultBundleValidator.Candidate("main-0", post, "POST")),
            "shard_00001", List.of(new ResultBundleValidator.Candidate("main-1", "main-1", "MAINSHOCK"),
                new ResultBundleValidator.Candidate("main-1", post, "POST"))));

        for (String algorithm : List.of("WINDOW", "DBSCAN", "HDBSCAN_GLOBAL")) {
            String dirName = algorithm.toLowerCase(Locale.ROOT);
            var full = MlResultBundleReader.read(workDir.resolve("out").resolve(dirName));
            assertEquals(4, full.memberships().size());
            assertEquals(2, full.summaries().size());
            for (String shard : List.of("shard_00000", "shard_00001")) {
                var entry = report.path("shard_verifications").path(algorithm).path(shard);
                var shardRequest = request.deepCopy().put("collection_shard_id", shard)
                    .put("experiment_run_id", entry.path("experiment_run_id").asText())
                    .put("algorithm_name", algorithm).put("model_config_version",
                        report.path("model_config_pins").path(algorithm).path("model_config_version").asText());
                var context = TrustedDatasetResolver.resolve(shardRequest, registry, null);
                var decoded = MlResultBundleReader.read(workDir.resolve("out").resolve(dirName + "_shards").resolve(shard));
                assertEquals("RESULT_READY", VALIDATOR.validate(decoded, context).nextStatus(), algorithm + "/" + shard);
            }
        }
    }

    @Test void crossShardAdaptiveOverlapPassesReaderWhileContractAndResolverBoundariesHold(@TempDir Path temporary) throws Exception {
        Path root = temporary.toRealPath();
        Path registryRoot = root.resolve("registry");
        String dsId = "ds_" + "c".repeat(64);
        String colId = "col_" + "d".repeat(64);
        String modelVer = "hdbscan-adaptive-dev-v1";

        var fullCandidates = List.of(
            new ResultBundleValidator.Candidate("main-0", "main-0", "MAINSHOCK"),
            new ResultBundleValidator.Candidate("main-0", "shared_aft", "POST"),
            new ResultBundleValidator.Candidate("main-1", "main-1", "MAINSHOCK"),
            new ResultBundleValidator.Candidate("main-1", "shared_aft", "POST"));
        var fullReq = collectionRegistry(registryRoot, dsId, colId, Map.of(
            "shard_00000", fullCandidates.subList(0, 2), "shard_00001", fullCandidates.subList(2, 4)))
            .put("experiment_run_id", "exp_adapt_full").put("algorithm_name", "HDBSCAN_ADAPTIVE")
            .put("model_config_version", modelVer);

        Path outRoot = root.resolve("out");
        Path fullDir = outRoot.resolve("hdbscan_adaptive");
        Path shard0Dir = outRoot.resolve("hdbscan_adaptive_shards").resolve("shard_00000");
        Path shard1Dir = outRoot.resolve("hdbscan_adaptive_shards").resolve("shard_00001");
        Files.createDirectories(fullDir);
        Files.createDirectories(shard0Dir);
        Files.createDirectories(shard1Dir);

        java.util.function.Function<String, JsonNode[]> buildWindowRows = (mid) -> {
            String runId = mid.equals("main-0") ? "exp_adapt_shard_00000" : "exp_adapt_shard_00001";
            int sharedRank = mid.equals("main-0") ? 1 : 2;
            ObjectNode mRow = base("memberships.parquet");
            mRow.put("dataset_id", dsId).put("experiment_run_id", runId).put("algorithm_name", "HDBSCAN_ADAPTIVE")
                .put("model_config_version", modelVer).put("mainshock_event_id", mid).put("candidate_event_id", mid)
                .put("event_role_candidate", "MAINSHOCK").put("cluster_id", 0L).put("is_noise", false)
                .put("is_mainshock_cluster_member", true).put("is_sequence_member", true).put("membership_probability", 1.0)
                .put("multi_sequence_status", "UNIQUE").put("resolution_rank", 1);
            ObjectNode aRow = mRow.deepCopy();
            aRow.put("candidate_event_id", "shared_aft").put("event_role_candidate", "POST")
                .put("membership_probability", mid.equals("main-0") ? 0.92 : 0.70)
                .put("multi_sequence_status", "MULTIPLE").put("resolution_rank", sharedRank);
            ObjectNode sRow = base("sequence_summary.parquet");
            sRow.put("dataset_id", dsId).put("experiment_run_id", runId).put("algorithm_name", "HDBSCAN_ADAPTIVE")
                .put("model_config_version", modelVer).put("mainshock_event_id", mid)
                .put("window_result_status", "SUCCESS").put("productive_sequence_detected", true)
                .put("candidate_count", 2L).put("cluster_count", 1).put("noise_count", 0L).put("noise_ratio", 0.0)
                .put("sequence_member_count", 2L).put("aftershock_count", 1L).put("foreshock_candidate_count", 0L)
                .put("mean_membership_probability", mid.equals("main-0") ? 0.96 : 0.85);
            sRow.putArray("metric_reason_codes").add("METRIC_NOT_APPLICABLE");
            return new JsonNode[] { mRow, aRow, sRow };
        };

        java.util.function.BiConsumer<Path, String> writeAdaptiveBundle = (dir, modeTag) -> {
            try {
                boolean isShard = modeTag.startsWith("shard_");
                String runId = isShard ? "exp_adapt_" + modeTag : "exp_adapt_full";
                List<JsonNode> members = new ArrayList<>();
                List<JsonNode> summaries = new ArrayList<>();
                List<String> mids = isShard ? List.of(modeTag.equals("shard_00000") ? "main-0" : "main-1") : List.of("main-0", "main-1");
                for (String mid : mids) {
                    JsonNode[] rows = buildWindowRows.apply(mid);
                    ((ObjectNode) rows[0]).put("experiment_run_id", runId);
                    ((ObjectNode) rows[1]).put("experiment_run_id", runId);
                    ((ObjectNode) rows[2]).put("experiment_run_id", runId);
                    members.add(rows[0]);
                    members.add(rows[1]);
                    summaries.add(rows[2]);
                }
                writeParquet(dir.resolve("memberships.parquet"), "memberships.parquet", members);
                writeParquet(dir.resolve("sequence_summary.parquet"), "sequence_summary.parquet", summaries);
                ObjectNode cfg = JSON.createObjectNode().put("dataset_id", dsId).put("experiment_run_id", runId)
                    .put("algorithm_name", "HDBSCAN_ADAPTIVE").put("model_config_version", modelVer)
                    .put("algorithm_version", "1.0.0").put("package_version", "1.0.0").put("code_version", "v2");
                if (isShard) {
                    cfg.set("collection_scope", new ResultBundleValidator.CollectionScope(
                        ResearchCollectionExportBundle.VERSION, colId, modeTag).toJson());
                }
                Files.writeString(dir.resolve("experiment_config.json"), fixtureJson(cfg));
                Files.writeString(dir.resolve("experiment_metrics.json"), fixtureJson(
                    JSON.createObjectNode().put("dataset_id", dsId).put("experiment_run_id", runId)
                        .put("algorithm_name", "HDBSCAN_ADAPTIVE").put("model_config_version", modelVer)
                        .put("membership_row_count", (long) members.size()).put("summary_row_count", (long) summaries.size())));
                Files.writeString(dir.resolve("requirements-lock.txt"), "# verified adaptive sibling fixture\n");
                ObjectNode succ = JSON.createObjectNode().put("schema_version", "1.0")
                    .put("dataset_id", dsId).put("experiment_run_id", runId)
                    .put("algorithm_name", "HDBSCAN_ADAPTIVE").put("model_config_version", modelVer)
                    .put("algorithm_version", "1.0.0").put("package_version", "1.0.0").put("code_version", "v2")
                    .put("membership_row_count", (long) members.size()).put("summary_row_count", (long) summaries.size())
                    .put("status", "COMPLETED").put("completed_at_utc", "2024-01-01T00:00:00Z")
                    .put("artifact_uri", "s3://fixture/results/" + runId + "/");
                ObjectNode chks = succ.putObject("checksums");
                for (JsonNode file : ResultBundleContract.contract().get("required_artifacts")) {
                    chks.put(file.asText(), ResultBundleContract.sha256(Files.readAllBytes(dir.resolve(file.asText()))));
                }
                Files.writeString(dir.resolve("_SUCCESS.json"), fixtureJson(succ));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };

        writeAdaptiveBundle.accept(fullDir, "full");
        writeAdaptiveBundle.accept(shard0Dir, "shard_00000");
        writeAdaptiveBundle.accept(shard1Dir, "shard_00001");

        // 1. Full-split passes MlResultBundleReader + standalone contract check (evaluation only),
        //    but TrustedDatasetResolver rejects full-split requests lacking collection_shard_id.
        var fullDecoded = MlResultBundleReader.read(fullDir);
        assertEquals(4, fullDecoded.memberships().size());
        assertEquals("RESULT_READY", VALIDATOR.validate(fullDecoded, new ResultBundleValidator.Context(
            dsId, "EXPORTED", "exp_adapt_full", "HDBSCAN_ADAPTIVE", modelVer, fullCandidates, null)).nextStatus());

        assertEquals("IMP_REQUEST_FIELD_REQUIRED", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(fullReq, registryRoot, null)).getMessage());
        assertEquals("IMP_COLLECTION_SCOPE_REQUIRED", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(
                fullReq.deepCopy().put("collection_shard_id", "full"), registryRoot, null)).getMessage());

        var mislabeledFull = fullReq.deepCopy().put("collection_shard_id", "shard_00000");
        var shard0Ctx = TrustedDatasetResolver.resolve(mislabeledFull, registryRoot, null);
        assertEquals("IMP_COLLECTION_SCOPE_MISMATCH", assertThrows(IllegalArgumentException.class,
            () -> VALIDATOR.validate(fullDecoded, shard0Ctx)).getMessage());

        // Each shard retains its collection-wide rank, including rank 2 when rank 1 is in its sibling.
        for (String sid : List.of("shard_00000", "shard_00001")) {
            Path shardDir = outRoot.resolve("hdbscan_adaptive_shards").resolve(sid);
            ObjectNode req = fullReq.deepCopy().put("experiment_run_id", "exp_adapt_" + sid).put("collection_shard_id", sid);
            var resolvedCtx = TrustedDatasetResolver.resolve(req, registryRoot, null);
            var shardDecoded = MlResultBundleReader.read(shardDir);
            assertEquals(2, shardDecoded.memberships().size());
            assertEquals("MULTIPLE", shardDecoded.memberships().get(1).path("multi_sequence_status").asText());
            assertEquals(sid.equals("shard_00000") ? 1 : 2, shardDecoded.memberships().get(1).path("resolution_rank").asInt());
            assertEquals(Set.of("shared_aft"), resolvedCtx.collectionScope().crossShardCandidateIds());
            assertEquals("RESULT_READY", VALIDATOR.validate(shardDecoded, resolvedCtx).nextStatus());

            var localOnly = new ResultBundleValidator.Context(dsId, resolvedCtx.datasetStatus(),
                resolvedCtx.runId(), resolvedCtx.algorithm(), modelVer, resolvedCtx.candidates(), null,
                resolvedCtx.datasetPins(), null, new ResultBundleValidator.CollectionScope(
                    ResearchCollectionExportBundle.VERSION, colId, sid));
            assertEquals("IMP_SCHEMA_MISMATCH", assertThrows(IllegalArgumentException.class,
                () -> VALIDATOR.validate(shardDecoded, localOnly)).getMessage());
        }
        Path sibling = registryRoot.resolve("exports").resolve(dsId).resolve("shard_00001.json");
        Files.writeString(sibling, "changed bytes");
        var adaptiveRequest = fullReq.deepCopy().put("experiment_run_id", "exp_adapt_shard_00000")
            .put("collection_shard_id", "shard_00000");
        assertEquals("IMP_CHECKSUM_MISMATCH", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(adaptiveRequest, registryRoot, null)).getMessage());
        Files.delete(sibling);
        assertEquals("IMP_DATASET_REFERENCE_INVALID", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(adaptiveRequest, registryRoot, null)).getMessage());
    }

    @Test void crossShardResolutionAllowsGlobalRanksButRejectsInvalidLocalGrain() {
        var rankTwo = JSON.createObjectNode().put("multi_sequence_status", "MULTIPLE").put("resolution_rank", 2);
        var rankThree = rankTwo.deepCopy().put("resolution_rank", 3);
        var ambiguous = JSON.createObjectNode().put("multi_sequence_status", "AMBIGUOUS").putNull("resolution_rank");
        ResultBundleContract.validateResolution(List.of(rankTwo, rankThree), true);
        ResultBundleContract.validateResolution(List.of(ambiguous), true);
        assertThrows(IllegalArgumentException.class, () -> ResultBundleContract.validateResolution(List.of(rankTwo, rankThree), false));
        assertThrows(IllegalArgumentException.class, () -> ResultBundleContract.validateResolution(List.of(ambiguous), false));
        assertThrows(IllegalArgumentException.class, () -> ResultBundleContract.validateResolution(List.of(rankTwo, rankTwo), true));
        assertThrows(IllegalArgumentException.class, () -> ResultBundleContract.validateResolution(List.of(rankTwo, ambiguous), true));

    }
}
