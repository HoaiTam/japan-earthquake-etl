"""OPS-01 bounded Iceberg backup/restore control plane, never event ETL.

Back up original bytes, relocate only Iceberg metadata/Avro paths into a new
namespace, preserve snapshots and Parquet bytes. No DROP/DELETE/ETL/lease writes.
"""
import base64
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import uuid
import zipfile
from urllib.parse import urlsplit

VERSION = 'ops01-backup-v1'
MAX_OBJECT = 32 * 1024**2
MAX_TOTAL = 64 * 1024**2
MAX_OBJECTS = 512
IDENT = re.compile(r'[a-z][a-z0-9_]{0,100}\Z')
SHA = re.compile(r'[a-f0-9]{64}\Z')
RESTORE_KEY = r'warehouse/ops01-restores/([a-f0-9]{32})/ops01_\1_[0-9]+/[a-z][a-z0-9_]*/[a-zA-Z0-9_./=-]+'


def require(ok, reason):
    if not ok:
        raise ValueError('OPS01_' + reason)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def decode(data):
    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, 'DUPLICATE_JSON_KEY'); out[key] = value
        return out
    def bad(_):
        raise ValueError('OPS01_NONFINITE_JSON')
    return json.loads(data, object_pairs_hook=pairs, parse_constant=bad)


def safe(path):
    p = Path(path).absolute()
    require(not any(x.is_symlink() for x in (p, *p.parents)), 'SYMLINK_REJECTED')
    return p.resolve()


def uri(value):
    require(isinstance(value, str), 'OBJECT_URI')
    p = urlsplit(value)
    require(p.scheme == 's3' and re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', p.netloc)
            and not p.query and not p.fragment and '%' not in value and '\\' not in value
            and p.path.startswith('/') and all(x not in ('', '.', '..') for x in p.path[1:].split('/')), 'OBJECT_URI')
    return p.netloc, p.path[1:]


def identifier(value):
    require(isinstance(value, str) and IDENT.fullmatch(value), 'IDENTIFIER')
    return value


def restore_uri(value):
    bucket, key = uri(value)
    require(re.fullmatch(RESTORE_KEY, key), 'EXACT_RESTORE_OBJECT_PREFIX')
    return bucket, key


def snapshot_id(value):
    require(type(value) is int or isinstance(value, str) and re.fullmatch(r'[1-9][0-9]{0,18}', value), 'SNAPSHOT_INT64')
    value = int(value)
    require(0 < value <= 9223372036854775807, 'SNAPSHOT_INT64')
    return value


def private_write(path, data):
    path = safe(path)
    with path.open('xb') as f:
        os.chmod(path, 0o600); f.write(data)


def scan_bytes(raw, role, secrets=()):
    """Scan decoded content, not just compressed bytes. Never print findings' values."""
    import importlib.util
    spec = importlib.util.spec_from_file_location('ops01_security', Path(__file__).with_name('audit-security.py'))
    security = importlib.util.module_from_spec(spec); spec.loader.exec_module(security)
    def text(value):
        if isinstance(value, dict):
            for k, v in value.items(): text(k); text(v)
        elif isinstance(value, (list, tuple)):
            for x in value: text(x)
        elif isinstance(value, (str, bytes)):
            s = value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value
            require(not security.scan_text(s, ml=True) and not any(v in s for v in secrets), 'SECRET_OR_PRIVATE_PATH')
    if role == 'data':
        import pyarrow.parquet as pq
        with pq.ParquetFile(io.BytesIO(raw)) as f:
            require(f.metadata.num_rows <= 50000, 'PARQUET_SCAN_ROW_BOUND')
            text(f.schema_arrow.metadata or {})
            for batch in f.iter_batches(batch_size=1024): text(batch.to_pylist())
    elif role in ('manifest', 'manifest-list'):
        schema, metadata, rows = avro_read(raw); text(schema); text(metadata); text(rows)
    else: text(raw)


def avro_read(data):
    import fastavro
    require(0 < len(data) <= MAX_OBJECT, 'AVRO_BYTES_BOUND')
    reader = fastavro.reader(io.BytesIO(data)); records = []
    for row in reader:
        require(len(records) < 10000, 'AVRO_RECORD_BOUND'); records.append(row)
    return reader.writer_schema, reader.metadata, records


def avro_write(schema, metadata, rows):
    import fastavro
    out = io.BytesIO()
    # Stable sync marker makes a repeated restore plan byte-identical.
    fastavro.writer(out, schema, rows, codec=metadata.get('avro.codec', 'null'),
                    metadata={k: v for k, v in metadata.items() if k not in ('avro.schema', 'avro.codec')},
                    sync_marker=hashlib.sha256(encode(schema)).digest()[:16], validator=True)
    require(out.tell() <= MAX_OBJECT, 'REWRITTEN_OBJECT_BOUND')
    return out.getvalue()


def puffin_footer(raw):
    require(len(raw) >= 20 and raw[:4] == raw[-4:] == b'PFA1', 'PUFFIN_MAGIC')
    length = int.from_bytes(raw[-12:-8], 'little', signed=True)
    require(raw[-8:-4] == b'\0' * 4 and 0 < length <= len(raw) - 20
            and raw[-16-length:-12-length] == b'PFA1', 'PUFFIN_FOOTER_UNSUPPORTED_OR_INVALID')
    footer = decode(raw[-12-length:-12])
    require(isinstance(footer.get('blobs'), list) and len(footer['blobs']) <= 1024, 'PUFFIN_BLOB_BOUND')
    for blob in footer['blobs']:
        require(blob['type'] == 'apache-datasketches-theta-v1'
                and type(blob['offset']) is int and type(blob['length']) is int
                and 4 <= blob['offset'] and 0 <= blob['length']
                and blob['offset'] + blob['length'] <= len(raw) - 16 - length, 'PUFFIN_BLOB_UNSUPPORTED')
    return footer


class Graph:
    def __init__(self, get):
        self.get = get; self.objects = {}; self.total = 0; self.metadata = set()

    def add(self, location, role, root):
        uri(location)
        require(location.startswith(root + '/'), 'EXTERNAL_GRAPH_REFERENCE')
        if location in self.objects:
            require(self.objects[location]['role'] == role, 'AMBIGUOUS_OBJECT_ROLE')
            return self.objects[location]['data']
        require(len(self.objects) < MAX_OBJECTS, 'OBJECT_COUNT_BOUND')
        raw = self.get(location)
        require(0 < len(raw) <= MAX_OBJECT and self.total + len(raw) <= MAX_TOTAL, 'BACKUP_BYTE_BOUND')
        self.total += len(raw)
        self.objects[location] = {'role': role, 'data': raw}
        if role == 'metadata':
            self.metadata.add(location)
            m = decode(raw)
            require(m.get('format-version') == 2 and m.get('location') == root, 'SUPPORTED_TABLE_FORMAT')
            require(not m.get('partition-statistics'), 'PARTITION_STATISTICS_NOT_SUPPORTED')
            for stat in m.get('statistics', []):
                data = self.add(stat['statistics-path'], 'statistics', root)
                require(len(data) == stat['file-size-in-bytes'], 'STATISTICS_SIZE')
            for prior in m.get('metadata-log', []):
                self.add(prior['metadata-file'], 'metadata', root)
            for snapshot in m.get('snapshots', []):
                require('manifest-list' in snapshot, 'MANIFEST_LIST_REQUIRED')
                self.add(snapshot['manifest-list'], 'manifest-list', root)
        elif role == 'manifest-list':
            for row in avro_read(raw)[2]:
                data = self.add(row['manifest_path'], 'manifest', root)
                require(len(data) == row['manifest_length'], 'MANIFEST_LENGTH')
        elif role == 'manifest':
            for row in avro_read(raw)[2]:
                df = row['data_file']
                require(df.get('content', 0) == 0 and df.get('file_format') == 'PARQUET', 'DELETE_OR_NONPARQUET_UNSUPPORTED')
                data = self.add(df['file_path'], 'data', root)
                require(len(data) == df['file_size_in_bytes'], 'DATA_FILE_SIZE')
        elif role == 'statistics': puffin_footer(raw)
        return raw


def make_bundle(path, tables, graph, artifacts):
    objects = [{'uri': key, 'role': v['role'], 'bytes': len(v['data']), 'sha256': digest(v['data'])}
               for key, v in sorted(graph.objects.items())]
    a = [{'name': name, 'sha256': digest(raw), 'bytes': len(raw)} for name, raw in sorted(artifacts.items())]
    manifest = {'version': VERSION, 'tables': tables, 'objects': objects, 'artifacts': a,
                'scope': 'bounded_representative_smoke_not_full_disaster_recovery'}
    with safe(path).open('xb') as out:
        os.chmod(path, 0o600)
        with zipfile.ZipFile(out, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr('manifest.json', encode(manifest))
            blobs = {digest(v['data']): v['data'] for v in graph.objects.values()}
            blobs.update({digest(v): v for v in artifacts.values()})
            for sha, raw in sorted(blobs.items()): z.writestr('objects/' + sha, raw)
    return digest(safe(path).read_bytes())


def read_bundle(path, trusted_sha):
    p = safe(path)
    require(SHA.fullmatch(trusted_sha or '') and p.stat().st_size <= MAX_TOTAL * 2, 'TRUSTED_BUNDLE_BOUND')
    require(digest(p.read_bytes()) == trusted_sha, 'BUNDLE_SHA_CHANGED')
    with zipfile.ZipFile(p) as z:
        names = z.namelist()
        require(len(names) == len(set(names)) and len(names) <= MAX_OBJECTS * 2 + 1, 'DUPLICATE_ZIP_MEMBER')
        require(sum(x.file_size for x in z.infolist()) <= MAX_TOTAL * 2, 'ZIP_EXPANSION_BOUND')
        require('manifest.json' in names and z.getinfo('manifest.json').file_size <= 1024**2, 'MANIFEST_BOUND')
        m = decode(z.read('manifest.json'))
        require(m['version'] == VERSION and 0 < len(m['tables']) <= 16, 'BACKUP_VERSION_OR_TABLE_BOUND')
        require(0 < len(m['objects']) <= MAX_OBJECTS and len(m['artifacts']) <= 32, 'MANIFEST_COUNT_BOUND')
        rows = m['objects'] + m['artifacts']
        require(all(SHA.fullmatch(x['sha256']) and type(x['bytes']) is int and 0 < x['bytes'] <= MAX_OBJECT for x in rows), 'OBJECT_PIN')
        expected = {'manifest.json'} | {'objects/' + x['sha256'] for x in rows}
        require(set(names) == expected, 'EXACT_ZIP_MEMBER_SET')
        blobs = {}
        for row in rows:
            name = 'objects/' + row['sha256']; info = z.getinfo(name)
            require(not info.is_dir() and info.external_attr >> 16 & 0o170000 != 0o120000 and info.file_size == row['bytes'], 'ZIP_OBJECT_SIZE_OR_TYPE')
            raw = z.read(name); require(digest(raw) == row['sha256'], 'OBJECT_SHA_CHANGED'); blobs[row['sha256']] = raw
        require(len({x['uri'] for x in m['objects']}) == len(m['objects']), 'DUPLICATE_OBJECT_URI')
        require(len({x['name'] for x in m['artifacts']}) == len(m['artifacts']), 'DUPLICATE_ARTIFACT_NAME')
        for row in m['artifacts']:
            name = row['name']; part = PurePosixPath(name)
            require(isinstance(name, str) and not part.is_absolute() and part.as_posix() == name
                    and '\\' not in name and all(x not in ('', '.', '..') for x in name.split('/')), 'ARTIFACT_PATH')
        graph = Graph(lambda key: blobs[next(x['sha256'] for x in m['objects'] if x['uri'] == key)])
        require(len({(t['namespace'], t['name']) for t in m['tables']}) == len(m['tables']), 'DUPLICATE_TABLE')
        for table in m['tables']:
            identifier(table['namespace']); identifier(table['name']); uri(table['root'])
            raw = graph.add(table['metadata_uri'], 'metadata', table['root'])
            meta = decode(raw)
            require(meta['table-uuid'] == table['uuid'] and meta.get('current-snapshot-id') == table['snapshot_id'], 'TABLE_IDENTITY')
        require(set(graph.objects) == {x['uri'] for x in m['objects']}, 'UNREACHABLE_OR_MISSING_GRAPH_OBJECT')
        require(all(graph.objects[x['uri']]['role'] == x['role'] for x in m['objects']), 'GRAPH_ROLE_CHANGED')
        return m, graph, {x['name']: blobs[x['sha256']] for x in m['artifacts']}


def relocated(manifest, graph, run):
    require(re.fullmatch(r'[a-f0-9]{32}', run), 'RESTORE_RUN_ID')
    roots = {}; namespaces = {}
    for table in manifest['tables']:
        ns = table['namespace']
        namespaces.setdefault(ns, 'ops01_' + run + '_' + str(len(namespaces)))
        bucket, _ = uri(table['root'])
        roots[table['root']] = 's3://' + bucket + '/warehouse/ops01-restores/' + run + '/' + namespaces[ns] + '/' + table['name']
    require(len(roots) == len(manifest['tables']), 'SHARED_TABLE_ROOT_UNSUPPORTED')
    mapping = {}
    for old in graph.objects:
        matches = [root for root in roots if old.startswith(root + '/')]
        require(len(matches) == 1, 'GRAPH_ROOT_AMBIGUOUS')
        mapping[old] = roots[matches[0]] + old[len(matches[0]):]
    def rewrite(value):
        if isinstance(value, dict): return {k: rewrite(v) for k, v in value.items()}
        if isinstance(value, list): return [rewrite(v) for v in value]
        if isinstance(value, str):
            if value in mapping: return mapping[value]
            if value in roots: return roots[value]
            for old, new in roots.items():
                if value.startswith(old + '/'): return new + value[len(old):]
        return value
    output = {}
    for role in ('data', 'statistics', 'manifest', 'manifest-list', 'metadata'):
        for old, entry in sorted(graph.objects.items()):
            if entry['role'] != role: continue
            raw = entry['data']
            if role in ('manifest', 'manifest-list'):
                schema, metadata, rows = avro_read(raw)
                if role == 'manifest-list':
                    for row in rows: row['manifest_length'] = len(output[mapping[row['manifest_path']]])
                raw = avro_write(schema, metadata, rewrite(rows))
            elif role == 'metadata': raw = encode(rewrite(decode(raw)))
            output[mapping[old]] = raw
    tables = [{**t, 'source_namespace': t['namespace'], 'namespace': namespaces[t['namespace']],
               'root': roots[t['root']], 'metadata_uri': mapping[t['metadata_uri']]} for t in manifest['tables']]
    return tables, output


# Executed in an existing scheduler, never logs environment/credential values.
AGENT = r'''
import base64,json,os,re,sys,time,urllib.request,urllib.parse
from pathlib import Path
from botocore.config import Config
import boto3
p=json.load(sys.stdin); op=p['op']
def check(ok):
 if not ok: raise ValueError('OPS01_REMOTE_GUARD')
def http(path,data=None):
 base=os.environ['ICEBERG_CATALOG_URI'].rstrip('/')
 req=urllib.request.Request(base+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=30) as r:
  raw=r.read(8*1024**2+1);check(len(raw)<=8*1024**2);return json.loads(raw) if raw else {}
def s3uri(value):
 u=urllib.parse.urlsplit(value)
 check(u.scheme=='s3' and re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',u.netloc) and not u.query and not u.fragment and '%' not in value and '\\' not in value and all(x not in ('','.','..') for x in u.path[1:].split('/')))
 return u.netloc,u.path[1:]
def client():
 return boto3.client('s3',endpoint_url=os.environ['MINIO_ENDPOINT'],region_name=os.environ['S3_REGION'],aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],config=Config(retries={'max_attempts':0},s3={'addressing_style':'path'}))
if op=='load':
 check(all(re.fullmatch(r'[a-z][a-z0-9_]{0,100}',p[k]) for k in ('namespace','name')))
 result=http('/v1/namespaces/'+p['namespace']+'/tables/'+p['name']);result.pop('config',None)
elif op in ('get','put'):
 bucket,key=s3uri(p['uri']);s=client()
 if op=='put':
  check(re.fullmatch(RESTORE_KEY,key) is not None)
  data=base64.b64decode(p['data'],validate=True);check(0<len(data)<=32*1024**2)
  s.put_object(Bucket=bucket,Key=key,Body=data,IfNoneMatch='*');result={'written':True}
 else:
  r=s.get_object(Bucket=bucket,Key=key);check(0<r['ContentLength']<=32*1024**2)
  data=r['Body'].read(32*1024**2+1);r['Body'].close();check(len(data)==r['ContentLength']);result={'data':base64.b64encode(data).decode()}
elif op=='register':
 ns=p['namespace'];check(re.fullmatch(r'ops01_[a-f0-9]{32}_[0-9]+',ns) and re.fullmatch(r'[a-z][a-z0-9_]{0,100}',p['name']))
 bucket,key=s3uri(p['metadata_uri']);check(key.startswith('warehouse/ops01-restores/'+ns[6:38]+'/'+ns+'/'))
 result=http('/v1/namespaces/'+ns+'/register',{'name':p['name'],'metadata-location':p['metadata_uri']});result.pop('config',None)
elif op=='artifact':
 path=Path(p['path']);check(re.fullmatch(r'/opt/pipeline/staging/mli-03-qa/mli03-qa-[a-f0-9]{32}/(valid|registry)/[A-Za-z0-9_.-]+',str(path)))
 check(not any(x.is_symlink() for x in (path,*path.parents)) and path.is_file() and 0<path.stat().st_size<=1024**2)
 result={'data':base64.b64encode(path.read_bytes()).decode()}
elif op=='namespace':
 check(re.fullmatch(r'ops01_[a-f0-9]{32}_[0-9]+',p['namespace']))
 result=http('/v1/namespaces',{'namespace':[p['namespace']],'properties':{}})
elif op=='query':
 sql=p['sql'];check(sql.startswith('SELECT ') and ';' not in sql)
 base='http://'+os.environ['TRINO_HOST']+':'+os.environ['TRINO_INTERNAL_PORT'];headers={'X-Trino-User':'ops01-restore','X-Trino-Session':'query_max_run_time=60s'}
 req=urllib.request.Request(base+'/v1/statement',data=sql.encode(),headers=headers);rows=[];deadline=time.monotonic()+80
 while True:
  check(time.monotonic()<deadline)
  with urllib.request.urlopen(req,timeout=20) as r: raw=r.read(8*1024**2+1);check(len(raw)<=8*1024**2);d=json.loads(raw)
  check('error' not in d);rows.extend(d.get('data',[]));check(len(rows)<=50000)
  if not d.get('nextUri'):break
  url=d['nextUri'];check(urllib.parse.urlsplit(url).netloc==urllib.parse.urlsplit(base).netloc and url.startswith(base+'/v1/statement/'))
  time.sleep(.05);req=urllib.request.Request(url,headers=headers)
 result=rows
elif op=='idle':
 from sqlalchemy import create_engine,text
 from sqlalchemy.pool import NullPool
 check(not (Path(os.environ['SOURCE_GUARD_ROOT'])/'owner.json').exists())
 e=create_engine(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'],poolclass=NullPool,connect_args={'connect_timeout':10,'options':'-c default_transaction_read_only=on -c statement_timeout=10000'})
 try:
  with e.connect() as c:check(c.execute(text("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')")).scalar_one()==0)
 finally:e.dispose()
 for proc in Path('/proc').iterdir():
  if proc.name.isdigit():
   try:check((proc/'comm').read_text().strip()!='java')
   except FileNotFoundError:pass
 url='http://'+os.environ['TRINO_HOST']+':'+os.environ['TRINO_INTERNAL_PORT']+'/v1/query'
 with urllib.request.urlopen(urllib.request.Request(url,headers={'X-Trino-User':'ops01-preflight'}),timeout=15) as r:qs=json.load(r)
 check(all(q['state'] in ('FINISHED','FAILED') for q in qs));result={'idle':True,'lease_empty':True}
else:raise ValueError('OPS01_UNKNOWN_OPERATION')
print(json.dumps(result,allow_nan=False))
'''
AGENT = 'RESTORE_KEY=' + repr(RESTORE_KEY) + '\n' + AGENT


class Remote:
    def __init__(self, project='japan-earthquake-etl', context='desktop-linux'):
        require(re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,62}', project) and context in ('desktop-linux', 'default'), 'DOCKER_TARGET')
        self.project = project; self.docker = ['docker', '--context', context]

    def call(self, op, **payload):
        p = subprocess.run([*self.docker, 'exec', '-i', self.project + '-airflow-scheduler-1', 'python3', '-B', '-c', AGENT],
                           input=encode({'op': op, **payload}), capture_output=True, timeout=100)
        if p.returncode != 0:
            if getattr(self, 'private_root', None) is not None:
                private_write(self.private_root / (op + '-' + uuid.uuid4().hex + '.stderr.private'), p.stderr[:1024**2])
            require(False, 'REMOTE_' + op.upper() + '_FAILED_NO_RETRY')
        return decode(p.stdout)

    def get(self, location):
        return base64.b64decode(self.call('get', uri=location)['data'], validate=True)

    def put(self, location, raw):
        restore_uri(location)
        self.call('put', uri=location, data=base64.b64encode(raw).decode())

    def load(self, ns, name):
        return self.call('load', namespace=ns, name=name)

    def query(self, sql):
        return self.call('query', sql=sql)

    def services(self):
        names = [self.project + '-' + s + '-1' for s in ('minio', 'iceberg-rest', 'trino', 'airflow-scheduler', 'airflow-postgres')]
        p = subprocess.run([*self.docker, 'inspect', *names], capture_output=True, timeout=20, check=True)
        result = {}
        for row in decode(p.stdout):
            require(row['State']['Status'] == 'running' and row['State'].get('Health', {}).get('Status') == 'healthy', 'SERVICES_HEALTHY')
            result[row['Name'].lstrip('/')] = {'id': row['Id'], 'started_at': row['State']['StartedAt'], 'restarts': row['RestartCount']}
        return result

    def pg(self, script, raw=None):
        p = subprocess.run([*self.docker, 'exec', '-i', self.project + '-airflow-postgres-1', 'sh', '-eu', '-c',
                            'export PGPASSWORD="$POSTGRES_PASSWORD"; ' + script],
                           input=raw, capture_output=True, timeout=100)
        require(p.returncode == 0 and len(p.stdout) <= MAX_OBJECT, 'AIRFLOW_SCHEMA_OPERATION_FAILED_NO_RETRY')
        return p.stdout

    def pg_signature(self, database=None):
        if database is None: db = '"$POSTGRES_DB"'
        else:
            require(re.fullmatch(r'ops01_[a-f0-9]{32}', database), 'RESTORE_DATABASE_NAME'); db = database
        sql = "SELECT table_name,column_name,data_type,is_nullable,coalesce(column_default,'') FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position"
        return self.pg('psql -X -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d ' + db + ' -c "' + sql + '"')

    def pg_backup(self):
        return self.pg('pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --schema-only --no-owner --no-acl --no-comments --schema=public')

    def pg_restore(self, raw, run):
        require(re.fullmatch(r'[a-f0-9]{32}', run), 'RESTORE_RUN_ID')
        name = 'ops01_' + run
        self.pg('createdb -U "$POSTGRES_USER" --template=template0 ' + name)
        # template0 already contains public. Only this exact idempotent DDL differs.
        data = raw.replace(b'CREATE SCHEMA public;', b'CREATE SCHEMA IF NOT EXISTS public;')
        self.pg('psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d ' + name, data)
        return name


def signature(remote, ns, name, snapshot):
    identifier(ns); identifier(name)
    require(type(snapshot) is int and snapshot > 0, 'SNAPSHOT_INT64')
    meta = remote.load(ns, name)['metadata']
    schema = next(s for s in meta['schemas'] if s['schema-id'] == meta['current-schema-id'])
    columns = ','.join('"' + identifier(f['name']) + '"' for f in schema['fields'])
    rows = remote.query('SELECT count(*), to_hex(checksum(ROW(' + columns + '))) FROM iceberg.' + ns + '.' + name + ' FOR VERSION AS OF ' + str(snapshot))
    require(len(rows) == 1 and type(rows[0][0]) is int and rows[0][0] >= 0, 'TRINO_SIGNATURE')
    return rows[0]


def verify_artifacts(manifest, artifacts):
    """Bind original completion manifest, dataset/export registry and pinned tables."""
    success = decode(artifacts['valid/_SUCCESS.json']); registry = decode(artifacts['registry/dataset.json'])
    export = decode(artifacts['registry/export.json'])
    names = {'memberships.parquet', 'sequence_summary.parquet', 'experiment_metrics.json', 'experiment_config.json', 'requirements-lock.txt'}
    require(success['status'] == 'COMPLETED' and set(success['checksums']) == names, 'COMPLETION_FILE_SET')
    for name, sha in success['checksums'].items():
        require(digest(artifacts['valid/' + name]) == sha, 'RESULT_ARTIFACT_SHA_CHANGED')
    require(success['dataset_id'] == registry['dataset_id'] == export['dataset_id']
            and registry['dataset_status'] == 'EXPORTED' and registry['export_manifest_ref'] == 'export.json'
            and registry['export_manifest_sha256'] == digest(artifacts['registry/export.json']), 'DATASET_EXPORT_LINEAGE')
    pins = {t['name']: t['snapshot_id'] for t in manifest['tables']}
    for field, table in (('candidate_snapshot_id', 'candidate_event'), ('mainshock_snapshot_id', 'mainshock_event'), ('registry_snapshot_id', 'dataset_registry')):
        require(snapshot_id(registry[field]) == snapshot_id(pins[table]), 'DATASET_REGISTRY_SNAPSHOT_PIN')
    import pyarrow.parquet as pq
    for name, field in (('memberships.parquet', 'membership_row_count'), ('sequence_summary.parquet', 'summary_row_count')):
        with pq.ParquetFile(io.BytesIO(artifacts['valid/' + name])) as f:
            require(f.metadata.num_rows == success[field], 'RESULT_ARTIFACT_ROW_COUNT')
    return {'dataset_id': success['dataset_id'], 'experiment_run_id': success['experiment_run_id'],
            'membership_rows': success['membership_row_count'], 'summary_rows': success['summary_row_count']}


def lineage_readback(remote, tables, binding):
    ns = next(t['namespace'] for t in tables if t['name'] == 'experiment_run')
    identifier(ns); prefix = 'iceberg.' + ns + '.'
    checks = {
        'published_experiment': ('SELECT count(*) FROM ' + prefix + 'experiment_run e JOIN ' + prefix + 'dataset_registry d ON e.dataset_id=d.dataset_id WHERE e.experiment_status=\'CANDIDATE\' AND d.dataset_status=\'EXPORTED\'', 1),
        'membership_run': ('SELECT count(*) FROM ' + prefix + 'sequence_membership m JOIN ' + prefix + 'experiment_run e ON m.experiment_run_id=e.experiment_run_id AND m.dataset_id=e.dataset_id', binding['membership_rows']),
        'candidate_keys': ('SELECT count(*) FROM ' + prefix + 'sequence_membership m LEFT JOIN ' + prefix + 'candidate_event c ON m.dataset_id=c.dataset_id AND m.mainshock_event_id=c.mainshock_event_id AND m.candidate_event_id=c.candidate_event_id WHERE c.candidate_event_id IS NULL', 0),
        'summary_run': ('SELECT count(*) FROM ' + prefix + 'sequence_summary s JOIN ' + prefix + 'experiment_run e ON s.experiment_run_id=e.experiment_run_id AND s.dataset_id=e.dataset_id', binding['summary_rows'])}
    result = {}
    for name, (sql, expected) in checks.items():
        rows = remote.query(sql); require(rows == [[expected]], 'RESTORED_ML_LINEAGE_' + name.upper()); result[name] = expected
    return result


def capture(remote, sources, path, artifacts):
    require(0 < len(sources) <= 16 and len(set(sources)) == len(sources), 'SOURCE_TABLE_SET')
    graph = Graph(remote.get); tables = []
    for ns, name in sources:
        identifier(ns); identifier(name)
        loaded = remote.load(ns, name); m = loaded['metadata']; root = m['location']; uri(root)
        raw = graph.add(loaded['metadata-location'], 'metadata', root)
        require(decode(raw) == m, 'REST_METADATA_BYTES_CHANGED')
        sid = m['current-snapshot-id']
        tables.append({'namespace': ns, 'name': name, 'root': root, 'metadata_uri': loaded['metadata-location'],
                       'uuid': m['table-uuid'], 'snapshot_id': sid, 'signature': signature(remote, ns, name, sid)})
        require(remote.load(ns, name)['metadata-location'] == loaded['metadata-location'], 'SOURCE_CHANGED_DURING_BACKUP')
    return make_bundle(path, tables, graph, artifacts)


def restore(remote, path, trusted_sha, run, folder):
    manifest, graph, artifacts = read_bundle(path, trusted_sha)
    tables, objects = relocated(manifest, graph, run)
    for location in objects: restore_uri(location)
    closure = Graph(objects.__getitem__)
    for table in tables: closure.add(table['metadata_uri'], 'metadata', table['root'])
    require(set(closure.objects) == set(objects), 'RESTORED_GRAPH_NOT_SELF_CONTAINED')
    # Verify ALL source pins and ALL local plans before the first remote write.
    for before in manifest['tables']:
        require(remote.load(before['namespace'], before['name'])['metadata-location'] == before['metadata_uri'], 'SOURCE_PIN_DRIFT')
    private_write(folder / 'restore-intent.json', encode({'version': VERSION, 'run': run, 'bundle_sha256': trusted_sha,
                    'tables': tables, 'objects': [{'uri': u, 'sha256': digest(raw)} for u, raw in sorted(objects.items())]}))
    artifact_root = safe(folder / 'artifacts'); artifact_root.mkdir(mode=0o700)
    for name, raw in artifacts.items():
        dest = safe(artifact_root / name); require(dest.is_relative_to(artifact_root), 'ARTIFACT_PATH')
        dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True); private_write(dest, raw)
        require(digest(dest.read_bytes()) == digest(raw), 'RESTORED_ARTIFACT_READBACK')
    for key, raw in sorted(objects.items()):
        remote.put(key, raw)
        require(digest(remote.get(key)) == digest(raw), 'RESTORED_OBJECT_READBACK')
    for ns in sorted({t['namespace'] for t in tables}): remote.call('namespace', namespace=ns)
    result = []
    for t in tables:
        remote.call('register', namespace=t['namespace'], name=t['name'], metadata_uri=t['metadata_uri'])
        require(signature(remote, t['namespace'], t['name'], t['snapshot_id']) == t['signature'], 'RESTORED_QUERY_CHANGED')
        state = remote.load(t['namespace'], t['name'])
        require(state['metadata']['table-uuid'] == t['uuid'] and state['metadata']['current-snapshot-id'] == t['snapshot_id'], 'RESTORED_SNAPSHOT_CHANGED')
        result.append({'source': t['source_namespace'] + '.' + t['name'], 'restored': t['namespace'] + '.' + t['name'],
                       'snapshot_id': t['snapshot_id'], 'rows': t['signature'][0], 'checksum': t['signature'][1]})
    # Read twice: immutable rerun/readback, not duplicate registration or writes.
    for t in tables:
        require(signature(remote, t['namespace'], t['name'], t['snapshot_id']) == t['signature'], 'RERUN_QUERY_CHANGED')
    for original in manifest['tables']:
        require(remote.load(original['namespace'], original['name'])['metadata-location'] == original['metadata_uri'], 'SOURCE_CHANGED_AFTER_RESTORE')
    return {'tables': result, 'objects': len(objects), 'bytes': sum(map(len, objects.values())),
            'restored_namespaces': sorted({t['namespace'] for t in tables}), 'artifact_names': sorted(artifacts),
            'original_pins_unchanged': True, 'rerun_readback_unchanged': True}
