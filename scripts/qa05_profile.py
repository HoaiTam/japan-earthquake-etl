"""QA-05 bounded SELECT profiling, not ETL/training or a deployment mutation."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import threading
import time

from ops_backup import Remote, decode, encode, safe, snapshot_id

VERSION = 'qa05-resource-profile-v1'
TABLES = {'event_current', 'event_source_bridge', 'dim_date', 'dim_region', 'dim_depth_band', 'dim_magnitude_band'}
SERVICES = ('minio', 'iceberg-rest', 'trino', 'airflow-scheduler', 'airflow-postgres')
RESERVE = 4 * 1024**3


def require(ok, reason):
    if not ok:
        raise ValueError('QA05_' + reason)


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def save(path, value):
    with safe(path).open('xb') as stream:
        os.chmod(path, 0o600); stream.write(encode(value) + b'\n')


def configuration(root, path):
    root, path = safe(root), safe(path)
    require(path.is_relative_to(root / 'config/qa') and path.stat().st_size <= 16384, 'CONFIG_PATH')
    raw = path.read_bytes(); c = decode(raw)
    require(set(c) == {'version', 'evidence', 'evidence_sha256', 'query_window_seconds', 'suite_window_seconds', 'repetitions', 'profiles'}
            and c['version'] == VERSION, 'CONFIG_SCHEMA')
    require(c['query_window_seconds'] == 60 and type(c['query_window_seconds']) is int
            and c['suite_window_seconds'] == 600 and type(c['suite_window_seconds']) is int
            and type(c['repetitions']) is int and c['repetitions'] == 2, 'FIXED_BUDGET')
    require(c['profiles'] == [{'name': 'baseline', 'task_concurrency': 1}, {'name': 'candidate', 'task_concurrency': 2}]
            and all(type(p['task_concurrency']) is int for p in c['profiles']), 'SESSION_PROFILES')
    require(c['evidence'] == 'docs/evidence/DAT-03_PORTABLE_GOLD_20261115.json'
            and re.fullmatch(r'[a-f0-9]{64}', c['evidence_sha256']), 'EVIDENCE_PATH')
    evidence_raw = safe(root / c['evidence']).read_bytes()
    require(hashlib.sha256(evidence_raw).hexdigest() == c['evidence_sha256'], 'EVIDENCE_SHA')
    ev = decode(evidence_raw)
    require(ev['part2_status'] == 'ACCEPTED_PUBLISHED_IMMUTABLE_RERUN'
            and ev['first_publication']['status'] == 'PUBLISHED'
            and ev['namespace'] == 'iceberg.dat03_unified_20261114'
            and ev['months'] == 288 and set(ev['tables']) == TABLES, 'PUBLISHED_VOLUME')
    for pin in ev['tables'].values():
        snapshot_id(pin['snapshot_id'])
        require(type(pin['count']) is int and pin['count'] > 0, 'EXPECTED_COUNT')
    return c, ev, hashlib.sha256(raw).hexdigest()


def workload(ev):
    fact = ev['namespace'] + '.event_current FOR VERSION AS OF ' + str(snapshot_id(ev['tables']['event_current']['snapshot_id']))
    return {
        'fact_summary': 'SELECT count(*), count(magnitude), sum(latitude), min(depth_km), max(magnitude) FROM ' + fact,
        'monthly': 'SELECT year(event_time_utc), month(event_time_utc), count(*), sum(latitude) FROM ' + fact
                   + ' GROUP BY 1,2 ORDER BY 1,2',
        'bands': 'SELECT magnitude_band_code, depth_band_code, count(*), sum(latitude) FROM ' + fact
                 + ' GROUP BY 1,2 ORDER BY 1,2',
    }


def result_signature(rows):
    # Aggregations are sorted in SQL. Floating sums may differ with concurrency;
    # normalize only numeric aggregates, not IDs/counts, to 7 significant digits.
    def normalize(x):
        if type(x) is float:
            require(math.isfinite(x), 'NONFINITE_RESULT')
            return float(format(x, '.7g'))
        if isinstance(x, list): return [normalize(v) for v in x]
        return x
    return digest(normalize(rows))


def byte_size(value):
    match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)\s*(B|kB|KB|MB|GB|KiB|MiB|GiB)', value)
    require(match is not None, 'DOCKER_MEMORY_UNIT')
    scales = {'B': 1, 'kB': 1000, 'KB': 1000, 'MB': 1000**2, 'GB': 1000**3,
              'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
    return int(float(match[1]) * scales[match[2]])


QUERY_AGENT = r'''
import json,os,re,sys,time,urllib.parse,urllib.request
p=json.load(sys.stdin)
def check(ok):
 if not ok:raise ValueError('QA05_REMOTE_GUARD')
sql=p['sql'];check(sql.startswith('SELECT ') and ';' not in sql and p['concurrency'] in (1,2))
check(not any(w in sql.upper().split() for w in ('INSERT','DELETE','DROP','ALTER','CREATE','UPDATE','CALL')))
base='http://'+os.environ['TRINO_HOST']+':'+os.environ['TRINO_INTERNAL_PORT']
headers={'X-Trino-User':'qa05-resource-profile','X-Trino-Session':'query_max_run_time=60s,query_max_memory=256MB,task_concurrency='+str(p['concurrency'])}
def request(req):
 with urllib.request.urlopen(req,timeout=20) as response:
  raw=response.read(2*1024**2+1);check(len(raw)<=2*1024**2);return json.loads(raw)
started=time.monotonic();req=urllib.request.Request(base+'/v1/statement',data=sql.encode(),headers=headers)
rows=[];query_id=None
while True:
 check(time.monotonic()-started<80)
 d=request(req);query_id=d['id'];check('error' not in d);rows.extend(d.get('data',[]));check(len(rows)<=1024)
 if not d.get('nextUri'):break
 url=d['nextUri'];check(url.startswith(base+'/v1/statement/') and urllib.parse.urlsplit(url).netloc==urllib.parse.urlsplit(base).netloc)
 time.sleep(.1);req=urllib.request.Request(url,headers=headers)
info=request(urllib.request.Request(base+'/v1/query/'+query_id,headers=headers));check(info['state']=='FINISHED')
stats=info['queryStats']
keys=('elapsedTime','executionTime','totalCpuTime','peakUserMemoryReservation','peakTotalMemoryReservation','physicalInputDataSize','physicalInputPositions','spilledDataSize')
check(all(k in stats for k in keys))
print(json.dumps({'query_id':query_id,'wall_seconds':time.monotonic()-started,'state':info['state'],'rows':rows,'engine':{k:stats[k] for k in keys}},allow_nan=False))
'''


class Runtime(Remote):
    def query_profile(self, sql, concurrency):
        p = subprocess.run([*self.docker, 'exec', '-i', self.project + '-airflow-scheduler-1', 'python3', '-B', '-c', QUERY_AGENT],
                           input=encode({'sql': sql, 'concurrency': concurrency}), capture_output=True, timeout=100)
        require(p.returncode == 0, 'QUERY_FAILED_NO_RETRY')
        result = decode(p.stdout)
        require(result['state'] == 'FINISHED' and result['wall_seconds'] <= 60, 'QUERY_WINDOW')
        return result

    def metrics(self):
        names = [self.project + '-' + s + '-1' for s in SERVICES]
        p = subprocess.run([*self.docker, 'stats', '--no-stream', '--format', '{{json .}}', *names],
                           capture_output=True, check=True, timeout=20)
        rows = [decode(line) for line in p.stdout.splitlines() if line]
        require({r['Name'] for r in rows} == set(names) and len(rows) == 5, 'METRIC_COVERAGE')
        return {r['Name']: {'memory_bytes': byte_size(r['MemUsage'].split('/')[0].strip()),
                           'cpu_percent': float(r['CPUPerc'].rstrip('%'))} for r in rows}

    def oom_counters(self):
        result = {}
        for service in SERVICES:
            name = self.project + '-' + service + '-1'
            p = subprocess.run([*self.docker, 'exec', name, 'cat', '/sys/fs/cgroup/memory.events'],
                               capture_output=True, check=True, timeout=10)
            counters = dict(line.split() for line in p.stdout.decode().splitlines())
            require({'oom', 'oom_kill'} <= set(counters), 'CGROUP_OOM_COVERAGE')
            result[service] = {k: int(counters[k]) for k in ('oom', 'oom_kill')}
        return result


def table_identity(remote, ev):
    out = {}
    for name, pin in sorted(ev['tables'].items()):
        loaded = remote.load(ev['namespace'].split('.')[1], name); meta = loaded['metadata']
        require(snapshot_id(meta['current-snapshot-id']) == snapshot_id(pin['snapshot_id']), 'CURRENT_SNAPSHOT_CHANGED')
        out[name] = {'metadata_uri_sha256': hashlib.sha256(loaded['metadata-location'].encode()).hexdigest(),
                     'metadata_sha256': digest(meta), 'snapshot_id': str(snapshot_id(pin['snapshot_id']))}
    return out


def file_profile(remote, ev):
    out = {}
    for name, pin in sorted(ev['tables'].items()):
        # Metadata system tables describe CURRENT. Require exact current pins
        # and metadata identity before/after the entire suite, not time travel on
        # system tables that older Trino releases do not support.
        table = ev['namespace'] + '."' + name + '$files"'
        sql = ('SELECT count(*), sum(record_count), sum(file_size_in_bytes), min(file_size_in_bytes), max(file_size_in_bytes), '
               'count_if(file_size_in_bytes < 1048576), count_if(content <> 0) FROM ' + table)
        rows = remote.query(sql)
        require(len(rows) == 1 and len(rows[0]) == 7, 'FILES_SHAPE')
        files, records, size, minimum, maximum, small, deletes = rows[0]
        require(type(files) is int and files > 0 and records == pin['count'] and deletes == 0
                and type(size) is int and 0 < minimum <= maximum <= size and 0 <= small <= files, 'FILES_RECONCILIATION')
        partitions = remote.query('SELECT count(*) FROM ' + ev['namespace'] + '."' + name + '$partitions"')
        require(len(partitions) == 1 and len(partitions[0]) == 1 and type(partitions[0][0]) is int
                and 0 < partitions[0][0] <= files, 'PARTITION_SHAPE')
        out[name] = {'files': files, 'records': records, 'bytes': size, 'min_file_bytes': minimum,
                     'max_file_bytes': maximum, 'under_1mib_files': small, 'partitions': partitions[0][0]}
    return out


class Monitor:
    def __init__(self, remote, root):
        self.remote = remote; self.root = root; self.samples = []; self.failed = False
        self.stop = threading.Event(); self.thread = threading.Thread(target=self.loop, daemon=True)

    def sample(self):
        free = shutil.disk_usage(self.root).free
        require(free >= RESERVE, 'HOST_DISK_RESERVE_4GIB')
        self.samples.append({'elapsed_seconds': round(time.monotonic() - self.started, 3),
                             'host_free_bytes': free, 'services': self.remote.metrics()})

    def loop(self):
        while not self.stop.wait(5):
            try: self.sample()
            except Exception:
                self.failed = True; return

    def start(self):
        self.started = time.monotonic(); self.sample(); self.thread.start()

    def finish(self):
        self.stop.set()
        if self.thread.ident is not None: self.thread.join(timeout=25)
        require(not self.thread.is_alive() and not self.failed, 'METRIC_MONITOR_FAILED')
        self.sample()


def comparison(queries):
    out = {}
    for name in ('fact_summary', 'monthly', 'bands'):
        records = [q for q in queries if not q['warmup'] and q['workload'] == name]
        require(len(records) == 4 and len({q['result_sha256'] for q in records}) == 1, 'REPEATED_RESULT_PARITY')
        medians = {}
        for profile in ('baseline', 'candidate'):
            selected = [q['wall_seconds'] for q in records if q['profile'] == profile]
            require(len(selected) == 2 and all(0 < n <= 60 for n in selected), 'PROFILE_COVERAGE')
            medians[profile] = statistics.median(selected)
        out[name] = {'median_seconds': medians, 'candidate_vs_baseline_ratio': medians['candidate'] / medians['baseline'],
                     'result_parity': True}
    return out


def run(root, config_path, output, remote):
    root, output = safe(root), safe(output)
    require(output.is_relative_to(root / 'staging') and output != root / 'staging' and not output.exists(), 'NEW_STAGING_OUTPUT')
    require(shutil.disk_usage(root).free >= RESERVE, 'HOST_DISK_RESERVE_4GIB')
    c, ev, config_sha = configuration(root, config_path)
    before = remote.services(); remote.call('idle')
    oom_before = remote.oom_counters(); identity = table_identity(remote, ev)
    output.mkdir(mode=0o700, parents=True)
    save(output / 'intent.json', {'version': VERSION, 'config_sha256': config_sha, 'evidence_sha256': c['evidence_sha256'],
                                'pins': identity, 'query_window_seconds': 60, 'suite_window_seconds': 600})
    monitor = Monitor(remote, root); queries = []; started = time.monotonic()
    try:
        monitor.start()
        files = file_profile(remote, ev)
        sqls = workload(ev)
        # Warm-up once per session profile, then alternate AB/BA ordering.
        schedule = [(True, p, 'fact_summary') for p in c['profiles']]
        schedule += [(False, p, name) for round_no in range(2) for name in sqls
                     for p in (c['profiles'] if round_no == 0 else reversed(c['profiles']))]
        for index, (warmup, profile, name) in enumerate(schedule, 1):
            require(not monitor.failed and shutil.disk_usage(root).free >= RESERVE, 'RESOURCE_MONITOR_OR_DISK')
            require(time.monotonic() - started < 500, 'SUITE_NO_NEW_QUERY_NEAR_DEADLINE')
            remote.call('idle')
            result = remote.query_profile(sqls[name], profile['task_concurrency'])
            rows = result.pop('rows')
            count = rows[0][0] if name == 'fact_summary' else sum(r[2] for r in rows)
            require(count == ev['tables']['event_current']['count'], 'WORKLOAD_COUNT')
            result.update(workload=name, profile=profile['name'], warmup=warmup, result_sha256=result_signature(rows))
            queries.append(result); save(output / ('query-' + str(index).zfill(2) + '.json'), result)
            print('event=qa05_query_finished query=' + str(index) + '/' + str(len(schedule))
                  + ' profile=' + profile['name'] + ' workload=' + name, flush=True)
        require(identity == table_identity(remote, ev), 'METADATA_OR_SNAPSHOT_CHANGED')
        require(before == remote.services() and oom_before == remote.oom_counters(), 'SERVICE_OR_OOM_CHANGED')
        remote.call('idle'); monitor.finish()
        elapsed = time.monotonic() - started
        require(elapsed <= c['suite_window_seconds'], 'SUITE_WINDOW')
        report = {'version': VERSION, 'status': 'PROFILE_ACCEPTED', 'task_id': 'QA-05', 'config_sha256': config_sha,
                  'evidence_sha256': c['evidence_sha256'], 'namespace': ev['namespace'], 'months': 288,
                  'tables': files, 'pins': identity, 'queries': queries, 'comparison': comparison(queries),
                  'samples': monitor.samples, 'whole_suite_seconds': elapsed, 'services_unchanged': True,
                  'oom_counters_unchanged': True, 'metadata_unchanged': True, 'runtime_idle_at_end': True,
                  'host_free_min_bytes': min(s['host_free_bytes'] for s in monitor.samples),
                  'configuration_scope': 'session_only_task_concurrency_1_vs_2_no_service_changes',
                  'lake_writes': False, 'scientific_approval': False, 'etl_rerun': False}
        save(output / 'receipt.json', report)
        return report
    except BaseException:
        monitor.stop.set()
        if monitor.thread.is_alive(): monitor.thread.join(timeout=25)
        save(output / 'failed.json', {'version': VERSION, 'status': 'FAILED_RETAINED_NO_RETRY',
                                     'completed_queries': len(queries), 'samples': monitor.samples})
        raise
