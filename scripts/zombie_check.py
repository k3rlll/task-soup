#!/usr/bin/env python3
"""Real SIGSTOP -> lease expiry -> replacement -> SIGCONT scenario; disposable DB only."""
import json
import os
import signal
import subprocess
import time
import urllib.request

base = 'http://127.0.0.1:18081'
env = dict(os.environ, HTTP_ADDR='127.0.0.1:18081', LEASE_DURATION='2s', MAX_ATTEMPTS='10')
if not env.get('DATABASE_URL') or not env.get('ZOMBIE_DB'):
    raise SystemExit('DATABASE_URL and ZOMBIE_DB (empty disposable DB name) required')
subprocess.run(['bin/soup', 'migrate'], env=env, check=True)
children = []

def spawn(mode):
    child = subprocess.Popen(['bin/soup', mode], env=env)
    children.append(child)
    return child

def req(method, path, payload=None):
    headers = {'Content-Type': 'application/json', 'Idempotency-Key': 'real-zombie'}
    data = json.dumps(payload).encode() if payload is not None else None
    with urllib.request.urlopen(urllib.request.Request(base+path, data=data, headers=headers, method=method), timeout=3) as r:
        value = json.load(r)
        print(method, path, value, flush=True)
        return value

def wait_for(path, predicate, seconds=15):
    deadline = time.monotonic()+seconds
    while time.monotonic()<deadline:
        j = req('GET',path)
        if predicate(j): return j
        time.sleep(.1)
    raise AssertionError('timeout '+path)

try:
    spawn('api')
    for _ in range(100):
        try:
            req('GET','/healthz')
            break
        except OSError:
            time.sleep(.05)
    old = spawn('worker')
    j = req('POST','/jobs',{'duration_ms':5000})
    path = '/jobs/'+str(j['id'])
    j = wait_for(path,lambda j:j['state']=='running')
    old_token = j['token']
    print('SIGSTOP',old.pid,flush=True)
    old.send_signal(signal.SIGSTOP)
    time.sleep(2.3)
    spawn('worker')
    wait_for(path,lambda j:j['token']>old_token)
    print('SIGCONT',old.pid,flush=True)
    old.send_signal(signal.SIGCONT)
    j = wait_for(path,lambda j:j['state'] in ('succeeded','failed','cancelled'))
    assert j['state']=='succeeded' and j['attempts']==2,j
    sql=f"SELECT count(*) FROM job_effects WHERE job_id={j['id']} AND token={j['token']}"
    output=subprocess.check_output(['docker','compose','exec','-T','db','psql','-U','soup','-d',env['ZOMBIE_DB'],'-Atc',sql],text=True).strip()
    assert output=='1',output
    print('PASS: real zombie fenced, replacement succeeded with heartbeat, exactly one effect',flush=True)
finally:
    for child in children:
        if child.poll() is None:
            child.send_signal(signal.SIGCONT)
            child.kill()
        child.wait()
