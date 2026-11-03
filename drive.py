#!/usr/bin/env python3
# Several long-lived BEAMs, one block each per round, the order rotating every round, so every
# pair is measured inside one run. ARMS="name:decree_path:mode:build,...". PAIRS="a-b,..." names
# what to subtract, each within its round: b's block minus a's.
#
# The trace samples the 1-minute load and the busiest processes at the start, every 50 counted
# rounds and at the end. Void (sparklespace#7627, coordinator ruling 2026-11-03): a BEAM or `mix`
# outside the rig, a container other than the rig's own above 10% CPU, or a 1-minute load above
# 15; the run stops at the first. OS and desktop processes never void a run; the trace lists them.
# A foreign BEAM or `mix` before the arms start refuses the run. The rig's own processes are this
# driver and its descendants (the arms).
#
# Every run that starts the arms writes its raw per-round blocks to gate-rows.json, void or not;
# the results block prints numbers only on a VALID verdict.
import datetime
import hashlib
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys

rounds = int(os.environ.get('BLOCKS', '401'))
arms = [a.split(':') for a in os.environ['ARMS'].split(',')]
pairs = [p.split('-') for p in os.environ['PAIRS'].split(',')]
home = os.environ['GATE_HOME']
work = os.environ['WORK']
container = os.environ.get('GATE_PG_CONTAINER', '')
rows_path = os.path.join(home, 'gate-rows.json')
FLAGS = '+sbwt none +sbwtdcpu none +sbwtdio none'
THRESHOLD_US = 500.0
CPU_LIMIT = 10.0
LOAD_LIMIT = 15.0


def start(name, path, mode, build):
    env = dict(os.environ)
    env.update(DECREE_PATH=path, GATE_BUILD=work + '/build-' + build, BENCH_ARM=name,
               BENCH_MODE=mode, MIX_ENV='prod', ELIXIR_ERL_OPTIONS=FLAGS,
               PATH=os.environ['GATE_PATH'])
    p = subprocess.Popen([os.environ['GATE_MIX'], 'run', '--no-compile', '--no-deps-check', '--no-start', 'server.exs'],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env, bufsize=1,
                         cwd=home)
    while True:
        line = p.stdout.readline()
        if not line:
            sys.exit('arm died ' + name)
        if line.startswith('READY'):
            return p


def block(name, p):
    p.stdin.write('go\n')
    p.stdin.flush()
    while True:
        line = p.stdout.readline()
        if line.startswith('BLOCK'):
            return float(line.split()[1])
        if not line:
            sys.exit('arm died in a block: ' + name)


def pids(pattern):
    out = subprocess.run(['pgrep', '-f', pattern], capture_output=True, text=True).stdout.split()
    return set(out)


def top_processes():
    """(pid, cpu %, command) for the busiest processes over one second."""
    if platform.system() == 'Darwin':
        out = subprocess.run(['top', '-l', '2', '-s', '1', '-n', '15', '-o', 'cpu',
                              '-stats', 'pid,cpu,command'], capture_output=True, text=True).stdout
        chunk = out.split('PID')[-1].splitlines()[1:]
        rows = []
        for line in chunk:
            parts = line.split(None, 2)
            if len(parts) == 3:
                try:
                    rows.append((parts[0], float(parts[1]), parts[2].strip()))
                except ValueError:
                    pass
        return rows
    lines = subprocess.run(['top', '-b', '-n', '2', '-d', '1', '-w', '512'],
                           capture_output=True, text=True).stdout.splitlines()
    heads = [i for i, line in enumerate(lines) if line.strip().startswith('PID')]
    chunk = lines[heads[-1] + 1:] if heads else []
    rows = []
    for line in chunk:
        parts = line.split(None, 11)
        if len(parts) == 12:
            try:
                rows.append((parts[0], float(parts[8].replace(',', '.')), parts[11].strip()))
            except ValueError:
                pass
    rows.sort(key=lambda r: -r[1])
    return rows[:15]


def containers():
    if not shutil.which('docker'):
        return []
    out = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{.Name}} {{.CPUPerc}}'],
                         capture_output=True, text=True).stdout.splitlines()
    rows = []
    for line in out:
        parts = line.split()
        if len(parts) == 2:
            try:
                rows.append((parts[0], float(parts[1].rstrip('%'))))
            except ValueError:
                pass
    return rows


def descendants(roots):
    """The roots and every process below them, from one `ps` listing."""
    out = subprocess.run(['ps', '-A', '-o', 'pid=,ppid='], capture_output=True, text=True).stdout
    children = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2:
            children.setdefault(parts[1], []).append(parts[0])
    found, todo = set(), [str(r) for r in roots]
    while todo:
        pid = todo.pop()
        if pid not in found:
            found.add(pid)
            todo.extend(children.get(pid, []))
    return found


def own():
    # The rig is this driver and everything it started: the arms are its children, and theirs.
    return descendants([os.getpid()])


def foreign_beams():
    return sorted((pids(r'beam\.smp') | pids(r'(^|/)mix( |$)')) - own(), key=int)


def command_of(pid):
    return subprocess.run(['ps', '-o', 'command=', '-p', pid], capture_output=True, text=True).stdout.strip()


# The start rule's last check, before any arm exists: a BEAM or `mix` here is someone else's.
early = foreign_beams()
if early:
    print('START RULE NOT MET: foreign BEAM', flush=True)
    for pid in early:
        print('  pid %s: %s' % (pid, command_of(pid)), flush=True)
    sys.exit(2)

names = [a[0] for a in arms]
procs = {a[0]: start(*a) for a in arms}
print('beam.smp at start', len(pids('beam.smp')), 'for', len(procs), 'arms', flush=True)


def sample(label):
    load = os.getloadavg()[0]
    mine = own()
    void = []
    for pid in foreign_beams():
        void.append('BEAM or mix pid %s: %s' % (pid, command_of(pid)[:100]))
    boxes = containers()
    for name, cpu in boxes:
        if name != container and cpu > CPU_LIMIT:
            void.append('container %s %.1f%%' % (name, cpu))
    if load > LOAD_LIMIT:
        void.append('load1 %.2f' % load)
    busy = top_processes()
    # Listed, never void: OS, desktop, Postgres and Docker's VM all land here.
    others = ', '.join('%s %.1f%% %s' % (pid, cpu, c[:40]) for pid, cpu, c in busy
                       if cpu > CPU_LIMIT and pid not in mine) or 'none'
    top5 = ', '.join('%s %.0f%% %s' % (pid, cpu, os.path.basename(c.split()[0])[:24]) for pid, cpu, c in busy[:5])
    box = ', '.join('%s %.1f%%' % b for b in boxes) or 'no docker'
    print('TRACE %-10s load1 %5.2f | void: %s | others >10%% (not void): %s | top: %s | containers: %s' %
          (label, load, '; '.join(void) or 'none', others, top5, box), flush=True)
    trace_log.append({'label': label, 'load1': load, 'void': void, 'others': others, 'top': top5,
                      'containers': box})
    return void


def write_rows(rows, verdict):
    """The raw blocks, void or not. An earlier file is kept under a new name, never overwritten."""
    if os.path.exists(rows_path):
        stamp = datetime.datetime.fromtimestamp(os.path.getmtime(rows_path)).strftime('%Y%m%d-%H%M%S')
        os.rename(rows_path, os.path.join(home, 'gate-rows.%s.json' % stamp))
    doc = {'main': os.environ.get('MAIN_SHA'), 'branch': os.environ.get('BRANCH_SHA'),
           'arms': names, 'pairs': ['-'.join(p) for p in pairs], 'block_n': os.environ.get('BENCH_N'),
           'smoke': os.environ.get('SMOKE', '0') == '1', 'verdict': verdict, 'trace': trace_log,
           'rows': rows}
    data = json.dumps(doc, indent=1).encode()
    with open(rows_path, 'wb') as f:
        f.write(data)
    print('rows written: %s, %d rounds, sha256 %s' % (rows_path, len(rows), hashlib.sha256(data).hexdigest()),
          flush=True)


trace_log = []
rows = []
verdict = 'INCOMPLETE'
try:
    for _ in range(3):
        for name in names:
            block(name, procs[name])

    interfered = sample('start')
    # A void condition voids the whole run, so the run stops at the first one: measuring on would
    # report nothing.
    stopped_early = bool(interfered)
    for i in range(rounds):
        if stopped_early:
            break
        k = i % len(names)
        order = names[k:] + names[:k]
        rows.append({'round': i + 1, 'order': order, 'us': {name: block(name, procs[name]) for name in order}})
        if (i + 1) % 50 == 0:
            interfered = sample('round %d' % (i + 1))
            stopped_early = bool(interfered)
    if not stopped_early:
        interfered = sample('end')
    void = bool(interfered)
    verdict = 'VOID' if void else 'VALID'
finally:
    for p in procs.values():
        p.terminate()
    write_rows(rows, verdict)

peak = max(t['load1'] for t in trace_log)


def stat(values):
    s = sorted(values)
    n = len(s)
    return statistics.median(s), s[n // 10], s[n * 9 // 10]


print('=== RESULTS (paste everything from here to END RESULTS)')
print('verdict', verdict, '(stopped at the first void sample)' if stopped_early else '', '| load1 max %.2f (limit %.0f)' % (peak, LOAD_LIMIT),
      '| void', interfered if interfered else 'none')
if void:
    print('VOID: per the rule, no numbers are reported from this run.')
else:
    for name in names:
        print('level', name, round(statistics.median(r['us'][name] for r in rows), 1), 'us')
    half = len(rows) // 2
    gate = {'dec_main-dec_fw6': 'Q2', 'rcold_main-rcold_fw6': 'Q3'}
    for a, b in pairs:
        d = [r['us'][b] - r['us'][a] for r in rows]
        m, p10, p90 = stat(d)
        h1 = statistics.median(d[:half])
        h2 = statistics.median(d[half:])
        label = gate.get(a + '-' + b, 'pair')
        result = 'TRIPPED' if m > THRESHOLD_US else 'passes'
        print(f'{label} {b} minus {a}: median {m:.1f} us, halves {h1:.1f} {h2:.1f}, '
              f'p10 {p10:.1f}, p90 {p90:.1f}, rounds {len(d)}: {result} (threshold +{THRESHOLD_US:.0f} us)')
print('=== END RESULTS')
