#!/usr/bin/env python3
# Several long-lived BEAMs, one block each per round, the order rotating every round, so every
# pair is measured inside one run. ARMS="name:decree_path:mode:build,...". PAIRS="a-b,..." names
# what to subtract, each within its round: b's block minus a's.
#
# The trace samples the 1-minute load and the busiest processes at the start, every 50 counted
# rounds and at the end. Void (sparklespace#7627, coordinator rule): any process outside the rig
# above 10% CPU, or a 1-minute load above 15. The rig is its own BEAMs, this driver, Postgres
# (native or in Docker) and the sampling tools.
import os
import platform
import re
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
FLAGS = '+sbwt none +sbwtdcpu none +sbwtdio none'
THRESHOLD_US = 500.0
CPU_LIMIT = 10.0
LOAD_LIMIT = 15.0
# Process names that belong to the rig when they are not the rig's own pids: the database server
# and the container runtime around it, and the tools this script runs to sample.
RIG_NAMES = re.compile(
    r'postgres|postmaster|com\.docker|docker|qemu|vpnkit|virtualization|com\.apple\.virtua|containerd|'
    r'^top$|^ps$|pgrep', re.I)


def start(name, path, mode, build):
    env = dict(os.environ)
    env.update(DECREE_PATH=path, GATE_BUILD=work + '/build-' + build, BENCH_ARM=name,
               BENCH_MODE=mode, MIX_ENV='prod', ELIXIR_ERL_OPTIONS=FLAGS)
    p = subprocess.Popen(['mix', 'run', '--no-compile', '--no-deps-check', '--no-start', 'server.exs'],
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


names = [a[0] for a in arms]
procs = {a[0]: start(*a) for a in arms}
own = pids('beam.smp') | {str(os.getpid())} | {str(p.pid) for p in procs.values()}
print('beam.smp at start', len(pids('beam.smp')), 'for', len(procs), 'arms', flush=True)
extra_at_start = ['another BEAM running when the arms started'] if len(pids('beam.smp')) != len(procs) else []


def sample(label):
    load = os.getloadavg()[0]
    foreign = []
    for pattern in ['beam.smp', 'mix ']:
        extra = pids(pattern) - own
        if extra:
            foreign.append(pattern.strip() + ' pids ' + ','.join(sorted(extra)))
    busy = top_processes()
    for pid, cpu, command in busy:
        if cpu > CPU_LIMIT and pid not in own and not RIG_NAMES.search(os.path.basename(command.split()[0])):
            foreign.append('process %s %.1f%% %s' % (pid, cpu, command[:60]))
    boxes = containers()
    for name, cpu in boxes:
        if name != container and cpu > CPU_LIMIT:
            foreign.append('container %s %.1f%%' % (name, cpu))
    top5 = ', '.join('%s %.0f%% %s' % (pid, cpu, os.path.basename(c.split()[0])[:24]) for pid, cpu, c in busy[:5])
    box = ', '.join('%s %.1f%%' % b for b in boxes) or 'no docker'
    print('TRACE %-10s load1 %5.2f | foreign: %s | top: %s | containers: %s' %
          (label, load, '; '.join(foreign) or 'none', top5, box), flush=True)
    return load, foreign


for _ in range(3):
    for name in names:
        block(name, procs[name])

rows = []
trace = [sample('start')]
trace[0] = (trace[0][0], trace[0][1] + extra_at_start)
for i in range(rounds):
    k = i % len(names)
    order = names[k:] + names[:k]
    rows.append({name: block(name, procs[name]) for name in order})
    if (i + 1) % 50 == 0:
        trace.append(sample('round %d' % (i + 1)))
trace.append(sample('end'))

peak = max(load for load, _ in trace)
interfered = [f for _, fs in trace for f in fs]
void = peak > LOAD_LIMIT or bool(interfered)


def stat(values):
    s = sorted(values)
    n = len(s)
    return statistics.median(s), s[n // 10], s[n * 9 // 10]


print('=== RESULTS (paste everything from here to END RESULTS)')
print('verdict', 'VOID' if void else 'VALID', '| load1 max %.2f (limit %.0f)' % (peak, LOAD_LIMIT),
      '| foreign', interfered if interfered else 'none')
if void:
    print('VOID: per the rule, no numbers are reported from this run.')
else:
    for name in names:
        print('level', name, round(statistics.median(r[name] for r in rows), 1), 'us')
    half = len(rows) // 2
    gate = {'dec_main-dec_fw6': 'Q2', 'rcold_main-rcold_fw6': 'Q3'}
    for a, b in pairs:
        d = [r[b] - r[a] for r in rows]
        m, p10, p90 = stat(d)
        h1 = statistics.median(d[:half])
        h2 = statistics.median(d[half:])
        label = gate.get(a + '-' + b, 'pair')
        result = 'TRIPPED' if m > THRESHOLD_US else 'passes'
        print(f'{label} {b} minus {a}: median {m:.1f} us, halves {h1:.1f} {h2:.1f}, '
              f'p10 {p10:.1f}, p90 {p90:.1f}, rounds {len(d)}: {result} (threshold +{THRESHOLD_US:.0f} us)')
print('=== END RESULTS')
