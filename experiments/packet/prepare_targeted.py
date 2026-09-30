#!/usr/bin/env python3
"""Declare a focused runtime instrumentation study before its measurements."""
import argparse,csv,hashlib,json,random
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
CODE=Path(__file__).resolve().parent
CONFIG=ROOT/'data/packet/config'
POLICIES=['fixed_h','predictive_h','gar_h','residual_priority_h']
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,d):p.write_text(json.dumps(d,indent=2)+'\n')
def csvwrite(p,rows):
    with p.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
def traffic(seed,duration,probe_size=None):
    rng=random.Random(seed);rows=[]
    def add(source,k,kind,size,period,D,copies=1):
        phase=rng.randrange(period)
        for t in range(phase,duration,period):
            for n in range(copies):rows.append(dict(arrival_ms=t,class_id=k,size_bytes=size,deadline_ms=t+D,source=source,id=f'{t}_{n}',type_id=kind))
    if probe_size:
        add('saturated-critical',0,0,probe_size,1,100,(1300+probe_size-1)//probe_size)
        add('saturated-video',1,6,1000,1,100)
        add('saturated-monitor',2,4,128,1,100,8)
    else:
        for spec in [('robot',0,0,240,10,20),('plc',0,1,128,5,50),('agv',0,2,256,20,100),
                     ('meter',2,3,128,100,500),('monitor',2,4,128,50,200),('video',1,6,1000,20,100)]:add(*spec)
        add('alarm-a',0,1,128,2000,50,8);add('alarm-b',0,1,128,2000,50,8)
    return sorted(rows,key=lambda r:(r['arrival_ms'],r['source'],r['id']))
def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--pilot',action='store_true');a=p.parse_args()
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);inputs=out/'inputs';inputs.mkdir()
    seeds=[1690] if a.pilot else [1701,1702,1703];jobs=[];configs={};sources={}
    for policy in POLICIES:
        source=CONFIG/f'{policy}.json';cfg=json.loads(source.read_text());sources[str(source.relative_to(ROOT))]=sha(source)
        # Fixed protocol choice from the earlier measured decision delays, not
        # selected on the new outcomes. Every policy gets the same lead time.
        cfg.update(delay_ms=20,tenant_headroom=.4,tenant_theta_ms=9,tenant_max_duration_ms=500)
        for mode in ['bulk','staged']:
            c={**cfg,'tenant_release_fraction':.8 if mode=='staged' else 0.0,'tenant_release_pattern':'four_batches'}
            dest=inputs/f'{policy}_{mode}.json';write(dest,c);configs[policy,mode]=dest
    for seed in seeds:
        trace=inputs/f'industrial_{seed}.csv';csvwrite(trace,traffic(seed,6000))
        requests=inputs/f'requests_{seed}.csv'
        csvwrite(requests,[dict(id=f'target-{i}',request_ms=t,units=16,duration_ms=500) for i,t in enumerate([1000,3000,5000])])
        for mode in ['bulk','staged']:
            for policy in (['gar_h'] if a.pilot else POLICIES):
                for condition in ['controller_present','controller_killed']:
                    jobs.append(dict(name=f'{seed}_{mode}_{policy}_{condition}',kind='targeted',seed=seed,mode=mode,policy=policy,
                                     condition=condition,duration_ms=6000,controller_stop_ms=1121 if condition=='controller_killed' else None,
                                     trace=str(trace.relative_to(out)),requests=str(requests.relative_to(out)),config=str(configs[policy,mode].relative_to(out))))
    random.Random(20260930).shuffle(jobs)
    probes=[]
    for seed in ([] if a.pilot else [1801,1802]):
        for size in [96,240]:
            trace=inputs/f'probe_{seed}_{size}.csv';csvwrite(trace,traffic(seed,2000,size))
            for floor in [21,49,68]:
                cfg=json.loads(configs['fixed_h','bulk'].read_text());cfg.update(static_units=floor,initial_units=floor,source_type_deadlines=[100]*7,gate_eta=-1)
                dest=inputs/f'probe_{floor}.json'
                if dest.exists():assert json.loads(dest.read_text())==cfg
                else:write(dest,cfg)
                probes.append(dict(name=f'probe_{seed}_{size}_{floor}',kind='service_probe',seed=seed,packet_bytes=size,floor=floor,
                    policy='static_soft',condition='controller_present',duration_ms=2000,controller_stop_ms=None,
                    trace=str(trace.relative_to(out)),requests=None,config=str(dest.relative_to(out))))
    random.Random(20260931).shuffle(probes)
    manifest={'study':'Targeted runtime mechanisms and measurement integrity','stage':'development_pilot' if a.pilot else 'predeclared_followup',
        'trials':probes+jobs,'targeted_trials':len(jobs),'service_probes':len(probes),
        'input_sha256':{str(p.relative_to(out)):sha(p) for p in sorted(inputs.iterdir())},'transferred_config_sha256':sources,
        'generator_sha256':sha(__file__),
        'question':'Do independently executing reservations preserve an accepted, unfinished finite job across a fixed controller interruption and subsequent installed fallback?',
        'scope':'Three synthetic phase blocks on one instrumented localhost gateway; a targeted mechanism check, not a new industrial dataset or renewed superiority benchmark.',
        'traffic':'One robot240B/10ms,D20; PLC128B/5ms,D50; AGV256B/20ms,D100; two alarms eight128B packets/2000ms,D50; meter128B/100ms,D500; monitor128B/50ms,D200; video1000B/20ms,D100. Seed changes source phase only.',
        'tenant':'Three fixed requests at1000/3000/5000ms, q16,L500ms, original starts request+20ms. Gamma.4,theta9ms from prior independent calibration; no new payload tuning.',
        'modes':'Bulk releases all payload at original start. Staged releases equal-count packet batches at0/20/50/80percent of the original window. Total payload and deadline are identical across modes.',
        'interruption':'SIGTERM to controller process group at fixed1121ms,101ms into target-0 window and1ms after its second staged batch. The time is not selected using admission or delivery outcomes. Controller-present partner uses the same trace and requests.',
        'required_case_counts':'For every original attempt report target acceptance, actual kill inside window, unfinished total bytes, already-released unreceived bytes, installed fallback within window after kill, payload delivered after kill/fallback, and full completion. Rejection/already-complete/latekill/nofallback remain explicit; never select a favorable subset.',
        'fallback_scope':'Floor methods are tested for installed fallback; fixed and direct-priority policies have no such timer and are explicitly not applicable for that event. A killed trial with no relevant GAR fallback cannot count as a fallback-preservation success.',
        'lead_time':'All policies use20ms planned admission/actuation lead, a declared change from previous2ms based on previous measured admission/actuation delays. Observation periods, command prices, gates and mathematical algorithms retain their prior selected values.',
        'probes':'Twelve separate saturated fixed-floor service probes: q21/49/68, critical packets96/240B, seeds1801/1802,2s offered time. Critical source offers at least1300B/ms, video1000B/ms, monitor1024B/ms,allD100. These deliberately violate traffic envelopes to keep queues backlogged; they are service diagnostics, not deadline-theorem validation.',
        'service_windows_ms':[5,20,100,500],'clock_sample_ms':20,'clock_gap_threshold_ms':50,'witness_max_interval_ms':100,
        'integrity':'Source/sink per-row logs, fsync every250ms and on close, independent completion hashes and counts; role exit records and nested-process cleanup; all failures retained. No run is repeated or replaced for its outcome.',
        'clocks':'Record boot-identity hash and paired wall/monotonic/continuous readings. Flag observed wall-continuous or continuous-monotonic elapsed discrepancy>50ms, or witness sampling gap>100ms; flag is descriptive, not grounds for deleting trial.',
        'service_scope':'Nominal650B/ms remains in certificate and quotas. New measurements characterize ingress allowances, envelope excess and actual critical service; they do not turn empirical margins into deterministic guarantees.',
        'execution':'Sequential trials with transient caffeinate and other tracked study computations paused. Record load averages; no assertion of an otherwise idle personal computer. Controller/gateway logging overhead is part of measured execution.',
        'analysis':'Exact counts and paired controller-present/killed contrasts per block and mode. Three synthetic phase blocks are not enough for broad performance inference; do not treat packets/windows as independent replications.'}
    write(out/'PROTOCOL.json',manifest);print(json.dumps({'protocol':str(out/'PROTOCOL.json'),'trials':len(manifest['trials']),'sha256':sha(out/'PROTOCOL.json')},indent=2))
if __name__=='__main__':main()
