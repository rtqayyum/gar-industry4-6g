#!/usr/bin/env python3
"""Real wall-clock UDP gateway and separate control process.

This prototype is packetized and uses ordinary OS scheduling. The shared byte
certificate guides decisions; it is not a hard real-time guarantee for this
runtime. All original offered deadlines remain in the outcome denominator.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import heapq
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import platform
import select
import socket
import struct
import subprocess
import tempfile
import signal
import shutil
import sys
import traceback
import time
from collections import defaultdict,deque
from instrumentation import (atomic_json,DurableCSV,DurableList,StateMap,BoundedAdapter,
    bounded_line,register_child,run_role,clocks,continuous_ns,boot_identity,journal)
from realtime_tenants import prepare_requests,broker_main,TenantPacketScheduler,summarize_tenants
from transaction_guard import (GatewayTransactionHandler, AdmissionReplay,
    bind_command, request_contract, release_contract_check)

HERE=Path(__file__).resolve().parent
WIRE=struct.Struct('!Q')
DEFAULT={"capacity_bytes_per_ms":400,"units":106,"base_units":21,"maximum_units":74,
         "background_units":[27,5],"spare_weights":[20,65,15],"static_units":56,
         "step_ms":5,"delay_ms":5,"term_ms":40,"guardband_ms":4,"command_price":0,
         "initial_units":74,"buffer_bytes_per_class":16*1024*1024,
         "reactive_high_bytes":512,"reactive_low_bytes":128,"reactive_dwell_ms":20,
         "envelopes":[{"min_deadline":20,"sigma":1024,"rho":120}],
         "heartbeat_interval_ms":10,"heartbeat_timeout_ms":25,"heartbeat_admission_pause":True,
         "gate_eta":-1,"pacer_spin_us":100,"tenant_max_duration_ms":500}
POLICY_MODES={'gar_h':'joint_guarded','predictive_h':'predictive','fixed_h':'static_soft','residual_priority_h':'residual_priority'}


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def dump(path,data):Path(path).write_text(json.dumps(data,indent=2,allow_nan=False)+'\n')


def write_csv(path,rows,fields=None):
    fields=fields or sorted({key for row in rows for key in row})
    with Path(path).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)


def control_socket():
    sock=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)
    # macOS defaults to only 2,048 send bytes. These are per-socket settings,
    # available to an ordinary user; no global network parameter is changed.
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,65536)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,131072)
    return sock


def calendar_runs(values):
    result=[]
    for value in values:
        if result and result[-1][1]==value:result[-1][0]+=1
        else:result.append([1,value])
    return result


class FutureAdmissionLedger:
    """Irrevocable finite reservations; this records rights, not delivered jobs."""
    def __init__(self,cfg):
        self.cfg=cfg
        self.total=cfg['units'];self.background=sum(cfg['background_units']);self.jobs=[];self.version=0;self.booked={};self.ids=set();self.max_end=0
        self.capacity=cfg['capacity_bytes_per_ms'];self.booked_bytes={}
    def other_at(self,t):
        return self.background+self.booked.get(t,0)
    def profile(self,now,horizon):return [self.other_at(now+n) for n in range(horizon)]
    def check(self,request,now,critical_calendar,priority_rights=None):
        j={key:request[key] for key in ['id','start_ms','end_ms','units']}
        if j['id'] in self.ids:return False,'duplicate_job'
        if any(not isinstance(j[k],int) for k in ['start_ms','end_ms','units']):return False,'noninteger_job'
        if j['start_ms']<now or j['end_ms']<=j['start_ms'] or j['units']<1:return False,'invalid_job_interval'
        accepted,reason=release_contract_check(request,self.cfg)
        if not accepted:return False,reason
        if len(critical_calendar)<j['end_ms']-now:return False,'incomplete_future_calendar'
        for t in range(j['start_ms'],j['end_ms']):
            rights=priority_rights if priority_rights is not None else critical_calendar[t-now]
            if rights+self.other_at(t)+j['units']>self.total:return False,'future_rights_conflict'
        return True,'admitted_full_future_interval'
    def commit(self,request):
        began=time.monotonic_ns()
        j=request_contract(request)
        if j['id'] in self.ids:raise ValueError('Duplicate reservation commit')
        self.jobs.append(j);self.ids.add(j['id']);self.version+=1
        self.max_end=max(self.max_end,j['end_ms'])
        for t in range(j['start_ms'],j['end_ms']):
            self.booked[t]=self.booked.get(t,0)+j['units']
            self.booked_bytes[t]=self.booked_bytes.get(t,0)+self.capacity*j['units']//self.total
        finished=time.monotonic_ns()
        journal('gateway_commitments',{'event':'booking_commit','id':j['id'],'commit_begin_ns':began,
            'commit_complete_ns':finished,'ledger_version':self.version,**j})
    def admit(self,request,now,critical_calendar,priority_rights=None):
        accepted,reason=self.check(request,now,critical_calendar,priority_rights)
        if accepted:self.commit(request)
        return accepted,reason


def read_trace(path,duration):
    rows=[];keys=set()
    with Path(path).open() as f:
        for i,row in enumerate(csv.DictReader(f)):
            x={k:int(row[k]) for k in ['arrival_ms','class_id','size_bytes','deadline_ms']}
            x.update(source=str(row['source']),id=str(row['id']),wire_id=i)
            if row.get('type_id','')!='':x['type_id']=int(row['type_id'])
            if x['class_id'] not in (0,1,2) or not 8<=x['size_bytes']<=65000:
                raise ValueError('Class must be 0/1/2; complete UDP payload must be 8..65000 bytes')
            if not 0<=x['arrival_ms']<duration or x['deadline_ms']<=x['arrival_ms']:
                raise ValueError('Trace arrival must be inside cohort and deadline after arrival')
            key=(x['source'],x['id'])
            if key in keys:raise ValueError('Duplicate source/id')
            keys.add(key);rows.append(x)
    if not rows:raise ValueError('Empty trace')
    critical_D=sorted({r['deadline_ms']-r['arrival_ms'] for r in rows if r['class_id']==0})
    for r in rows:
        if 'type_id' not in r:
            D=r['deadline_ms']-r['arrival_ms']
            r['type_id']=(critical_D.index(D) if r['class_id']==0 else (6 if r['class_id']==1 else (3 if D==500 else 4)))
        if not 0<=r['type_id']<7:raise ValueError('Original type_id must be0..6')
    return rows


class CausalArrivalHistory:
    """Integer ingress bytes in [t-99,t], exactly the shared 100ms rule."""
    def __init__(self):self.events=deque();self.totals=[0]*7;self.now=-1
    def advance(self,t):
        if t<self.now:raise ValueError('Arrival history clock moved backward')
        self.now=t
        while self.events and self.events[0][0]<t-99:
            _,kind,amount=self.events.popleft();self.totals[kind]-=amount
    def observe(self,t,kind,amount):
        self.advance(t);self.events.append((t,kind,amount));self.totals[kind]+=amount


def wait_start(start):
    while start.value==0:time.sleep(.001)
    while time.monotonic_ns()<start.value:
        time.sleep(min(.001,max(0,(start.value-time.monotonic_ns())/1e9)))
    return start.value


def sleep_until(target):
    # Sleep against an absolute target without accumulating relative drift.
    # Wakes can still be late; every actual timestamp is recorded.
    while True:
        left=(target-time.monotonic_ns())/1e9
        if left<=0:return
        time.sleep(min(left,.001))


Adapter=BoundedAdapter


class CompiledPacer:
    """One packet outstanding; actual UDP completion is independent of Python IPC."""
    def __init__(self,executable,sink_port,capacity,gateway_path,out,spin_us):
        self.path=gateway_path+'_p';self.ack_path=gateway_path+'_a'
        self.sock=control_socket();self.sock.bind(self.ack_path);self.sock.setblocking(False)
        before=time.monotonic_ns()
        self.error_log=(out/'pacer.stderr').open('w',buffering=1)
        self.p=register_child(subprocess.Popen([str(executable),self.path,str(sink_port),str(capacity),str(spin_us),str(out/'pacer.csv')],
                                stdout=subprocess.PIPE,stderr=self.error_log,bufsize=0),'pacer')
        line=bounded_line(self.p,timeout=2.0);after=time.monotonic_ns()
        if not line:raise RuntimeError('Compiled pacer failed; see pacer.stderr')
        ready=json.loads(line)
        if not ready.get('ready') or not before<=ready['clock_ns']<=after:
            self.p.terminate();raise RuntimeError('C++ and Python monotonic clocks do not share the verified epoch')
        self.clock_check={'python_before_ns':before,'cpp_ready_ns':ready['clock_ns'],'python_after_ns':after,'shared_epoch_pass':True}
    def send(self,payload):self.sock.sendto(payload,self.path)
    def receive(self):
        try:data,_=self.sock.recvfrom(65536)
        except BlockingIOError:return None
        return json.loads(data)
    def close(self):
        self.sock.sendto(b'QUIT',self.path);self.p.wait(timeout=2)
        if self.p.returncode:raise RuntimeError('Compiled pacer failed; see pacer.stderr')
        self.sock.close()


def source_main(trace,ingress,start,ready,out):
    sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);ready.put(('source',None))
    origin=wait_start(start)
    fields=['wire_id','intended_send_ns','actual_send_ns','source_wake_lateness_us','sent_bytes','error']
    with DurableCSV(out/'source.csv',fields) as events:
        for row in sorted(trace,key=lambda x:(x['arrival_ms'],x['wire_id'])):
            intended=origin+row['arrival_ms']*1_000_000;sleep_until(intended)
            payload=WIRE.pack(row['wire_id'])+bytes([row['class_id']])*(row['size_bytes']-WIRE.size)
            stamp=time.monotonic_ns();error='';sent=0
            try:sent=sock.sendto(payload,('127.0.0.1',ingress))
            except OSError as e:error=str(e)
            events.write({'wire_id':row['wire_id'],'intended_send_ns':intended,'actual_send_ns':stamp,
                          'source_wake_lateness_us':(stamp-intended)/1000,'sent_bytes':sent,'error':error})
    sock.close()


def sink_main(trace,start,ready,out,observation_ms):
    sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,16*1024*1024)
    sock.bind(('127.0.0.1',0));sock.setblocking(False)
    ready.put(('sink',sock.getsockname()[1]));origin=wait_start(start)
    end=origin+observation_ms*1_000_000
    with DurableCSV(out/'sink.csv',['wire_id','received_ns','received_bytes','valid']) as events:
        while time.monotonic_ns()<end:
            readable,_,_=select.select([sock],[],[],min(.001,max(0,(end-time.monotonic_ns())/1e9)))
            if not readable:continue
            for _ in range(1024):
                try:data,_=sock.recvfrom(65536)
                except BlockingIOError:break
                now=time.monotonic_ns();wire=WIRE.unpack(data[:WIRE.size])[0] if len(data)>=WIRE.size else -1
                valid=0<=wire<len(trace) and len(data)==trace[wire]['size_bytes']
                events.write({'wire_id':wire,'received_ns':now,'received_bytes':len(data),'valid':int(valid)})
    sock.close()


def planner_request(adapter,snapshot,cfg):
    env=[]
    for e in cfg['envelopes']:
        D=int(e['min_deadline'])-cfg['guardband_ms']
        if D<1:raise ValueError('Guardband consumes an entire deadline class')
        env += [D,e['sigma'],e['rho']]
    pending=snapshot['critical_pending']
    if any(d<=0 for d,b in pending if b):
        return {'replace':False,'certified':False,'no_feasible':True,'reason':'guardband_overdue_pending','planning_us':0}
    H=len(snapshot['calendar'])
    request=snapshot.get('tenant_request');delay=request['start_ms']-snapshot['now_ms'] if request else cfg['delay_ms']
    values=[cfg['capacity_bytes_per_ms'],cfg['units'],cfg['base_units'],cfg['maximum_units'],
            cfg['step_ms'],delay,cfg['term_ms'],snapshot['old_final_change'],cfg['command_price'],
            len(cfg['envelopes']),len(pending),H,*env]
    for p in pending:values.extend(p)
    values.extend(snapshot['calendar']);values.extend(snapshot.get('other_units',[sum(cfg['background_units'])]*H))
    if request:values.extend([request['units'],request['duration_ms']])
    return adapter.call(('JOINT ' if request else ('PRED ' if cfg.get('predictive_controller') else 'PLAN '))+' '.join(map(str,values)))


def controller_main(policy,cfg,adapter_path,gateway_path,controller_path,start,ready,out,observation_ms):
    sock=control_socket();sock.bind(controller_path);sock.settimeout(cfg['heartbeat_interval_ms']/1000)
    adapter=Adapter(adapter_path) if policy in ['guarded','predictive','joint_guarded','priority_joint'] else None
    ready.put(('controller',None));origin=wait_start(start);end=origin+observation_ms*1_000_000
    epoch=0;last_change=-10**9;last_target=cfg['initial_units'];admission_sent=set();next_heartbeat=origin
    # Flush control records so intentional process termination preserves evidence.
    with (out/'controller.jsonl').open('w') as log:
        def record(x):log.write(json.dumps(x,separators=(',',':'))+'\n');log.flush()
        while time.monotonic_ns()<end:
            if time.monotonic_ns()>=next_heartbeat:
                beat={'type':'heartbeat','sent_ns':time.monotonic_ns()}
                try:sock.sendto(json.dumps(beat).encode(),gateway_path);record(beat)
                except OSError as e:record({'type':'heartbeat_send_error','actual_ns':time.monotonic_ns(),'error':str(e)})
                next_heartbeat=time.monotonic_ns()+cfg['heartbeat_interval_ms']*1_000_000
            try:raw,_=sock.recvfrom(262144)
            except socket.timeout:continue
            except OSError:break
            arrived=time.monotonic_ns();m=json.loads(raw)
            if m['type']=='ack':record({**m,'controller_ack_ns':arrived});continue
            if m['type'] not in ['snapshot','tenant_snapshot']:continue
            for key in ['calendar','other_units']:
                if key+'_runs' in m:m[key]=[value for length,value in m.pop(key+'_runs') for _ in range(length)]
            for request in cfg.get('admission_requests',[]):
                if request['id'] not in admission_sent and m['now_ms']>=request.get('request_ms',0):
                    message={'type':'admit','transmit_ns':time.monotonic_ns(),**request}
                    sock.sendto(json.dumps(message).encode(),gateway_path);admission_sent.add(request['id']);record(message)
            elapsed=(arrived-origin)/1e6
            if any(a<=elapsed<b for a,b in cfg.get('controller_blackouts_ms',[])):
                record({'type':'blackout_snapshot_skipped','seq':m['seq'],'actual_ns':arrived});continue
            if policy in ['static_soft','priority','residual_priority'] or (m['pending_install'] and m['type']=='snapshot') or (m.get('admission_inflight') and m['type']=='snapshot'):
                continue
            began=time.monotonic_ns()
            if policy in ['guarded','predictive','joint_guarded','priority_joint']:d=planner_request(adapter,m,cfg)
            else:
                q=m['critical_backlog_bytes'];target=m['units']
                if q>=cfg['reactive_high_bytes']:
                    target=min(cfg['maximum_units'],max(cfg['base_units'],math.ceil((q/max(1,cfg['step_ms']))*cfg['units']/cfg['capacity_bytes_per_ms'])))
                elif q<=cfg['reactive_low_bytes'] and m['now_ms']-last_change>=cfg['reactive_dwell_ms']:
                    target=cfg['base_units']
                d={'replace':target!=m['units'],'certified':False,'no_feasible':False,'target':target,
                   'term':cfg['term_ms'],'tail':cfg['base_units'],'reason':'reactive_threshold_dwell','planning_us':0}
            finished=time.monotonic_ns()
            record({'type':'decision','seq':m['seq'],'snapshot_gateway_ns':m['gateway_ns'],'controller_receive_ns':arrived,
                    'planning_begin_ns':began,'planning_end_ns':finished,'adapter_roundtrip_us':(finished-began)/1000,**d})
            if cfg.get('calibration_no_actuation'):continue
            if not d['replace']:
                if m.get('tenant_request'):
                    sock.sendto(json.dumps({'type':'tenant_reject','id':m['tenant_request']['id'],'reason':d['reason']}).encode(),gateway_path)
                continue
            epoch+=1
            cmd={'type':'command','epoch':epoch,'snapshot_seq':m['seq'],'state_version':m['state_version'],
                 'ledger_version':m.get('ledger_version',0),
                 'observation_ms':m['now_ms'],'activate_ms':m['tenant_request']['start_ms'] if m.get('tenant_request') else m['now_ms']+cfg['delay_ms'],
                 'target':d['target'],'term':d['term'],'tail':d['tail'],
                 'certificate_passed':d['certified'],'policy':policy,'decision_reason':d['reason'],
                 'transmit_ns':time.monotonic_ns()}
            if m.get('tenant_request'):cmd['tenant_request']=m['tenant_request']
            cmd=bind_command(cmd,m,cfg)
            payload=json.dumps(cmd,separators=(',',':')).encode()
            try:
                sock.sendto(payload,gateway_path);record({**cmd,'payload_bytes':len(payload)})
                last_change=m['now_ms'];last_target=d['target']
            except OSError as e:record({'type':'command_send_error','epoch':epoch,'error':str(e),'actual_ns':time.monotonic_ns()})
    if adapter:adapter.close()
    sock.close()


class PacketDeficitScheduler:
    """Whole-packet deficit rounds; unused floors are shared by active queues.

    Deficits are virtual fairness accounting, never wall-clock service credit.
    The serial pacer limits actual service even when many rounds are needed.
    Packetization permits short-window departures from configured byte floors.
    """
    def __init__(self,cfg,priority=False):self.cfg=cfg;self.credit=[0.,0.,0.];self.cursor=0;self.priority=priority
    def choose(self,queues,units):
        active=[k for k in range(3) if queues[k]]
        for k in range(3):
            if not queues[k]:self.credit[k]=0
        if not active:return None
        floors=[units,*self.cfg['background_units']]
        C=self.cfg['capacity_bytes_per_ms'];N=self.cfg['units']
        floor_shares={k:(C*floors[k]//N)/C for k in active}
        spare=max(0.,1.-sum(floor_shares.values()))
        weights=sum(self.cfg['spare_weights'][k] for k in active)
        if self.priority and 0 in active:
            # Keep background floors, then give all available spare to critical.
            shares={k:floor_shares[k]+(spare if k==0 else 0) for k in active}
        else:
            shares={k:floor_shares[k]+spare*self.cfg['spare_weights'][k]/weights for k in active}
        for _ in range(100000):
            for j in range(3):
                k=(self.cursor+j)%3
                if k in shares and self.credit[k]+1e-9>=queues[k][0][2]['size_bytes']:
                    self.credit[k]-=queues[k][0][2]['size_bytes'];self.cursor=(k+1)%3;return k
            for k in active:self.credit[k]+=self.cfg['capacity_bytes_per_ms']*shares[k]
        raise RuntimeError('Deficit scheduler did not make progress')


def gateway_main(trace,policy,cfg,adapter_path,sink_port,gateway_path,controller_path,start,ready,out,observation_ms):
    ingress=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);ingress.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,16*1024*1024)
    ingress.bind(('127.0.0.1',0));ingress.setblocking(False)
    pacer=CompiledPacer(adapter_path.parent/'packet_pacer',sink_port,cfg['capacity_bytes_per_ms'],gateway_path,out,cfg.get('pacer_spin_us',100))
    control=control_socket();control.bind(gateway_path);control.setblocking(False)
    adapter=Adapter(adapter_path)
    initial=cfg['static_units'] if policy=='static_soft' else (cfg['base_units'] if policy in ['priority','residual_priority'] else cfg['initial_units'])
    state=adapter.call(f'INIT {initial}')
    ready.put(('gateway',ingress.getsockname()[1]));origin=wait_start(start);end=origin+observation_ms*1_000_000
    requests=cfg.get('tenant_requests',[]);request_by_id={r['id']:r for r in requests};pending_admissions={}
    queues=[[] for _ in range(3+len(requests))];queued=[0]*len(queues);nonempty=set();states=StateMap();events=DurableList('gateway_events_live');command_log=DurableList('gateway_commands_live');timeline=DurableList('gateway_timeline_live');ledger=FutureAdmissionLedger(cfg);releases=[]
    scheduler=TenantPacketScheduler(cfg,requests,origin,policy in ['priority','priority_joint','residual_priority']) if requests else PacketDeficitScheduler(cfg,policy in ['priority','priority_joint','residual_priority'])
    inflight=None;seq=0;next_snapshot=0;last_advance=-1;last_sample=-1
    admission_replay=AdmissionReplay()
    forwarded=[0]*len(queues);max_packet=max(x['size_bytes'] for x in trace)
    arrival_history=CausalArrivalHistory();last_heartbeat=origin;queue_epoch=0;request_timing={};gate_log=DurableList('admission_screens_live')
    max_D=max(int(e['min_deadline']) for e in cfg['envelopes'])
    horizon=max(256,cfg['term_ms']+cfg['delay_ms']+max_D+8,max(x['deadline_ms']-x['arrival_ms'] for x in trace)+8)
    def apply_events(data,actual):
        for ev in data['events']:
            events.append({**ev,'actual_application_ns':actual,'scheduled_ns':origin+ev['scheduled_ms']*1_000_000,
                           'timer_lateness_us':(actual-origin-ev['scheduled_ms']*1_000_000)/1000})
    def materialize(request,actual):
        count=len(request['wire_ids'])
        for index,wire in enumerate(request['wire_ids']):
            row=trace[wire];k=row['class_id'];payload=WIRE.pack(wire)+bytes([3])*(row['size_bytes']-WIRE.size)
            release_ms=request['start_ms']+request['packet_release_offsets'][index]
            states[wire]={'wire_id':wire,'ingress_ns':actual,'received_bytes':row['size_bytes'],'status':'scheduled',
                          'tenant_release_ns':origin+release_ms*1_000_000}
            heapq.heappush(releases,(origin+release_ms*1_000_000,wire,{**row,'payload':payload}))
    def release_tenant_payload(now):
        while releases and releases[0][0]<=now:
            release,wire,row=heapq.heappop(releases);k=row['class_id']
            states[wire].update(status='queued',tenant_release_observed_ns=now)
            if queued[k]+row['size_bytes']>cfg['buffer_bytes_per_class']:states[wire]['status']='overflow';continue
            heapq.heappush(queues[k],(row['deadline_ms'],wire,row));queued[k]+=row['size_bytes'];nonempty.add(k)
    def admission_ack(request,accepted,reason,received,address):
        ack={'type':'admission_ack','id':request['id'],'accepted':accepted,'reason':reason,
             'gateway_receive_ns':received,'ledger_version':ledger.version,
             'admission_ns':time.monotonic_ns() if accepted else '',
             'scope':'Tenant payload shares the actual serial packet server' if requests else 'Resource rights only'}
        timing=request_timing.get(request['id'],{})
        ack.update(broker_transmit_ns=timing.get('transmit_ns',''),decision_end_ns=time.monotonic_ns(),
                   first_gateway_receive_ns=timing.get('first_receive_ns',received),queue_epoch=queue_epoch)
        ack['request_receive_to_decision_us']=(ack['decision_end_ns']-ack['first_gateway_receive_ns'])/1000
        admission_replay.remember(request,ack)
        command_log.append(ack)
        if accepted and request['id'] in request_by_id:materialize(request_by_id[request['id']],ack['admission_ns'])
        try:control.sendto(json.dumps(ack).encode(),address)
        except OSError:pass
        pending_admissions.pop(request['id'],None)
    def heartbeat_fresh(now):return not cfg.get('heartbeat_admission_pause',True) or now-last_heartbeat<=cfg['heartbeat_timeout_ms']*1_000_000
    def admission_screen(request,phase='initial',residual=False):
        began=time.monotonic_ns();ms=max(0,(began-origin)//1_000_000);offset=request['start_ms']-ms
        if offset<0:return False,'missed_admission_start'
        D=cfg['source_type_deadlines'];last_booking=max(request['end_ms'],ledger.max_end)-ms
        H=max(horizon,last_booking+max(D)+1)
        pending=defaultdict(int)
        for k in ([0] if residual else [0,1,2]):
            for _,wire,row in queues[k]:
                deadline=row['deadline_ms']-ms-(cfg['guardband_ms'] if residual else 0)
                if residual and deadline<=0:return False,'guardband_overdue_pending'
                pending[max(1,deadline)]+=row['size_bytes']
        if inflight and (not residual or inflight['row']['class_id']==0) and inflight['row']['class_id']<3:
            row=inflight['row'];deadline=row['deadline_ms']-ms-(cfg['guardband_ms'] if residual else 0)
            if residual and deadline<=0:return False,'guardband_overdue_pending'
            pending[max(1,deadline)]+=row['size_bytes']
        H=max(H,max(pending,default=0));quota=cfg['capacity_bytes_per_ms']*request['units']//cfg['units']
        old=[ledger.booked_bytes.get(ms+n,0) for n in range(H)]
        if residual:
            env=[]
            for e in cfg['envelopes']:env.extend([int(e['min_deadline'])-cfg['guardband_ms'],e['sigma'],e['rho']])
            bg=sum(cfg['capacity_bytes_per_ms']*u//cfg['units'] for u in cfg['background_units'])
            values=[cfg['capacity_bytes_per_ms'],bg,offset,request['duration_ms'],quota,last_booking,len(cfg['envelopes']),len(pending),H,*env]
            op='RESIDUAL '
        else:
            if cfg['gate_eta']<0:return True,'gate_disabled'
            arrival_history.advance(ms)
            values=[cfg['capacity_bytes_per_ms'],offset,request['duration_ms'],quota,cfg['gate_eta'],len(pending),H,*D,*arrival_history.totals]
            op='GATE '
        for item in sorted(pending.items()):values.extend(item)
        values.extend(old);decision=adapter.call(op+' '.join(map(str,values)));finished=time.monotonic_ns()
        gate_log.append({'id':request['id'],'screen':'residual_certificate' if residual else 'headroom','phase':phase,
            'snapshot_ms':ms,'queue_epoch':queue_epoch,'ledger_version':ledger.version,'request_start_ms':request['start_ms'],
            'decision_begin_ns':began,'decision_end_ns':finished,'snapshot_to_decision_us':(finished-began)/1000,
            'deadline_remaining_after_decision_us':(origin+request['start_ms']*1_000_000-finished)/1000,**decision})
        return bool(decision['pass']),decision['reason']
    def snapshot(now,ms,request=None):
        nonlocal seq,state
        H=max(horizon,ledger.max_end-ms+max_D+1)
        if request:H=max(H,request['end_ms']-ms+max_D+1)
        if cfg.get('calibration_no_actuation') and not request:H=horizon
        state=adapter.call(f'ADV {ms} {H}');actual=time.monotonic_ns();apply_events(state,actual)
        pending=defaultdict(int)
        for _,wire,row in queues[0]:pending[row['deadline_ms']-ms-cfg['guardband_ms']]+=row['size_bytes']
        if inflight and inflight['row']['class_id']==0:
            row=inflight['row'];pending[row['deadline_ms']-ms-cfg['guardband_ms']]+=row['size_bytes']
        seq+=1
        message={'type':'tenant_snapshot' if request else 'snapshot','seq':seq,'gateway_ns':actual,'now_ms':ms,
                 'state_version':state['state_version'],'units':state['units'],'pending_install':state['pending'],
                 'ledger_version':ledger.version,'other_units_runs':calendar_runs(ledger.profile(ms,H)),
                 'admission_inflight':bool(pending_admissions),
                 'calendar_runs':calendar_runs(state['calendar']),'old_final_change':state['old_final_change'],
                 'critical_pending':sorted(pending.items()),'critical_backlog_bytes':queued[0]+(inflight['row']['size_bytes'] if inflight and inflight['row']['class_id']==0 else 0)}
        if request:message['tenant_request']={**request_contract(request),'duration_ms':request['end_ms']-request['start_ms']}
        message=transactions.issue_snapshot(message)
        try:control.sendto(json.dumps(message,separators=(',',':')).encode(),controller_path)
        except OSError as e:command_log.append({'type':'snapshot_send_error','seq':seq,'actual_ns':time.monotonic_ns(),'error':str(e)})
    def complete_packet():
        nonlocal inflight
        ack=pacer.receive()
        if ack is None:
            if inflight and time.monotonic_ns()-inflight['dispatched_ns']>250_000_000:
                journal('gateway_errors',{'event':'pacer_ack_timeout','wire_id':inflight['row']['wire_id'],**clocks()})
                raise TimeoutError('Outstanding pacer ACK exceeded250ms')
            return
        if inflight is None or ack['wire_id']!=inflight['row']['wire_id']:raise RuntimeError('Unexpected compiled-pacer completion')
        row=inflight['row'];status=states[ack['wire_id']]
        status.update({k:ack[k] for k in ['serialization_start_ns','finish_target_ns','egress_ns','sent_bytes']})
        status['status']='forwarded' if not ack['send_errno'] else 'egress_error'
        if ack['send_errno']:status['error']='send_errno='+str(ack['send_errno'])
        forwarded[row['class_id']]+=ack['sent_bytes']
        status['pacer_lateness_us']=(ack['egress_ns']-ack['finish_target_ns'])/1000
        status['pacer_ack_observed_ns']=time.monotonic_ns()
        inflight=None
    transactions=GatewayTransactionHandler(cfg,ledger,adapter,now_ns=time.monotonic_ns,
        origin_ns=origin,on_events=apply_events,heartbeat_fresh=heartbeat_fresh,
        admission_screen=lambda request:admission_screen(request,phase='precommit'),policy=policy)
    while time.monotonic_ns()<end:
        complete_packet()
        now=time.monotonic_ns();ms=max(0,(now-origin)//1_000_000)
        if ms!=last_advance:
            state=adapter.call(f'ADV {ms} 0');apply_events(state,time.monotonic_ns());last_advance=ms
        complete_packet()
        release_tenant_payload(time.monotonic_ns())
        for _ in range(512):
            try:payload,_=ingress.recvfrom(65536)
            except BlockingIOError:break
            actual=time.monotonic_ns();wire=WIRE.unpack(payload[:WIRE.size])[0] if len(payload)>=WIRE.size else -1
            if not 0<=wire<len(trace):events.append({'kind':'unknown_datagram','actual_application_ns':actual});continue
            row=trace[wire];k=row['class_id']
            if wire in states:events.append({'kind':'duplicate_datagram','actual_application_ns':actual,'wire_id':wire});continue
            states[wire]={'wire_id':wire,'ingress_ns':actual,'received_bytes':len(payload),'status':'queued'}
            if len(payload)!=row['size_bytes']:states[wire]['status']='invalid_size';continue
            if k<3:arrival_history.observe(max(0,(actual-origin)//1_000_000),row['type_id'],row['size_bytes'])
            if queued[k]+row['size_bytes']>cfg['buffer_bytes_per_class']:states[wire]['status']='overflow';continue
            item={**row,'payload':payload};heapq.heappush(queues[k],(row['deadline_ms'],wire,item));queued[k]+=row['size_bytes'];nonempty.add(k)
            queue_epoch+=1
        # Critical work and finite tenants expire; original backgrounds may arrive late.
        now=time.monotonic_ns()
        while queues[0] and origin+queues[0][0][0]*1_000_000<=now:
            _,wire,row=heapq.heappop(queues[0]);queued[0]-=row['size_bytes'];states[wire].update(status='expired',expired_ns=now)
        if not queues[0]:nonempty.discard(0)
        for k in [x for x in nonempty if x>=3]:
            while queues[k] and origin+queues[k][0][0]*1_000_000<=now:
                _,wire,row=heapq.heappop(queues[k]);queued[k]-=row['size_bytes'];states[wire].update(status='expired',expired_ns=now)
            if not queues[k]:nonempty.discard(k)
        for key,(request,address,received) in list(pending_admissions.items()):
            if now>origin+request['start_ms']*1_000_000:admission_ack(request,False,'missed_joint_admission_start',received,address)
        for _ in range(32):
            try:raw,address=control.recvfrom(262144)
            except BlockingIOError:break
            received=time.monotonic_ns()
            try:cmd=json.loads(raw)
            except (ValueError,UnicodeDecodeError):
                command_log.append({'type':'invalid_control_message','reason':'invalid_json','gateway_receive_ns':received});continue
            if not isinstance(cmd,dict):
                command_log.append({'type':'invalid_control_message','reason':'nonobject_message','gateway_receive_ns':received});continue
            if cmd.get('type')=='heartbeat':
                emitted=cmd.get('sent_ns')
                if type(emitted) is not int:
                    command_log.append({'type':'invalid_heartbeat','gateway_receive_ns':received});continue
                if not origin<=emitted<=received:
                    command_log.append({'type':'invalid_heartbeat','transmit_ns':emitted,'gateway_receive_ns':received});continue
                last_heartbeat=max(last_heartbeat,emitted)
                command_log.append({'type':'heartbeat','transmit_ns':cmd['sent_ns'],'gateway_receive_ns':received})
                continue
            if cmd.get('type')=='tenant_reject':
                if isinstance(cmd.get('id'),str) and cmd['id'] in pending_admissions:
                    request,address,first_receive=pending_admissions[cmd['id']]
                    admission_ack(request,False,cmd.get('reason','controller_rejection'),first_receive,address)
                continue
            if cmd.get('type')=='admit':
                valid,invalid_reason=release_contract_check(cmd,cfg)
                if not valid or not isinstance(cmd.get('id'),str) or type(cmd.get('transmit_ns')) is not int:
                    invalid={'type':'admission_ack','id':cmd.get('id'),'accepted':False,'reason':invalid_reason if not valid else 'malformed_admission_message'}
                    command_log.append(invalid)
                    try:control.sendto(json.dumps(invalid).encode(),address)
                    except OSError:pass
                    continue
                replay=admission_replay.lookup(cmd)
                if replay is not None:
                    command_log.append(replay)
                    try:control.sendto(json.dumps(replay).encode(),address)
                    except OSError:pass
                    continue
                known=request_by_id.get(str(cmd['id']))
                if str(cmd['id']) in pending_admissions:
                    original=pending_admissions[str(cmd['id'])][0]
                    try:identical=request_contract(cmd)==request_contract(original)
                    except KeyError:identical=False
                    if identical:continue
                    conflict={'type':'admission_ack','id':str(cmd['id']),'accepted':False,'reason':'conflicting_pending_request_id_reuse'}
                    command_log.append(conflict)
                    try:control.sendto(json.dumps(conflict).encode(),address)
                    except OSError:pass
                    continue
                request_timing[str(cmd['id'])]={'transmit_ns':cmd['transmit_ns'],'first_receive_ns':received}
                if not heartbeat_fresh(received):
                    admission_ack(known or cmd,False,'controller_heartbeat_stale',received,address);continue
                if known and not known['supported']:
                    admission_ack(known,False,'unsupported_rate_latency_contract',received,address);continue
                if known:
                    try:matches=request_contract(cmd)==request_contract(known)
                    except KeyError:matches=False
                    if not matches:
                        admission_ack(known,False,'request_metadata_mismatch',received,address);continue
                ms=max(0,(received-origin)//1_000_000)
                H=max(horizon,int(cmd.get('end_ms',ms))-ms)
                state=adapter.call(f'ADV {ms} {H}');apply_events(state,time.monotonic_ns());last_advance=ms
                request=request_by_id.get(str(cmd['id']),cmd)
                screened,screen_reason=admission_screen(request)
                if not screened:admission_ack(request,False,screen_reason,received,address);continue
                accepted,reason=ledger.check(cmd,ms,state['calendar'],cfg.get('priority_ledger_units',cfg['static_units']) if policy=='priority' else None)
                if accepted and policy=='residual_priority':accepted,reason=admission_screen(request,residual=True)
                if time.monotonic_ns()>origin+cmd['start_ms']*1_000_000:accepted=False;reason='missed_admission_start'
                if requests and (str(cmd['id']) not in request_by_id or any(cmd[k]!=request[k] for k in ['start_ms','end_ms','units'])):
                    accepted=False;reason='request_metadata_mismatch'
                if accepted:ledger.commit(cmd);admission_ack(request,True,reason,received,address)
                elif policy in ['joint_guarded','priority_joint'] and reason=='future_rights_conflict' and not state['pending'] and not pending_admissions:
                    pending_admissions[request['id']]=(request,address,received);snapshot(time.monotonic_ns(),ms,request)
                else:admission_ack(request,False,reason,received,address)
                continue
            if cmd.get('type')!='command':continue
            joint=cmd.get('tenant_request')
            ack,new_state=transactions.handle(cmd,pending_admissions)
            if new_state is not None:state=new_state
            reason=ack['reason']
            if isinstance(joint,dict) and isinstance(joint.get('id'),str) and joint['id'] in pending_admissions and not ack.get('replayed'):
                request,broker_address,first_receive=pending_admissions[joint['id']]
                admission_ack(request,reason=='accepted','joint_certified_plan_and_admission' if reason=='accepted' else reason,first_receive,broker_address)
            sent=cmd.get('transmit_ns')
            ack.update({'transmit_ns':sent,'gateway_receive_ns':received,
                 'command_transport_us':(received-sent)/1000 if type(sent) is int else None,'activate_ms':cmd.get('activate_ms'),
                 'target':cmd.get('target'),'tail':cmd.get('tail'),'term':cmd.get('term'),'certificate_passed':cmd.get('certificate_passed')})
            command_log.append(ack)
            try:control.sendto(json.dumps(ack).encode(),address)
            except OSError:pass
        now=time.monotonic_ns();ms=max(0,(now-origin)//1_000_000)
        if ms>=next_snapshot and policy not in ['static_soft','priority','residual_priority']:
            snapshot(now,ms);next_snapshot=ms+cfg['step_ms'];last_advance=ms
        if inflight is None:
            # Planning/IPC can cross a scheduled installation boundary. Apply
            # that state before selecting another packet with a now-active
            # tenant floor. If the process cannot catch up, offer no service.
            for _ in range(4):
                dispatch_now=time.monotonic_ns();dispatch_ms=max(0,(dispatch_now-origin)//1_000_000)
                if dispatch_ms==last_advance:break
                state=adapter.call(f'ADV {dispatch_ms} 0');apply_events(state,time.monotonic_ns());last_advance=dispatch_ms
            dispatch_now=time.monotonic_ns();caught_up=max(0,(dispatch_now-origin)//1_000_000)==last_advance
            for k_expire in [0,*[x for x in nonempty if x>=3]]:
                while queues[k_expire] and origin+queues[k_expire][0][0]*1_000_000<=dispatch_now:
                    _,wire,row=heapq.heappop(queues[k_expire]);queued[k_expire]-=row['size_bytes'];states[wire].update(status='expired',expired_ns=dispatch_now)
                if not queues[k_expire]:nonempty.discard(k_expire)
            k=(scheduler.choose(queues,state['units'],dispatch_now,nonempty) if requests else scheduler.choose(queues,state['units'])) if caught_up else None
            if k is not None:
                _,wire,row=heapq.heappop(queues[k]);queued[k]-=row['size_bytes']
                if not queues[k]:nonempty.discard(k)
                began=time.monotonic_ns()
                states[wire].update(status='dispatched_to_pacer',gateway_dispatch_ns=began,floor_units_at_selection=state['units'])
                inflight={'row':row,'dispatched_ns':began};pacer.send(row['payload'])
        if ms!=last_sample:
            timeline.append({'actual_ns':time.monotonic_ns(),'elapsed_ms':ms,'floor_units':state['units'],
                             'critical_backlog_bytes':queued[0],'embb_backlog_bytes':queued[1],'mmtc_backlog_bytes':queued[2],
                             'critical_forwarded_bytes':forwarded[0],'embb_forwarded_bytes':forwarded[1],'mmtc_forwarded_bytes':forwarded[2],
                             'tenant_backlog_bytes':sum(queued[3:]),'tenant_unreleased_bytes':sum(r[2]['size_bytes'] for r in releases),'tenant_forwarded_bytes':sum(forwarded[3:]),
                             'inflight_bytes':inflight['row']['size_bytes'] if inflight else 0})
            last_sample=ms
        now=time.monotonic_ns();until=min(end,origin+(ms+1)*1_000_000)
        select.select([ingress,control,pacer.sock],[],[],max(0,min(.001,(until-now)/1e9)))
    if inflight:
        limit=time.monotonic()+1
        while inflight and time.monotonic()<limit:
            select.select([pacer.sock],[],[],.01);complete_packet()
        if inflight:raise RuntimeError('Compiled pacer did not finish its final outstanding packet')
    pacer.close()
    for sock in [ingress,control]:sock.close()
    state=adapter.call(f'ADV {max(last_advance,(time.monotonic_ns()-origin)//1_000_000)} 0');apply_events(state,time.monotonic_ns());adapter.close()
    write_csv(out/'gateway_packets.csv',list(states.values()),['wire_id','ingress_ns','received_bytes','status','gateway_dispatch_ns','serialization_start_ns','finish_target_ns','floor_units_at_selection','egress_ns','sent_bytes','pacer_lateness_us','pacer_ack_observed_ns','expired_ns','error','tenant_release_ns','tenant_release_observed_ns'])
    write_csv(out/'gateway_events.csv',events)
    write_csv(out/'gateway_commands.csv',command_log)
    write_csv(out/'admission_screens.csv',gate_log,['id','screen','phase','snapshot_ms','queue_epoch','ledger_version','request_start_ms','decision_begin_ns','decision_end_ns','snapshot_to_decision_us','deadline_remaining_after_decision_us','pass','horizon','checks','failing_start','failing_end','demand','available','planning_us','reason'])
    write_csv(out/'gateway_timeline.csv',timeline)
    dump(out/'gateway_summary.json',{'final_executor':state,'forwarded_bytes':forwarded,'end_queue_bytes':queued,
         'admitted_resource_reservations':ledger.jobs,'ledger_version':ledger.version,
         'tenant_job_completion_claimed':bool(requests),
         'max_packet_bytes':max_packet,'max_packet_serialization_ms':max_packet/cfg['capacity_bytes_per_ms'],
         'scheduler':'Whole-packet deficit rounds with active floors plus weighted spare; priority gives all spare to critical after background floors, then weighted spare to background when critical is idle',
         'pacer':'Independent compiled one-packet process; next serialization starts at actual receipt; no late-wake catch-up',
         'pacer_clock_check':pacer.clock_check,'pacer_spin_us':cfg.get('pacer_spin_us',100),
         'guarantee_scope':'No hard real-time or packetized service theorem claimed'})


def csv_data(path):
    with Path(path).open() as f:return list(csv.DictReader(f))


def summarize(trace,policy,cfg,out,origin,duration,observation_ms):
    source={int(r['wire_id']):r for r in csv_data(out/'source.csv')}
    gateway={int(r['wire_id']):r for r in csv_data(out/'gateway_packets.csv')}
    received={};duplicates=0
    for r in csv_data(out/'sink.csv'):
        if int(r['valid'])!=1:continue
        wire=int(r['wire_id'])
        if wire in received:duplicates+=1;continue
        received[wire]=r
    rows=[]
    for row in trace:
        wire=row['wire_id'];s=source.get(wire,{});g=gateway.get(wire,{});r=received.get(wire,{})
        deadline=origin+row['deadline_ms']*1_000_000;rx=int(r['received_ns']) if r else None
        rows.append({**row,'original_deadline_ns':deadline,'intended_release_ns':origin+row['arrival_ms']*1_000_000,
                     'source_send_ns':s.get('actual_send_ns',''),'source_error':s.get('error','not_attempted'),
                     'gateway_status':g.get('status','not_received'),'gateway_ingress_ns':g.get('ingress_ns',''),
                     'gateway_egress_ns':g.get('egress_ns',''),'sink_receive_ns':rx if rx is not None else '',
                     'delivered':int(rx is not None),'on_time':int(rx is not None and rx<=deadline),
                     'late':int(rx is not None and rx>deadline),'missing':int(rx is None),
                     'end_to_end_ms':(rx-origin-row['arrival_ms']*1_000_000)/1e6 if rx is not None else ''})
    write_csv(out/'packet_ledger.csv',rows)
    classes=[]
    for k in range(3):
        rs=[r for r in rows if r['class_id']==k];n=len(rs)
        classes.append({'class_id':k,'offered_packets':n,'offered_bytes':sum(r['size_bytes'] for r in rs),
                        'on_time':sum(r['on_time'] for r in rs),'late':sum(r['late'] for r in rs),
                        'missing':sum(r['missing'] for r in rs),'failures':sum(1-r['on_time'] for r in rs),
                        'failure_fraction':sum(1-r['on_time'] for r in rs)/n if n else None,
                        'gateway_expired':sum(r['gateway_status']=='expired' for r in rs),
                        'gateway_overflow':sum(r['gateway_status']=='overflow' for r in rs)})
    commands=csv_data(out/'gateway_commands.csv');events=csv_data(out/'gateway_events.csv')
    control=[]
    if (out/'controller.jsonl').exists():
        control=[json.loads(line) for line in (out/'controller.jsonl').read_text().splitlines() if line.strip()]
    decisions=[r for r in control if r['type']=='decision'];tx=[r for r in control if r['type']=='command']
    installed={int(r['plan_id']):r for r in events if r.get('kind')=='installation'}
    applied_commands=[(r,installed[int(r['executor_plan_id'])]) for r in commands
                      if r.get('type')=='ack' and r.get('accepted')=='True' and int(r['executor_plan_id']) in installed]
    def stats(values):
        v=sorted(float(x) for x in values)
        return {'n':len(v),'median':v[len(v)//2],'p99':v[int(.99*(len(v)-1))],'max':v[-1]} if v else {'n':0}
    serialization=[r for r in gateway.values() if r.get('serialization_start_ns')]
    serialization.sort(key=lambda r:int(r['serialization_start_ns']))
    overlap=sum(int(b['serialization_start_ns'])<int(a.get('egress_ns') or a['finish_target_ns']) for a,b in zip(serialization,serialization[1:]))
    early=sum(int(r['egress_ns'])<int(r['finish_target_ns']) for r in serialization if r.get('egress_ns'))
    summary={'policy':policy,'duration_ms':duration,'observation_ms':observation_ms,'origin_monotonic_ns':origin,
             'classes':classes,'sink_duplicates':duplicates,'command_transmissions':len(tx),
             'command_arrivals':sum(r.get('type')=='ack' for r in commands),
             'commands_accepted':sum(r.get('type')=='ack' and r.get('accepted')=='True' for r in commands),
             'admission_requests_received':sum(r.get('type')=='admission_ack' for r in commands),
             'admission_requests_accepted':sum(r.get('type')=='admission_ack' and r.get('accepted')=='True' for r in commands),
             'planning_calls':len(decisions),'planning_no_feasible':sum(r.get('no_feasible',False) for r in decisions),
             'snapshot_send_errors':sum(r.get('type')=='snapshot_send_error' for r in commands),
             'control_liveness_passed':policy in ['static_soft','priority','residual_priority'] or len(decisions)>0,
             'planning_adapter_us':stats(r['adapter_roundtrip_us'] for r in decisions),
             'command_transport_us':stats(r['command_transport_us'] for r in commands if r.get('command_transport_us')),
             'command_transmit_to_activation_us':stats((int(e['actual_application_ns'])-int(c['transmit_ns']))/1000 for c,e in applied_commands),
             'command_receive_to_activation_us':stats((int(e['actual_application_ns'])-int(c['gateway_receive_ns']))/1000 for c,e in applied_commands),
             'source_wake_lateness_us':stats(r['source_wake_lateness_us'] for r in source.values()),
             'timer_lateness_us':stats(r['timer_lateness_us'] for r in events if r.get('timer_lateness_us')),
             'pacer_lateness_us':stats(r['pacer_lateness_us'] for r in gateway.values() if r.get('pacer_lateness_us')),
             'serialization_overlap_count':overlap,'early_egress_count':early,
             'all_offered_ids_in_ledger':len(rows)==len(trace),'maximum_packet_blocking_ms':max(r['size_bytes'] for r in trace)/cfg['capacity_bytes_per_ms'],
             'configured_guardband_ms':cfg['guardband_ms'],'control_events_are_measured':True,
             'scope':'Actual same-host userspace UDP forwarding, nonpreemptive packets, ordinary OS scheduling; empirical runtime validation only'}
    gateway_summary=json.loads((out/'gateway_summary.json').read_text())
    screens=csv_data(out/'admission_screens.csv')
    summary['headroom_screen_calls']=sum(r['screen']=='headroom' for r in screens)
    summary['headroom_screen_passed']=sum(r['screen']=='headroom' and r['pass']=='True' for r in screens)
    summary['headroom_screen_roundtrip_us']=stats(r['snapshot_to_decision_us'] for r in screens if r['screen']=='headroom')
    summary['residual_certificate_calls']=sum(r['screen']=='residual_certificate' for r in screens)
    summary['admission_receive_to_decision_us']=stats(r['request_receive_to_decision_us'] for r in commands if r.get('type')=='admission_ack')
    summary['heartbeat_admission_rejections']=sum(r.get('reason')=='controller_heartbeat_stale' for r in commands)
    summary['source_types']=[{'type_id':k,'offered':sum(r['type_id']==k for r in rows),
        'failures':sum(1-r['on_time'] for r in rows if r['type_id']==k),
        'on_time_bytes':sum(r['size_bytes']*r['on_time'] for r in rows if r['type_id']==k)} for k in range(7)]
    summary['executor_counters']=gateway_summary['final_executor']['counters']
    assert all(c['offered_packets']==c['on_time']+c['late']+c['missing'] for c in classes)
    assert overlap==0 and early==0,'Serialized service violated'
    dump(out/'summary.json',summary);return summary


def main():
    def stop_parent(signum,frame):raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,stop_parent)
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--trace',type=Path,required=True);ap.add_argument('--config',type=Path,required=True)
    ap.add_argument('--policy',choices=[*POLICY_MODES,'static_soft'],required=True)
    ap.add_argument('--duration-ms',type=int,required=True);ap.add_argument('--drain-ms',type=int,default=100)
    ap.add_argument('--controller-stop-ms',type=int);ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--tenant-requests',type=Path);ap.add_argument('--tenant-chunk-bytes',type=int,default=96)
    ap.add_argument('--prebuilt-dir',type=Path)
    a=ap.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    os.environ['GAR_TRIAL_DIR']=str(out)
    cfg={**DEFAULT,**json.loads(a.config.read_text())};policy=POLICY_MODES.get(a.policy,a.policy)
    cfg['predictive_controller']=policy=='predictive'
    if a.policy in POLICY_MODES and cfg['gate_eta']<0:raise ValueError('Enabled gate required')
    if cfg['capacity_bytes_per_ms']<=0 or cfg['step_ms']<1 or cfg['delay_ms']<1 or cfg['guardband_ms']<0:raise ValueError('Invalid runtime configuration')
    if cfg['maximum_units']+sum(cfg['background_units'])>cfg['units']:raise ValueError('Floors exceed capacity')
    if not 0<=cfg.get('tenant_release_fraction',0)<=.8:raise ValueError('Invalid declared tenant release fraction')
    trace=read_trace(a.trace,a.duration_ms)
    shutil.copy2(a.trace,out/'offered_packets.csv')
    if 'source_type_deadlines' not in cfg:
        cfg['source_type_deadlines']=[min((r['deadline_ms']-r['arrival_ms'] for r in trace if r['type_id']==k),default=100) for k in range(7)]
    requests=[];tenant_packets=[]
    if a.tenant_requests:
        requests,tenant_packets=prepare_requests(a.tenant_requests,cfg,len(trace),a.duration_ms,a.tenant_chunk_bytes)
        cfg['tenant_requests']=requests;shutil.copy2(a.tenant_requests,out/'offered_requests.csv')
    all_trace=trace+tenant_packets;observation_ms=max(a.duration_ms,max(r['deadline_ms'] for r in all_trace))+a.drain_ms
    source_names=['realtime_gateway.py','realtime_planner_adapter.cpp','realtime_tenants.py','packet_pacer.cpp',
        'controller.hpp','plan_executor.hpp','deadline_certificate.hpp','request_admission.hpp',
        'background_admission_gate.hpp','residual_admission.hpp','instrumentation.py','transaction_guard.py']
    sources=[HERE/name for name in source_names];hashes={p.name:digest(p) for p in sources}
    frozen=out/'frozen_source';frozen.mkdir()
    for source in sources:shutil.copy2(source,frozen/source.name)
    atomic_json(frozen/'sha256.json',hashes);atomic_json(out/'config.json',cfg)
    build_hashes={p.name:digest(p) for p in sources if p.suffix in {'.cpp','.hpp'}}
    for target in ['realtime_planner_adapter','packet_pacer']:
        if a.prebuilt_dir:
            build=json.loads((a.prebuilt_dir/'build_receipt.json').read_text())
            if build['source_sha256']!=build_hashes:raise ValueError('Prebuilt source mismatch')
            if digest(a.prebuilt_dir/target)!=build['binary_sha256'][target]:raise ValueError('Prebuilt binary mismatch')
            shutil.copy2(a.prebuilt_dir/target,out/target)
        else:
            result=subprocess.run(['c++','-O2','-std=c++17',str(HERE/(target+'.cpp')),'-o',str(out/target)],capture_output=True,text=True)
            (out/(target+'_build.log')).write_text(result.stdout+result.stderr)
            if result.returncode:raise RuntimeError('Compile failed: '+target)
    receipt={'status':'STARTED','started_unix':time.time(),'start_clocks':clocks(),'boot_identity':boot_identity(),
        'trace_sha256':digest(a.trace),'source_sha256':hashes,'policy':a.policy,'duration_ms':a.duration_ms,
        'observation_ms':observation_ms,'controller_stop_ms':a.controller_stop_ms,'config':cfg,
        'platform':platform.platform(),'python':platform.python_version(),'cpu_count':os.cpu_count(),
        'load_average_before':list(os.getloadavg()),'google_cloud_used':False,'privilege_changes':False,
        'tenant_release_scope':'Wire requests bind actual work_bytes and grouped release_schedule. The gateway validates every release suffix against the declared rate-latency model before accepting; local materialization follows the same packet release offsets.',
        'packet_size_semantics':'Complete UDP payload including8-byte wireID; protocol headers outside application bottleneck',
        'compiler':subprocess.check_output(['c++','--version'],text=True).splitlines()[0],
        'binary_sha256':{name:digest(out/name) for name in ['realtime_planner_adapter','packet_pacer']}}
    if a.tenant_requests:receipt['tenant_requests_sha256']=digest(a.tenant_requests)
    atomic_json(out/'receipt.json',receipt)
    context=mp.get_context('spawn');start=context.Value('q',0);ready=context.Queue();processes=[];stopped=False;origin=None
    error=None;summary=None
    fields=['phase','monotonic_ns','wall_ns','continuous_ns','read_span_ns']
    witness=DurableCSV(out/'clock_witness.csv',fields)
    try:
        with tempfile.TemporaryDirectory(prefix='dca-') as temporary:
            gp=str(Path(temporary)/'g');cp=str(Path(temporary)/'c');bp=str(Path(temporary)/'b')
            def launch(name,target,args):
                child=context.Process(name=name,target=run_role,args=(name,target,args,out));child.start();processes.append(child)
                role,value=ready.get(timeout=5)
                if role!=name:raise RuntimeError('Unexpected startup role '+role)
                return child,value
            _,sink=launch('sink',sink_main,(all_trace,start,ready,out,observation_ms))
            _,ingress=launch('gateway',gateway_main,(all_trace,policy,cfg,out/'realtime_planner_adapter',sink,gp,cp,start,ready,out,observation_ms))
            controller,_=launch('controller',controller_main,(policy,cfg,out/'realtime_planner_adapter',gp,cp,start,ready,out,observation_ms))
            if requests:launch('broker',broker_main,(requests,gp,bp,start,ready,out,observation_ms))
            launch('source',source_main,(trace,ingress,start,ready,out))
            start.value=time.monotonic_ns()+200_000_000;origin=start.value
            receipt.update(origin_monotonic_ns=origin,process_ids={p.name:p.pid for p in processes})
            atomic_json(out/'receipt.json',receipt)
            deadline=continuous_ns()+int((observation_ms/1000+5.2)*1e9);next_clock=0
            while any(p.is_alive() for p in processes) and continuous_ns()<deadline:
                now=time.monotonic_ns()
                if now>=next_clock:
                    witness.write({'phase':'measurement',**clocks()});next_clock=now+20_000_000
                if a.controller_stop_ms is not None and not stopped and now>=origin+a.controller_stop_ms*1_000_000:
                    receipt['controller_stop_actual_ns']=time.monotonic_ns();receipt['controller_stop_clocks']=clocks()
                    os.killpg(controller.pid,signal.SIGTERM);stopped=True
                    receipt['controller_stop_signal_return_ns']=time.monotonic_ns();atomic_json(out/'receipt.json',receipt)
                time.sleep(.001)
            for child in processes:child.join(timeout=.05)
            if any(p.is_alive() for p in processes):raise TimeoutError('Parent continuous-clock deadline exceeded: '+','.join(p.name for p in processes if p.is_alive()))
            exits={p.name:p.exitcode for p in processes}
            if any(code!=0 and not (name=='controller' and stopped and code in (143,-15)) for name,code in exits.items()):
                raise RuntimeError('Child failure: '+str(exits))
            for role in ['source','sink','gateway','broker'] if requests else ['source','sink','gateway']:
                marker=json.loads((out/(role+'_completion.json')).read_text())
                if marker['status']!='COMPLETE':raise RuntimeError('Missing complete role '+role)
            for name in ['source.csv','sink.csv']:
                marker=json.loads((out/(name+'.completion.json')).read_text())
                if not marker['complete'] or marker['sha256']!=digest(out/name):raise RuntimeError('Incomplete independent log '+name)
            if len(csv_data(out/'source.csv'))!=len(trace):raise RuntimeError('Source did not account for every offered packet')
            summary=summarize(trace,policy,cfg,out,origin,a.duration_ms,observation_ms);summary['policy']=a.policy
            summary['controller_stop_actual_ns']=receipt.get('controller_stop_actual_ns')
            summary['controller_process_was_terminated']=stopped
            if requests:summary['tenants']=summarize_tenants(requests,tenant_packets,out,origin)
            atomic_json(out/'summary.json',summary);receipt['status']='COMPLETE'
    except BaseException:
        error=traceback.format_exc();receipt['status']='FAILED_INTEGRATION';receipt['error']=error
    finally:
        # Terminate whole per-role groups, including adapter and pacer descendants.
        for child in processes:
            if child.is_alive():
                try:os.killpg(child.pid,signal.SIGTERM)
                except ProcessLookupError:
                    if child.is_alive():child.terminate()
        for child in processes:
            child.join(timeout=1)
            if child.is_alive():
                try:os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError:
                    if child.is_alive():child.kill()
                child.join(timeout=1)
            # A descendant can outlive a Python role that failed before cleanup.
            try:os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        witness.write({'phase':'end',**clocks()});witness.close(error is None)
        receipt.update(finished_unix=time.time(),end_clocks=clocks(),process_exitcodes={p.name:p.exitcode for p in processes},
                       processes_still_alive=[p.name for p in processes if p.is_alive()],
                       load_average_after=list(os.getloadavg()),source_unchanged=all(digest(p)==hashes[p.name] for p in sources))
        if receipt['processes_still_alive']:receipt['status']='FAILED_INTEGRATION'
        if not receipt['source_unchanged']:receipt['status']='FAILED_INTEGRATION';receipt['source_error']='Source changed during trial'
        receipt['record_sha256']={p.name:digest(p) for p in out.iterdir() if p.is_file() and p.suffix in {'.csv','.jsonl','.json','.stderr'} and p.name!='receipt.json'}
        atomic_json(out/'receipt.json',receipt)
    if error:print(error,file=sys.stderr)
    print(json.dumps({'status':receipt['status'],'policy':a.policy,'controller_stopped':stopped,'output':str(out)}))
    if receipt['status']!='COMPLETE':raise SystemExit(1)


if __name__=='__main__':main()
