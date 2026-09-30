"""Actual finite-tenant packet generation, pacing shares, and sink accounting."""
import csv
import json
import math
from pathlib import Path
import select
import socket
import time
from decimal import Decimal
from transaction_guard import request_contract


def prepare_requests(path,cfg,first_wire,duration,chunk_bytes=96):
    with Path(path).open() as f:raw=list(csv.DictReader(f))
    requests=[];packets=[];seen=set()
    if chunk_bytes<8 or chunk_bytes>65000:raise ValueError('Invalid tenant chunk size')
    theta=int(cfg.get('tenant_theta_ms',0));headroom=float(cfg.get('tenant_headroom',1.0))
    if theta<0 or not 0<headroom<=1:raise ValueError('Invalid empirical tenant service contract')
    rate_capacity=int(Decimal(str(cfg['capacity_bytes_per_ms']))*Decimal(str(headroom)))
    for index,r in enumerate(raw):
        key=str(r['id']);t=int(r['request_ms']);q=int(r['units']);L=int(r['duration_ms'])
        if key in seen or not 0<=t<duration or not 0<q<=cfg['units'] or L<1:raise ValueError('Invalid tenant request')
        seen.add(key);start=t+cfg['delay_ms'];end=start+L
        nominal=L*(cfg['capacity_bytes_per_ms']*q//cfg['units'])
        work=max(0,L-theta)*(rate_capacity*q//cfg['units'])
        if L>int(cfg.get('tenant_max_duration_ms',500)):work=0
        supported=work>=8 and L>theta and L<=int(cfg.get('tenant_max_duration_ms',500))
        count=math.ceil(work/chunk_bytes) if supported else 0
        small,extra=divmod(work,count) if count else (0,0)
        if count and small<8:raise ValueError('Tenant packet smaller than its trace identifier')
        wires=[];offsets=[];release_batches={}
        for n in range(count):
            wire=first_wire+len(packets);wires.append(wire)
            fraction=float(cfg.get('tenant_release_fraction',0.0))
            phase=([0.0,.2,.5,.8][min(3,4*n//count)] if fraction and cfg.get('tenant_release_pattern')=='four_batches' else fraction*n/max(1,count-1))
            offset=int(phase*L);offsets.append(offset)
            release_batches[offset]=release_batches.get(offset,0)+small+(n<extra)
            packets.append({'wire_id':wire,'class_id':3+index,'size_bytes':small+(n<extra),
                            'arrival_ms':start,'deadline_ms':end,'source':'tenant:'+key,'id':str(n),'tenant_id':key})
        requests.append({'id':key,'request_ms':t,'start_ms':start,'end_ms':end,'duration_ms':L,
                         'units':q,'work_bytes':work,'nominal_request_bytes':nominal,'supported':supported,'headroom':headroom,'theta_ms':theta,
                         'rate_capacity_bytes_per_ms':rate_capacity,'queue_id':3+index,'wire_ids':wires,
                         'packet_release_offsets':offsets,'release_schedule':sorted([t,b] for t,b in release_batches.items())})
    return requests,packets


def broker_main(requests,gateway_path,broker_path,start,ready,out,observation_ms):
    sock=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM);sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,65536)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,131072);sock.bind(broker_path);sock.setblocking(False)
    ready.put(('broker',None))
    while not start.value:time.sleep(.001)
    origin=start.value;end=origin+observation_ms*1_000_000;ordered=sorted(requests,key=lambda r:(r['request_ms'],r['id']));i=0;awaiting={}
    with (out/'broker.jsonl').open('w') as f:
        def log(x):f.write(json.dumps(x,separators=(',',':'))+'\n');f.flush()
        while time.monotonic_ns()<end:
            now=time.monotonic_ns()
            while i<len(ordered) and now>=origin+ordered[i]['request_ms']*1_000_000:
                r=ordered[i];message={'type':'admit',**request_contract(r),'transmit_ns':time.monotonic_ns()}
                try:sock.sendto(json.dumps(message).encode(),gateway_path);log({**message,'request_ms':r['request_ms']})
                except OSError as e:log({'type':'send_error','id':r['id'],'error':str(e),'actual_ns':time.monotonic_ns()})
                awaiting[r['id']]=(message,now+5_000_000)
                i+=1
            for key,(message,next_retry) in list(awaiting.items()):
                if now>=origin+message['start_ms']*1_000_000:
                    awaiting.pop(key);continue
                if now>=next_retry:
                    message={**message,'transmit_ns':time.monotonic_ns()}
                    try:sock.sendto(json.dumps(message).encode(),gateway_path);log({**message,'retry':True})
                    except OSError as e:log({'type':'send_error','id':key,'error':str(e),'actual_ns':time.monotonic_ns()})
                    awaiting[key]=(message,now+5_000_000)
            target=min(end,origin+ordered[i]['request_ms']*1_000_000 if i<len(ordered) else end)
            readable,_,_=select.select([sock],[],[],max(0,min(.001,(target-time.monotonic_ns())/1e9)))
            if readable:
                while True:
                    try:data,_=sock.recvfrom(65536)
                    except BlockingIOError:break
                    response=json.loads(data);awaiting.pop(str(response.get('id')),None)
                    log({**response,'broker_receive_ns':time.monotonic_ns()})
    sock.close()


class TenantPacketScheduler:
    """Same serial server, with an explicit queue/floor for every finite tenant."""
    def __init__(self,cfg,requests,origin,priority=False):
        self.cfg=cfg;self.requests=requests;self.origin=origin;self.priority=priority
        self.credit=[0.]*(3+len(requests));self.cursor=0;self.last_active=set()
        self.floors=[0,*self.cfg['background_units']]+[r['units'] for r in self.requests]
    def choose(self,queues,units,now,nonempty=None):
        n=len(queues)
        active=[k for k in (nonempty if nonempty is not None else range(n)) if queues[k] and (k<3 or self.origin+self.requests[k-3]['start_ms']*1_000_000<=now<self.origin+self.requests[k-3]['end_ms']*1_000_000)]
        active_set=set(active)
        for k in self.last_active-active_set:self.credit[k]=0
        self.last_active=active_set
        if not active:return None
        # Integer byte quotas match the shared certificate; packetization and
        # OS scheduling still make delivered service empirical.
        C=self.cfg['capacity_bytes_per_ms'];N=self.cfg['units']
        shares={k:(C*(units if k==0 else self.floors[k])//N)/C for k in active};spare=max(0.,1-sum(shares.values()))
        weights={k:(self.cfg['spare_weights'][k] if k<3 else 0) for k in active};weight=sum(weights.values())
        if self.priority and 0 in active:shares[0]+=spare
        elif weight:
            for k in active:shares[k]+=spare*weights[k]/weight
        else:
            for k in active:shares[k]+=spare/len(active)
        ordered=sorted(active,key=lambda k:(k-self.cursor)%n)
        for _ in range(100000):
            for k in ordered:
                if k in shares and self.credit[k]+1e-9>=queues[k][0][2]['size_bytes']:
                    self.credit[k]-=queues[k][0][2]['size_bytes'];self.cursor=(k+1)%n;return k
            for k in active:self.credit[k]+=self.cfg['capacity_bytes_per_ms']*shares[k]
        raise RuntimeError('Tenant packet scheduler made no progress')


def summarize_tenants(requests,packets,out,origin):
    def read(name):
        with (out/name).open() as f:return list(csv.DictReader(f))
    commands=read('gateway_commands.csv');gateway={int(r['wire_id']):r for r in read('gateway_packets.csv')}
    sink={}
    for r in read('sink.csv'):
        if r['valid']=='1':sink.setdefault(int(r['wire_id']),r)
    decisions={str(r['id']):r for r in commands if r.get('type')=='admission_ack'}
    by_wire={r['wire_id']:r for r in packets};jobs=[];packet_rows=[]
    for job in requests:
        d=decisions.get(job['id'],{});accepted=d.get('accepted')=='True';deadline=origin+job['end_ms']*1_000_000
        received=on_time=0;complete_at=0
        for wire in job['wire_ids']:
            packet=by_wire[wire];g=gateway.get(wire,{});s=sink.get(wire,{})
            rx=int(s['received_ns']) if s else None;received+=packet['size_bytes'] if rx else 0
            on_time+=packet['size_bytes'] if rx and rx<=deadline else 0
            if rx:complete_at=max(complete_at,rx)
            packet_rows.append({**packet,'accepted_job':accepted,'admission_ns':d.get('admission_ns',''),
                 'gateway_status':g.get('status','not_materialized' if not accepted else 'missing_gateway_record'),
                 'sink_receive_ns':rx or '', 'original_deadline_ns':deadline,
                 'on_time':int(accepted and rx is not None and rx<=deadline),'missing':int(accepted and rx is None),
                 'late':int(accepted and rx is not None and rx>deadline)})
        jobs.append({k:v for k,v in job.items() if k!='wire_ids'}|{'accepted':accepted,'reason':d.get('reason','no_admission_response'),
             'admission_ns':d.get('admission_ns',''),'received_bytes':received,'on_time_bytes':on_time,
             'completed':accepted and received==job['work_bytes'],'completed_on_time':accepted and on_time==job['work_bytes'],
             'completion_ns':complete_at if accepted and received==job['work_bytes'] else '',
             'source_semantics':'Payload is materialized at actual admission; service eligibility and deadline use the original requested window'})
    def write(name,data):
        with (out/name).open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=sorted({k for r in data for k in r}));w.writeheader();w.writerows(data)
    write('tenant_job_ledger.csv',jobs);write('tenant_packet_ledger.csv',packet_rows)
    accepted=[j for j in jobs if j['accepted']]
    nominal_total=sum(j['nominal_request_bytes'] for j in jobs)
    summary={'offered_jobs':len(jobs),'supported_offered_jobs':sum(j['supported'] for j in jobs),
             'unsupported_offered_jobs':sum(not j['supported'] for j in jobs),
             'accepted_jobs':len(accepted),'completed_jobs':sum(j['completed'] for j in jobs),
             'completed_on_time_jobs':sum(j['completed_on_time'] for j in jobs),
             'admitted_job_failures':sum(not j['completed_on_time'] for j in accepted),
             'offered_work_bytes':sum(j['work_bytes'] for j in jobs),'admitted_work_bytes':sum(j['work_bytes'] for j in accepted),
             'sink_on_time_work_bytes':sum(j['on_time_bytes'] for j in accepted),
             'completed_on_time_work_bytes':sum(j['work_bytes'] for j in jobs if j['completed_on_time']),
             'nominal_requested_work_bytes':nominal_total,
             'sink_on_time_fraction_of_nominal_work':sum(j['on_time_bytes'] for j in accepted)/nominal_total if nominal_total else None,
             'contract_work_fraction_of_nominal':sum(j['work_bytes'] for j in jobs)/nominal_total if nominal_total else None,
             'on_time_admitted_job_fraction':sum(j['completed_on_time'] for j in accepted)/len(accepted) if accepted else None,
             'work_definition':'max(0,duration_ms-theta_ms) * floor(floor(capacity_bytes_per_ms*headroom) * units / total_units)',
             'scope':'Actual tenant UDP sink bytes through the shared nonpreemptive server; no hard real-time guarantee'}
    assert all(j['received_bytes']<=j['work_bytes'] for j in jobs)
    assert all(not j['completed_on_time'] or j['accepted'] for j in jobs)
    (out/'tenant_summary.json').write_text(json.dumps(summary,indent=2)+'\n');return summary
