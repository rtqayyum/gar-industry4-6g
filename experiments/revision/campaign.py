#!/usr/bin/env python3
"""Frozen numerical revision campaign. No automatic claim of reviewer acceptance."""
from __future__ import annotations
import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor,as_completed
import csv
from datetime import datetime,timezone
import gzip
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
import sys
import time
import traceback
import zipfile
CODE=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
FAMILIES=["deadline_mix","heavy_tenants","regime_shift","synchronized"]
METHODS=["peak","keep_calendar","minimum_floor","gar","gar_h"]
ADAPTERS=["joint_guarded","luu_joint_style","luu_sequential_style","yu_gateway_style"]
BASE=dict(duration=300000,capacity=650,units=106,delay=2,step=5,term=40,window=0,
          background_contract=1,service=1.0,actual_service=1.0,burst=1.0,eta=1.0,price=64.0,history_window=100)
OPEN_WORK={
 "gateway_restart":"Controller outages are tested. Durable gateway restart recovery remains open.",
 "industrial_data":"No independently measured industrial byte-arrival trace is claimed. Current generators are synthetic contracts.",
 "packet_service":"Recorded packet evidence can be verified. A repaired live executor with a defensible service bound remains open.",
 "multihop":"Supply-drop stress is a one-gateway abstraction, not a physical wireless or general multi-hop validation.",
 "manuscript":"Integrate and independently review the proof and detailed algorithm, reconcile notation, references and every original review comment."
}
def stamp():return datetime.now(timezone.utc).isoformat()
def read(p):return json.loads(Path(p).read_text())
def write(p,v):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+".tmp-"+str(os.getpid()))
 tmp.write_text(json.dumps(v,indent=2,sort_keys=True,allow_nan=False)+"\n");os.replace(tmp,p)
def sha(p):
 h=hashlib.sha256()
 with Path(p).open("rb") as f:
  for b in iter(lambda:f.read(1048576),b""):h.update(b)
 return h.hexdigest()
def key(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(",",":")).encode()).hexdigest()[:24]
def task(kind,study,c):
 t=dict(kind=kind,study=study,config=c);t["id"]=key(t);return t
def call(cmd,log,cwd=None,timeout=7200):
 with Path(log).open("a") as f:
  f.write("\nCOMMAND "+json.dumps(list(map(str,cmd)))+"\n");f.flush()
  r=subprocess.run(list(map(str,cmd)),stdout=f,stderr=subprocess.STDOUT,cwd=cwd,timeout=timeout)
 if r.returncode:raise RuntimeError("Command exit %s; see %s"%(r.returncode,log))
def build(root):
 d=root/"build";d.mkdir(exist_ok=True)
 compiler=os.environ.get("CXX") or shutil.which("c++") or shutil.which("g++")
 if not compiler:raise RuntimeError("C++17 compiler required")
 for name,src in {"sustained":HERE/"sustained.cpp","online":CODE/"experiments/online/simulator.cpp","benchmark":HERE/"benchmark.cpp"}.items():
  call([compiler,"-std=c++17","-O2",src,"-o",d/name],d/(name+".build.log"),timeout=300)
 write(d/"identity.json",dict(timestamp=stamp(),python=sys.version,compiler=subprocess.check_output([compiler,"--version"],text=True).splitlines()[0],
  binaries={n:sha(d/n) for n in ["sustained","online","benchmark"]},
  sources={str(p.relative_to(CODE)):sha(p) for p in sorted((CODE/"experiments").rglob("*")) if p.suffix in {".cpp",".hpp",".py"}}))
def primary(seeds=30):
 return [task("sustained","E1",dict(BASE,family=f,load=l,rate=r,seed=s,policy=m,trace=int(s==1000001)))
  for f,l,r,s,m in itertools.product(FAMILIES,[.4,.8,1.0],[10,100],range(1000001,1000001+seeds),METHODS)]
def diagnostics(seeds=10,duration=60000):
 variants=[{}]
 factors={"delay":[0,2,5,10,20,50],"window":[20,100,500,2000],"load":[.4,.8,1,1.4],"burst":[.5,1,1.5,2],
 "service":[1,.95,.9,.8],"step":[5,10,20],"term":[20,40,100,500],"price":[0,16,64,256],
 "eta":[1,1.25,1.5],"history_window":[50,100,200,500],"capacity":[325,650,1300],"units":[53,106,212]}
 for k,values in factors.items():variants += [{k:v} for v in values]
 variants += [dict(delay=d,window=w) for d,w in itertools.product([0,2,5,10,20,50],[20,100,500,2000])]
 variants += [dict(burst=b,service=s) for b,s in itertools.product([.5,1,1.5,2],[1,.95,.9,.8])]
 configs={}
 for f,s,m,v in itertools.product(["deadline_mix","synchronized"],range(200001,200001+seeds),["minimum_floor","gar","gar_h"],variants):
  c=dict(BASE,duration=duration,family=f,seed=s,policy=m,load=.8,rate=100,trace=0);c.update(v)
  if "service" in v:c["actual_service"]=v["service"]
  if "units" in v:c["price"]=64*v["units"]/106
  configs[key(c)]=c
 out=[task("sustained","E3",c) for c in configs.values()]
 for s,m,at,length in itertools.product(range(300001,300001+seeds),METHODS,[duration//4,duration//2],[100,1000,5000]):
  out.append(task("sustained","E5_CONTROLLER_ONLY",dict(BASE,duration=duration,family="synchronized",load=.8,rate=100,seed=s,policy=m,outage_start=at,outage_length=length,trace=0)))
 for s,m,actual,bounded in itertools.product(range(400001,400001+seeds),["minimum_floor","gar","gar_h"],[.95,.9,.8],[True,False]):
  out.append(task("sustained","E6_SERVICE_ABSTRACTION",dict(BASE,duration=duration,family="regime_shift",load=.8,rate=100,seed=s,policy=m,actual_service=actual,service=actual if bounded else 1,trace=0)))
 return sorted(out,key=lambda x:x["id"])
def scaling(seeds=30):
 base=dict(jobs=100,classes=3,horizon=512,floors=54,bookings=100,repeats=30);cases=[base]
 for k,values in {"jobs":[0,10,100,1000,10000],"classes":[1,3,10,30],"horizon":[128,512,2048,8192,32768],"floors":[8,16,32,54],"bookings":[0,10,100,1000,10000]}.items():
  cases += [dict(base,**{k:v}) for v in values]
 unique={key(c):c for c in cases}
 return [task("benchmark","E4",dict(c,seed=s)) for c,s in itertools.product(unique.values(),range(500001,500001+seeds))]
def context(root,c):
 d=root/"contexts";d.mkdir(exist_ok=True);p=d/("%s-%s-%s.csv"%(c["family"],c["seed"],c["duration"]))
 if p.exists():return p
 rng=random.Random(str((c["family"],c["seed"],"v1")));x=[1.,1.,1.];lines=["critical,video,monitoring"]
 for _ in range(math.ceil(c["duration"]/1000)+1):
  x=[max(.1,min(1.4,.9*v+.1+rng.gauss(0,.025))) for v in x];lines.append(",".join("%.8f"%v for v in x))
 tmp=p.with_name(p.name+".tmp-"+str(os.getpid()));tmp.write_text("\n".join(lines)+"\n");os.replace(tmp,p);return p
def audit(folder,p):
 if p["status"]=="configuration_infeasible":return {"status":"PROVISIONING_REJECTION"}
 with (folder/"result.json.tenants.csv").open() as f:tenants=list(csv.DictReader(f))
 deltas=defaultdict(int);floors={};area=0;fail=0
 for x in tenants:
  r={k:int(v) for k,v in x.items()};s,e=r["start_ms"],r["end_ms"];q=r["units"];L=e-s;rate=p["certified_capacity"]*q//p["units"]
  if not 0<=r["offer_ms"]<=s<e<=p["execution_ms"]:raise ValueError("Invalid accepted interval")
  if r["work_bytes"]>rate*L:raise ValueError("Full availability certificate failed")
  if r["release_pattern"]:
   at=L//2 if r["release_pattern"]==1 else L*9//10
   if r["work_bytes"]-r["work_bytes"]//2>rate*(L-at):raise ValueError("Staged availability certificate failed")
  deltas[s]+=q;deltas[e]-=q;area+=q*L
  fail+=bool(r["remaining_bytes"] or not 0<=r["completion_ms"]<=e)
 with (folder/"result.json.calendar.csv").open() as f:
  for r in csv.DictReader(f):floors[int(r["time_ms"])]=int(r["critical_units"])
 if 0 not in floors:raise ValueError("Missing initial calendar")
 rights=0;floor=0
 for t in sorted(set(deltas)|set(floors)):
  rights+=deltas[t];floor=floors.get(t,floor)
  if rights<0 or rights+floor+p["background_units"]>p["units"]:raise ValueError("Independent capacity sweep failed")
 if rights or area!=p["booked_unit_ms"] or len(tenants)!=p["accepted_requests"] or fail!=p["accepted_requests"]-p["timely_tenants"]:raise ValueError("Ledger or completion accounting mismatch")
 trace=folder/"result.json.jobs.csv"
 if trace.exists():
  failed=[0]*6
  with trace.open() as f:
   for r in csv.DictReader(f):
    if int(r["remaining_bytes"]) or not 0<=int(r["completion_ms"])<=int(r["deadline_ms"]):failed[int(r["class_id"])]+=1
  if sum(failed[:3])!=p["critical_failed"] or sum(failed[3:5])!=p["background_failed"]:raise ValueError("Independent deadline audit failed")
 conforming=not p["supply_shortfall_slots"] and not p["envelope_violations"]
 if p["capacity_ledger_violations"] or p["envelope_violations"]:raise ValueError("Resource/envelope invariant failed")
 if conforming and (p["critical_failed"] or fail or (p["protected_background"] and p["background_failed"])):raise ValueError("Commitment failure within claimed assumptions")
 return dict(status="PASS",accepted_intervals=len(tenants),in_contract=conforming,original_trace_checked=trace.exists())
def run_one(t,root):
 root=Path(root);d=root/"tasks"/t["id"];d.mkdir(parents=True,exist_ok=True);receipt=d/"receipt.json"
 if receipt.exists():
  old=read(receipt)
  if old.get("task")==t and old["status"] in {"COMPLETE","PROVISIONING_REJECTION"}:return old
  raise RuntimeError("Previous failed attempt retained: "+str(d))
 r=dict(task=t,status="FAILED",started=stamp(),slurm_job_id=os.environ.get("SLURM_JOB_ID"));begin=time.perf_counter()
 try:
  c=t["config"];out=d/"result.json";kind=t["kind"]
  if kind=="online":
   ctx=context(root,c);cmd=[root/"build/online","--context",ctx,"--output",out,"--family",c["family"],"--policy",c["policy"],
    "--duration",c["duration"],"--seed",c["seed"],"--load",c["load"],"--lease-rate",100,"--step",c["step"],"--delay",2,"--term",40,
    "--gate-eta",c["eta"],"--expire-background",1,"--batch-size",3,"--burst-factor",2.5,"--lease-offer-trace-export",d/"offers.csv"]
   r["context_sha256"]=sha(ctx)
  else:
   cmd=[root/"build"/kind,"--output",out]
   for k,v in c.items():cmd += ["--"+k.replace("_","-"),str(v)]
  write(d/"task.json",t);r["command"]=list(map(str,cmd));call(cmd,d/"execution.log")
  p=read(out);r["payload"]=p;r["result_sha256"]=sha(out)
  if p["status"]=="configuration_infeasible":r["status"]="PROVISIONING_REJECTION"
  else:
   if kind=="sustained":r["audit"]=audit(d,p)
   if kind=="online":
    if p["status"]!="PASS" or any(p.get(k,0) for k in ["critical_failures","capacity_ledger_violations","envelope_violation_endpoints"]):raise ValueError("Online safety check failed")
    r["offer_sha256"]=sha(d/"offers.csv")
   r["status"]="COMPLETE"
  for file in d.glob("*.csv"):
   with file.open("rb") as a,gzip.open(str(file)+".gz","wb") as b:shutil.copyfileobj(a,b)
   file.unlink()
 except Exception as e:r.update(error=str(e),traceback=traceback.format_exc())
 r["elapsed_seconds"]=time.perf_counter()-begin;r["finished"]=stamp();write(receipt,r);return r
def run_many(tasks,root,workers):
 rows=[]
 with ProcessPoolExecutor(max_workers=workers) as pool:
  pending={pool.submit(run_one,t,str(root)):t["id"] for t in tasks}
  for n,f in enumerate(as_completed(pending),1):
   try:rows.append(f.result())
   except Exception as e:rows.append(dict(status="FAILED",id=pending[f],error=str(e)))
   if n%25==0 or n==len(tasks):print(json.dumps(dict(finished=n,total=len(tasks),failed=sum(r["status"]=="FAILED" for r in rows))),flush=True)
 bad=[r for r in rows if r["status"]=="FAILED"]
 if bad:raise RuntimeError("%s tasks failed; inspect retained receipts"%len(bad))
 return rows
def pilot(root,workers,quick=False):
 build(root);n=1 if quick else 3;duration=1000 if quick else 10000
 tasks=[task("sustained","PILOT",dict(BASE,duration=duration,family=f,load=l,rate=r,seed=s,policy=m,trace=1))
  for f,l,r,s,m in itertools.product(["deadline_mix","synchronized"],[.4,1.0],[10,100],range(900001,900001+n),METHODS)]
 write(root/"pilot_manifest.json",tasks);rows=run_many(tasks,root,workers);groups=defaultdict(set)
 for r in rows:
  c=dict(r["task"]["config"]);c.pop("policy");groups[key(c)].add(r["payload"]["input_fingerprint_fnv64"])
 if any(len(v)!=1 for v in groups.values()) or not any(r["status"]=="COMPLETE" for r in rows):raise ValueError("Pilot common-input validation failed")
 probe=task("benchmark","PILOT_BENCHMARK",dict(jobs=100,classes=3,horizon=128,floors=8,bookings=10,repeats=3,seed=800211))
 run_many([probe],root,1)
 write(root/"pilot.json",dict(status="PASS",tasks=len(tasks),timestamp=stamp(),quick=quick,median_seconds=statistics.median(r["elapsed_seconds"] for r in rows)))
def efficiency(p):return 100*(sum(x["on_time_bytes"] for x in p["classes"])+p["lease_served_bytes"])/p["capacity_bytes"]
def bgfail(p):return 100*sum(x["failures"] for x in p["classes"][1:3])/max(1,sum(x["generated"] for x in p["classes"][1:3]))
def calibrate(root,workers,quick=False):
 if read(root/"pilot.json")["status"]!="PASS":raise RuntimeError("Pilot required")
 grid=list(itertools.product([1.,1.25,1.5],[5,10]));families=FAMILIES[:1] if quick else FAMILIES;seeds=range(910001,910002 if quick else 910007)
 tasks=[task("online","E2_CALIBRATION",dict(policy=m,eta=e,step=step,family=f,load=l,seed=s,duration=1000 if quick else 300000))
  for m,(e,step),f,l,s in itertools.product(ADAPTERS,grid,families,[.8,1.0],seeds)]
 write(root/"calibration_manifest.json",tasks);rows=run_many(tasks,root,workers);selected={};unattainable=[]
 for method,target in itertools.product(ADAPTERS,[1,3,5]):
  choices=[]
  for eta,step in grid:
   valid=[r["payload"] for r in rows if r["status"]=="COMPLETE" and r["task"]["config"]["policy"]==method and r["task"]["config"]["eta"]==eta and r["task"]["config"]["step"]==step]
   if valid:
    fail=statistics.mean(map(bgfail,valid))
    if fail<=target:choices.append((statistics.mean(map(efficiency,valid)),-fail,-eta,-step,eta,step))
  label=method+":"+str(target)
  if choices:
   best=max(choices);selected[label]=dict(eta=best[-2],step=best[-1],target_pct=target,calibration_failure_pct=-best[1])
  else:unattainable.append(label)
 write(root/"selected.json",dict(selected=selected,unattainable=unattainable,rule="Select only on development traces; targets are empirical means, not hard service guarantees."))
 evaluate={}
 if not quick:
  for label,v in selected.items():
   method=label.split(":")[0]
   for f,l,s in itertools.product(FAMILIES,[.8,1.0],range(1000001,1000031)):
    t=task("online","E2_EVALUATION",dict(policy=method,eta=v["eta"],step=v["step"],family=f,load=l,seed=s,duration=300000));evaluate[t["id"]]=t
  write(root/"manifest.json",primary()+diagnostics());write(root/"scaling_manifest.json",scaling())
  write(root/"evaluation_manifest.json",sorted(evaluate.values(),key=lambda t:t["id"]))
  write(root/"protocol_lock.json",dict(manifest_sha256=sha(root/"manifest.json"),evaluation_sha256=sha(root/"evaluation_manifest.json"),scaling_sha256=sha(root/"scaling_manifest.json"),
   timestamp=stamp(),test_seed_start=1000001,test_blocks=30,precision_note="Thirty fixed independent blocks; interval widths are reported, not guaranteed.",open_work=OPEN_WORK))
 write(root/"calibration.json",dict(status="PASS",tasks=len(tasks),evaluation_tasks=len(evaluate),quick=quick,timestamp=stamp()))
def worker(root,shard,shards,workers):
 lock=read(root/"protocol_lock.json")
 if read(root/"pilot.json").get("quick") or read(root/"calibration.json").get("quick"):raise RuntimeError("Developer quick runs cannot authorize production")
 for file,k in [("manifest.json","manifest_sha256"),("evaluation_manifest.json","evaluation_sha256")]:
  if sha(root/file)!=lock[k]:raise RuntimeError("Frozen manifest changed")
 tasks=read(root/"manifest.json")+read(root/"evaluation_manifest.json")
 rows=run_many([t for i,t in enumerate(tasks) if i%shards==shard],root,workers)
 write(root/("shard-%s.json"%shard),dict(status="COMPLETE",tasks=len(rows)))
def table(path,rows):
 if not rows:Path(path).write_text("");return
 fields=list(dict.fromkeys(k for r in rows for k in r))
 with Path(path).open("w",newline="") as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
def metrics(r):
 t=r["task"];c=t["config"];p=r["payload"];out=dict(id=t["id"],study=t["study"],kind=t["kind"],status=r["status"],**c)
 if r["status"]!="COMPLETE":return out
 if t["kind"]=="sustained":
  out.update(admission_pct=100*p["accepted_requests"]/max(1,p["offered_requests"]),timely_payload_efficiency_pct=100*p["timely_bytes"]/p["capacity_bytes"],
   timely_tenant_bytes_per_offer=p["timely_tenant_bytes"]/max(1,p["offered_requests"]),critical_failed=p["critical_failed"],
   background_failure_pct=100*p["background_failed"]/max(1,p["background_offered"]),tenant_failed=p["accepted_requests"]-p["timely_tenants"],
   commands=p["plan_commands"],in_contract=int(not p["supply_shortfall_slots"] and not p["envelope_violations"]),input_fingerprint=str(p["input_fingerprint_fnv64"]))
 elif t["kind"]=="online":out.update(admission_pct=100*p["lease_accepted"]/max(1,p["lease_offered"]),timely_payload_efficiency_pct=efficiency(p),background_failure_pct=bgfail(p),commands=p["accepted_plan_commands"],input_fingerprint=r["offer_sha256"])
 else:out.update({k:p[k] for k in ["p50_us","p95_us","max_us","max_rss_kb","construction_us"]})
 return out
def paired(rows):
 d={(r["family"],r["load"],r["rate"],r["seed"],r["policy"]):r for r in rows if r["study"]=="E1" and r["status"]=="COMPLETE"}
 rng=random.Random(74611);effects=[]
 for endpoint,control in itertools.product(["admission_pct","timely_payload_efficiency_pct","timely_tenant_bytes_per_offer"],["peak","keep_calendar","minimum_floor"]):
  blocks=defaultdict(list)
  for k,r in d.items():
   if k[-1]=="gar" and (*k[:-1],control) in d:blocks[k[3]].append(r[endpoint]-d[(*k[:-1],control)][endpoint])
  diffs=[statistics.mean(v) for _,v in sorted(blocks.items())]
  if not diffs:continue
  estimate=statistics.mean(diffs);draws=sorted(statistics.mean(rng.choices(diffs,k=len(diffs))) for _ in range(3000))
  extreme=sum(abs(sum(v*(1 if rng.random()<.5 else -1) for v in diffs)/len(diffs))>=abs(estimate)-1e-12 for _ in range(20000))
  effects.append(dict(endpoint=endpoint,method="gar",control=control,seed_blocks=len(diffs),difference=estimate,ci95_low=draws[74],ci95_high=draws[2924],p=(extreme+1)/20001))
 prior=0
 for i,r in enumerate(sorted(effects,key=lambda r:r["p"])):prior=max(prior,min(1,(len(effects)-i)*r["p"]));r["holm_p"]=prior
 return effects
def collect(root):
 out=root/"report";out.mkdir(exist_ok=True);tasks=[];rows=[];receipts=[];missing=[];failed=[]
 for name in ["manifest.json","evaluation_manifest.json","scaling_manifest.json"]:
  if (root/name).exists():tasks+=read(root/name)
 for t in tasks:
  p=root/"tasks"/t["id"]/"receipt.json"
  if not p.exists():missing.append(t["id"]);continue
  r=read(p);receipts.append(r)
  if r["status"]=="FAILED":failed.append(dict(id=t["id"],error=r.get("error")));continue
  rows.append(metrics(r))
 groups=defaultdict(set)
 for r in rows:
  if r["study"] in {"E1","E2_EVALUATION"} and r.get("input_fingerprint"):groups[(r["study"],r["family"],r["load"],r["seed"],r.get("rate",100))].add(r["input_fingerprint"])
 mismatch=[str(k) for k,v in groups.items() if len(v)>1]
 reproduction=read(root/"reproduction.json") if (root/"reproduction.json").exists() else {"status":"MISSING"}
 calibration=read(root/"calibration.json") if (root/"calibration.json").exists() else {"status":"MISSING"}
 complete=bool(tasks) and not missing and not failed and not mismatch and reproduction["status"]=="COMPLETE" and calibration["status"]=="PASS"
 status="NUMERICAL_CAMPAIGN_COMPLETE" if complete else "INCOMPLETE_OR_FAILED"
 table(out/"metrics.csv",rows);table(out/"paired_effects.csv",paired(rows))
 targets=[]
 if (root/"selected.json").exists():
  sel=read(root/"selected.json")
  for label,v in sel["selected"].items():
   method=label.split(":")[0];rr=[r for r in rows if r["study"]=="E2_EVALUATION" and r["status"]=="COMPLETE" and r["policy"]==method and r["eta"]==v["eta"] and r["step"]==v["step"]]
   blocks=defaultdict(list)
   for r in rr:blocks[r["seed"]].append(r["background_failure_pct"])
   values=[statistics.mean(x) for x in blocks.values()];rng=random.Random(400)
   draws=sorted(statistics.mean(rng.choices(values,k=len(values))) for _ in range(3000)) if values else []
   mean=statistics.mean(values) if values else None;upper=draws[2924] if draws else None
   targets.append(dict(method=method,target_pct=v["target_pct"],seed_blocks=len(values),test_failure_pct=mean,test_ci95_upper=upper,
    met_test_mean=bool(mean is not None and mean<=v["target_pct"]),met_test_upper=bool(upper is not None and upper<=v["target_pct"])))
  targets += [dict(method=x.split(":")[0],target_pct=int(x.split(":")[1]),status="UNATTAINABLE_IN_CALIBRATION") for x in sel["unattainable"]]
 table(out/"reliability_targets.csv",targets)
 coverage=dict(status=status,expected=len(tasks),received=len(receipts),missing=missing,failed=failed,input_mismatches=mismatch,
  provisioning_rejections=sum(r["status"]=="PROVISIONING_REJECTION" for r in receipts),reproduction=reproduction,calibration=calibration,open_work=OPEN_WORK,ready_for_resubmission=False)
 write(out/"coverage.json",coverage)
 lines=["# GAR revision campaign","","Status: "+status,"","Expected tasks: %s; received: %s; missing: %s; failed: %s."%(len(tasks),len(receipts),len(missing),len(failed)),
  "","Numerical completion does not establish that every reviewer comment is resolved. No automatic resubmission is performed.","","| Study | Received | Complete | Provisioning rejected |","|---|---:|---:|---:|"]
 for study in sorted({t["study"] for t in tasks}):
  rr=[r for r in receipts if r["task"]["study"]==study];lines.append("| %s | %s | %s | %s |"%(study,len(rr),sum(r["status"]=="COMPLETE" for r in rr),sum(r["status"]=="PROVISIONING_REJECTION" for r in rr)))
 lines+=["","## Open work",""]+["- "+k+": "+v for k,v in OPEN_WORK.items()]
 lines+=["","Paired effects resample whole independent seed blocks. Missing, failed and infeasible cases remain explicit. Peak and keep-calendar decisions coincide when the installed baseline calendar is constant; this is reported as equivalence, not extra independent evidence."]
 (out/"SUMMARY.md").write_text("\n".join(lines)+"\n")
 try:
  import matplotlib;matplotlib.use("Agg");import matplotlib.pyplot as plt
  main=[r for r in rows if r["study"]=="E1" and r["status"]=="COMPLETE"]
  if main:
   fig,axs=plt.subplots(1,2,figsize=(10,4))
   for ax,metric,label in zip(axs,["admission_pct","timely_payload_efficiency_pct"],["Admissions (%)","Timely payload efficiency (%)"]):
    ax.bar(METHODS,[statistics.mean(r[metric] for r in main if r["policy"]==m) for m in METHODS]);ax.set_ylabel(label);ax.tick_params(axis="x",rotation=25)
   fig.tight_layout();fig.savefig(out/"sustained_summary.png",dpi=180);plt.close(fig)
 except Exception as e:write(out/"plot_error.json",{"error":str(e)})
 with zipfile.ZipFile(root/"report_bundle.zip","w",zipfile.ZIP_DEFLATED) as z:
  for p in out.iterdir():z.write(p,"report/"+p.name)
  for name in ["protocol_lock.json","selected.json","pilot.json","source_identity.json"]:
   if (root/name).exists():z.write(root/name,name)
 print(json.dumps(dict(status=status,summary=str(out/"SUMMARY.md"),bundle=str(root/"report_bundle.zip"))),flush=True)
 return 0 if complete else 3
def reproduce(root):
 d=root/"reproduction";d.mkdir(exist_ok=True);result=dict(status="FAILED",started=stamp())
 try:
  call([sys.executable,CODE/"run.py","verify"],d/"verify.log",cwd=CODE)
  call([sys.executable,CODE/"run.py","check"],d/"tests.log",cwd=CODE,timeout=3600)
  for study in ["matched","online","reserve","formal","gateway","analyze"]:
   extra=["--workers","4"] if study=="online" else []
   call([sys.executable,CODE/"run.py",study,"--output",d/study,*extra],d/(study+".log"),cwd=CODE,timeout=172800)
  call([sys.executable,CODE/"run.py","packet-verify"],d/"packet-verify.log",cwd=CODE)
  result["status"]="COMPLETE"
 except Exception as e:result["error"]=str(e)
 result["finished"]=stamp();write(root/"reproduction.json",result)
 if result["status"]!="COMPLETE":raise RuntimeError(result.get("error"))
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument("command",choices=["pilot","calibrate","worker","scaling","collect","reproduce","describe"])
 p.add_argument("--root",type=Path,required=True);p.add_argument("--workers",type=int,default=4);p.add_argument("--shard",type=int,default=0);p.add_argument("--shards",type=int,default=6);p.add_argument("--quick",action="store_true")
 a=p.parse_args();a.root=a.root.resolve();a.root.mkdir(parents=True,exist_ok=True)
 if not 1<=a.workers<=40 or not 0<=a.shard<a.shards:p.error("Invalid workers or shard")
 if a.command=="pilot":pilot(a.root,a.workers,a.quick)
 elif a.command=="calibrate":calibrate(a.root,a.workers,a.quick)
 elif a.command=="worker":worker(a.root,a.shard,a.shards,a.workers)
 elif a.command=="scaling":
  if sha(a.root/"scaling_manifest.json")!=read(a.root/"protocol_lock.json")["scaling_sha256"]:raise RuntimeError("Scaling manifest changed")
  run_many(read(a.root/"scaling_manifest.json"),a.root,1)
 elif a.command=="collect":sys.exit(collect(a.root))
 elif a.command=="reproduce":reproduce(a.root)
 else:print(json.dumps(dict(primary=len(primary()),diagnostics=len(diagnostics()),scaling=len(scaling()),open_work=OPEN_WORK),indent=2))
if __name__=="__main__":main()
