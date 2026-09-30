#!/usr/bin/env python3
"""Targeted fault checks for lifecycle and IPC observability, not workload results."""
from pathlib import Path
import argparse,hashlib,json,multiprocessing as mp,os,sys,time
CODE=Path(__file__).resolve().parent;sys.path.insert(0,str(CODE/'engine'))
from instrumentation import BoundedAdapter,DurableCSV,run_role,atomic_json
def fail_role(out):
    with DurableCSV(Path(out)/'partial.csv',['id']) as writer:
        writer.write({'id':1});raise RuntimeError('deliberate instrumentation fixture failure')
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    os.environ['GAR_TRIAL_DIR']=str(out)
    child=out/'hung_adapter.py';child.write_text('#!'+sys.executable+'\nimport sys,time\nfor line in sys.stdin:\n time.sleep(5)\n');child.chmod(0o700)
    adapter=BoundedAdapter(child);start=time.monotonic();failed=False
    try:adapter.call('HANG')
    except TimeoutError:failed=True
    elapsed=time.monotonic()-start;adapter.close()
    assert failed and .20<=elapsed<1.0,(failed,elapsed)
    responsive=out/'responsive_adapter.py'
    responsive.write_text('#!'+sys.executable+'\nimport sys,time,json\ntime.sleep(.4)\nfor line in sys.stdin:\n if line.strip()=="QUIT": break\n print(json.dumps({"op":line.split()[0]}),flush=True)\n')
    responsive.chmod(0o700);working=BoundedAdapter(responsive)
    assert working.call('INIT 49')['op']=='INIT'
    assert working.call('ADV 1 0')['op']=='ADV';working.close()
    context=mp.get_context('spawn');p=context.Process(name='fault_fixture',target=run_role,args=('fault_fixture',fail_role,(out,),out));p.start();p.join(timeout=5)
    assert not p.is_alive() and p.exitcode==1
    marker=json.loads((out/'fault_fixture_completion.json').read_text());partial=json.loads((out/'partial.csv.completion.json').read_text())
    assert marker['status']=='FAILED' and not partial['complete'] and partial['rows']==1
    assert partial['sha256']==hashlib.sha256((out/'partial.csv').read_bytes()).hexdigest()
    receipt={'status':'PASS','scope':'Synthetic instrumentation fault fixtures, not a runtime workload','checks':[
        'Hung adapter returns bounded timeout with recorded operation','Failed role persists exit status and exception',
        'Interrupted CSV remains hash-bound and marked incomplete',
        'Cold startup can complete before origin; subsequent operational reply works'],'adapter_timeout_seconds':elapsed,
        'source_sha256':{str(p.relative_to(CODE)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [CODE/'engine/instrumentation.py',Path(__file__)]}}
    atomic_json(out/'receipt.json',receipt);print(json.dumps(receipt,indent=2))
if __name__=='__main__':main()
