#!/usr/bin/env python3
"""Sequential runtime follow-up; every started attempt remains in the registry."""
import argparse,ctypes,hashlib,json,os,platform,signal,subprocess,sys,time,traceback
from pathlib import Path
CODE=Path(__file__).resolve().parent;ENGINE=CODE/'engine';ROOT=CODE.parents[1]
sys.path.insert(0,str(ENGINE))
from instrumentation import atomic_json,clocks,boot_identity,continuous_ns
WATCHDOG_OVERHEAD_SECONDS=45.0
ROLES={'source','sink','gateway','controller','broker'}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def sources():return {p.name:sha(p) for p in sorted(ENGINE.iterdir()) if p.suffix in {'.py','.cpp','.hpp'}}

def verify_identity(protocol,plan,identity,build=None):
    if sha(__file__)!=identity['runner_sha256']:raise ValueError('Frozen runner changed')
    if sha(protocol)!=identity['protocol_sha256']:raise ValueError('Frozen protocol changed')
    if sources()!=identity['source_sha256']:raise ValueError('Frozen engine source changed')
    companion=protocol.parent/'PREMEASUREMENT_CLARIFICATIONS.json'
    if (sha(companion) if companion.exists() else None)!=identity['clarifications_sha256']:
        raise ValueError('Premeasurement clarification companion changed')
    if companion.exists() and json.loads(companion.read_text()).get('protocol_sha256')!=identity['protocol_sha256']:
        raise ValueError('Clarifications refer to a different protocol')
    for rel,digest in identity['input_sha256'].items():
        if sha(protocol.parent/rel)!=digest:raise ValueError('Changed input '+rel)
    if sha(CODE/'prepare_targeted.py')!=identity['generator_sha256']:raise ValueError('Changed protocol generator')
    for rel,digest in identity['transferred_config_sha256'].items():
        if sha(ROOT/rel)!=digest:raise ValueError('Changed transferred configuration '+rel)
    if build is not None:
        receipt=json.loads((build/'build_receipt.json').read_text())
        if sha(build/'build_receipt.json')!=identity['build_receipt_sha256']:raise ValueError('Changed build receipt')
        for name,digest in receipt['binary_sha256'].items():
            if sha(build/name)!=digest:raise ValueError('Changed built executable '+name)


if sys.platform=='darwin':
    # Layout from the Darwin SDK sys/proc_info.h, struct proc_bsdinfo.
    class BSDInfo(ctypes.Structure):
        _fields_=[(n,ctypes.c_uint32) for n in ['flags','status','xstatus','pid','ppid','uid','gid','ruid','rgid','svuid','svgid','reserved']]+[('comm',ctypes.c_char*16),('name',ctypes.c_char*32)]+[(n,ctypes.c_uint32) for n in ['nfiles','pgid','jobc','tdev','tpgid']]+[('nice',ctypes.c_int32),('sec',ctypes.c_uint64),('usec',ctypes.c_uint64)]
    _proc=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
    _proc.proc_pidinfo.argtypes=[ctypes.c_int,ctypes.c_int,ctypes.c_uint64,ctypes.c_void_p,ctypes.c_int]
    _proc.proc_pidinfo.restype=ctypes.c_int


def process_identity(pid):
    """Query only a recorded PID; no process inventory or command lines."""
    try:
        if sys.platform=='darwin':
            b=BSDInfo();n=_proc.proc_pidinfo(pid,3,0,ctypes.byref(b),ctypes.sizeof(b))
            if n!=ctypes.sizeof(b) or b.status==5:return None
            return {'pid':pid,'ppid':b.ppid,'pgid':b.pgid,'start_identity':f'{b.sec}.{b.usec:06d}'}
        if sys.platform.startswith('linux'):
            fields=(Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()
            if fields[0]=='Z':return None
            return {'pid':pid,'ppid':int(fields[1]),'pgid':int(fields[2]),'start_identity':fields[19]}
        raise RuntimeError('Verified process identity requires Darwin or Linux')
    except (ValueError,FileNotFoundError,ProcessLookupError,PermissionError):return None


class OwnedProcesses:
    """Only this Popen child and roles/descendants registered by this fresh trial.

    A recorded start identity and group are checked again before signaling.
    No process-name matching or global process termination is used.
    """
    def __init__(self,process,folder):
        self.process=process;self.folder=folder;self.roles={};self.members={}
    def collect(self):
        for role in sorted(ROLES):
            marker=self.folder/(role+'_started.json')
            if role not in self.roles and marker.exists():
                try:
                    m=json.loads(marker.read_text());pid=int(m['pid']);pgid=int(m['pgid'])
                    live=process_identity(pid)
                    if m['role']!=role or pid<=1 or pgid!=pid or not live:continue
                    if live['ppid']!=self.process.pid or live['pgid']!=pid:continue
                    self.roles[role]=pid;self.members[pid]={**live,'role':role,'kind':'role'}
                except (ValueError,KeyError,json.JSONDecodeError,FileNotFoundError):continue
            # Registered children inherit their role's process group. The role
            # registration was verified as a direct child of this engine.
            if role in self.roles:
                journal=self.folder/(role+'_children.jsonl')
                if not journal.exists():continue
                for line in journal.read_text().splitlines():
                    try:
                        m=json.loads(line);pid=int(m['pid'])
                        if m.get('event')!='spawn' or pid in self.members or pid<=1:continue
                        live=process_identity(pid)
                        if live and live['pgid']==self.roles[role] and live['ppid']==self.roles[role]:
                            self.members[pid]={**live,'role':role,'kind':m.get('kind','registered_child')}
                    except (ValueError,KeyError,json.JSONDecodeError):continue
    def same_live(self,old):
        now=process_identity(old['pid'])
        return now if now and now['pgid']==old['pgid'] and now['start_identity']==old['start_identity'] else None
    def cleanup(self):
        self.collect();events=[]
        for sig in (signal.SIGTERM,signal.SIGKILL):
            groups={x['pgid'] for x in self.members.values() if self.same_live(x)}
            for group in sorted(groups):
                try:os.killpg(group,sig);events.append({'pgid':group,'signal':int(sig),'scope':'verified_recorded_role_group'})
                except ProcessLookupError:pass
            if self.process.poll() is None:
                # Popen owns this unreaped direct child, whose new session/group
                # was created by start_new_session=True before its exec.
                try:os.killpg(self.process.pid,sig);events.append({'pgid':self.process.pid,'signal':int(sig),'scope':'owned_engine_group'})
                except ProcessLookupError:
                    if self.process.poll() is None:self.process.send_signal(sig)
            end=continuous_ns()+500_000_000
            while continuous_ns()<end and self.process.poll() is None:time.sleep(.01)
        try:self.process.wait(timeout=.5)
        except subprocess.TimeoutExpired:events.append({'error':'direct engine child did not reap within bound'})
        survivors=[x for x in self.members.values() if self.same_live(x)]
        return {'signals':events,'recorded_members':list(self.members.values()),'verified_survivors':survivors,
                'engine_exitcode':self.process.poll(),'cleanup_complete':not survivors and self.process.poll() is not None}


def run_attempt(command,folder,log_path,watchdog_seconds,on_spawn):
    process=None;owned=None;status='RUNNER_EXCEPTION';error=None;cleanup=None;timed_out=False
    try:
        with log_path.open('w') as f:
            process=subprocess.Popen(command,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
            owned=OwnedProcesses(process,folder);on_spawn(process.pid)
            end=continuous_ns()+int(watchdog_seconds*1e9);next_collect=0
            while process.poll() is None:
                now=continuous_ns()
                if now>=end:timed_out=True;status='OUTER_WATCHDOG_TIMEOUT';break
                if now>=next_collect:owned.collect();next_collect=now+100_000_000
                time.sleep(.02)
            if not timed_out:status='EXITED'
    except BaseException:
        error=traceback.format_exc();status='RUNNER_EXCEPTION'
    finally:
        if owned:cleanup=owned.cleanup()
    return {'runner_status':status,'exitcode':process.poll() if process else None,'runner_error':error,
            'watchdog_seconds':watchdog_seconds,'cleanup':cleanup}


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--protocol',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--prepare-only',action='store_true');ap.add_argument('--tracked-work-paused',action='store_true');ap.add_argument('--limit',type=int)
    a=ap.parse_args()
    def terminated(signum,frame):raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,terminated)
    if a.limit is not None and a.limit<1:raise ValueError('--limit must be positive')
    protocol=a.protocol.resolve();plan=json.loads(protocol.read_text());base=protocol.parent
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    identity={'protocol_sha256':sha(protocol),'source_sha256':sources(),'runner_sha256':sha(__file__),
              'clarifications_sha256':sha(base/'PREMEASUREMENT_CLARIFICATIONS.json') if (base/'PREMEASUREMENT_CLARIFICATIONS.json').exists() else None,
              'input_sha256':plan['input_sha256'],'generator_sha256':plan['generator_sha256'],
              'transferred_config_sha256':plan['transferred_config_sha256'],
              'provenance_scope':'Generator and transferred configurations verified, as well as every executed input.',
              'watchdog_overhead_seconds':WATCHDOG_OVERHEAD_SECONDS,
              'stage':plan['stage'],'boot_identity':boot_identity(),'created_clocks':clocks(),
              'platform':platform.platform(),'python':platform.python_version()}
    attempts=[];fatal=None;final_verified=False;build=None;prepared=False
    selected=plan['trials'][:a.limit] if a.limit is not None else plan['trials']
    def registry():
        atomic_json(out/'attempt_registry.json',{'attempts':attempts,'planned':len(plan['trials']),
            'planned_names':[j['name'] for j in plan['trials']],
            'not_started_names':[j['name'] for j in plan['trials'] if j['name'] not in {x['name'] for x in attempts}]})
    registry()
    try:
        if process_identity(os.getpid()) is None:raise RuntimeError('Cannot verify owned process identities on this host')
        verify_identity(protocol,plan,identity)
        atomic_json(out/'FROZEN_EXECUTION.json',identity)
        build=out/'build';build.mkdir();build_sources={k:v for k,v in identity['source_sha256'].items() if Path(k).suffix in {'.cpp','.hpp'}}
        for name in ['realtime_planner_adapter','packet_pacer']:
            result=subprocess.run(['c++','-O2','-std=c++17',str(ENGINE/(name+'.cpp')),'-o',str(build/name)],capture_output=True,text=True,timeout=60)
            (build/(name+'.log')).write_text(result.stdout+result.stderr)
            if result.returncode:raise RuntimeError('Compilation failed '+name)
        atomic_json(build/'build_receipt.json',{'source_sha256':build_sources,
            'binary_sha256':{n:sha(build/n) for n in ['realtime_planner_adapter','packet_pacer']},
            'compiler':subprocess.check_output(['c++','--version'],text=True,timeout=5).splitlines()[0],'flags':['-O2','-std=c++17']})
        identity['build_receipt_sha256']=sha(build/'build_receipt.json')
        verify_identity(protocol,plan,identity,build);prepared=True
        atomic_json(out/'FROZEN_EXECUTION.json',identity)
        if not a.prepare_only:
            if not a.tracked_work_paused and plan['stage']!='development_pilot':raise ValueError('Pause tracked competing work before measurements')
            for index,job in enumerate(selected):
                verify_identity(protocol,plan,identity,build)
                folder=out/job['name'];command=[sys.executable,str(ENGINE/'realtime_gateway.py'),'--trace',str(base/job['trace']),
                    '--config',str(base/job['config']),'--policy',job['policy'],'--duration-ms',str(job['duration_ms']),
                    '--output',str(folder),'--prebuilt-dir',str(build)]
                if job['requests']:command+=['--tenant-requests',str(base/job['requests'])]
                if job['controller_stop_ms'] is not None:command+=['--controller-stop-ms',str(job['controller_stop_ms'])]
                record={'index':index,'name':job['name'],'job':job,'started':clocks(),'status':'STARTED','exitcode':None}
                attempts.append(record);registry();atomic_json(out/'active_attempt.json',record)
                print(f"ATTEMPT {index+1}/{len(selected)} {job['name']}",flush=True)
                def spawned(pid):
                    record['engine_pid']=pid;record['spawned_clocks']=clocks();registry();atomic_json(out/'active_attempt.json',record)
                try:
                    result=run_attempt(command,folder,out/(job['name']+'.log'),job['duration_ms']/1000+WATCHDOG_OVERHEAD_SECONDS,spawned)
                    record.update(result);receipt_path=folder/'receipt.json'
                    engine_receipt=json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
                    record.update(engine_status=engine_receipt.get('status','NO_ENGINE_RECEIPT'),
                        receipt_sha256=sha(receipt_path) if receipt_path.exists() else None)
                    record['status']=record['engine_status'] if result['runner_status']=='EXITED' else result['runner_status']
                    if record['status']=='COMPLETE' and record['exitcode']!=0:record['status']='NONZERO_ENGINE_EXIT'
                    if result['cleanup'] and not result['cleanup']['cleanup_complete']:record['status']='FAILED_PROCESS_CLEANUP'
                    verify_identity(protocol,plan,identity,build);record['post_attempt_identity_verified']=True
                    if result['runner_status']=='RUNNER_EXCEPTION':raise RuntimeError(result['runner_error'])
                except BaseException:
                    record['status']='RUNNER_OR_PROVENANCE_FAILURE';record['error']=traceback.format_exc();raise
                finally:
                    record['finished']=clocks();registry();atomic_json(out/'active_attempt.json',record)
    except BaseException:
        fatal=traceback.format_exc()
    finally:
        try:
            verify_identity(protocol,plan,identity,build if prepared else None);final_verified=True
        except BaseException:
            fatal=(fatal or '')+'\nFINAL IDENTITY FAILURE\n'+traceback.format_exc()
        registry()
        completed=sum(x['status']=='COMPLETE' and x['exitcode']==0 for x in attempts)
        all_accounted=len(attempts)==len(plan['trials']) and all(x['status']!='STARTED' for x in attempts)
        status='FAILED_RUNNER_OR_PROVENANCE' if fatal else ('PREPARED_ONLY' if a.prepare_only else ('ALL_ATTEMPTS_ACCOUNTED' if all_accounted else 'LIMITED_EXECUTION'))
        result={'status':status,'attempts':len(attempts),'planned':len(plan['trials']),'complete_executions':completed,
            'failed_executions':len(attempts)-completed,'full_protocol_executed':bool(all_accounted and not fatal),
            'execution_identity':identity,'final_identity_verified':final_verified,'error':fatal,'finished_clocks':clocks(),
            'scope':'All scheduled attempts invoked is distinct from completed measurements. Accounting is not evidence of fulfilled deadlines or confirmed scientific claims.'}
        atomic_json(out/'receipt.json',result)
    print(json.dumps({k:result[k] for k in ['status','attempts','complete_executions','failed_executions','full_protocol_executed']}),flush=True)
    if fatal:raise SystemExit(1)
if __name__=='__main__':main()
