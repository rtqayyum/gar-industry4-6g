"""Bounded IPC, incremental evidence and explicit process lifecycle records.

These records improve observability; they do not make the OS a real-time server.
"""
import csv
import ctypes
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time
import traceback

_children=[]
_journals={}

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def atomic_json(path,data):
    path=Path(path);temporary=path.with_name(path.name+'.writing')
    with temporary.open('w') as f:
        json.dump(data,f,indent=2,allow_nan=False);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(temporary,path)

if sys.platform=='darwin':
    _lib=ctypes.CDLL('/usr/lib/libSystem.B.dylib')
    class _Timebase(ctypes.Structure):_fields_=[('numer',ctypes.c_uint32),('denom',ctypes.c_uint32)]
    _scale=_Timebase();_lib.mach_timebase_info(ctypes.byref(_scale))
    _lib.mach_continuous_time.restype=ctypes.c_uint64
    def continuous_ns():return _lib.mach_continuous_time()*_scale.numer//_scale.denom
else:
    def continuous_ns():return time.clock_gettime_ns(getattr(time,'CLOCK_BOOTTIME',time.CLOCK_MONOTONIC))

def clocks():
    before=time.monotonic_ns();wall=time.time_ns();continuous=continuous_ns();after=time.monotonic_ns()
    return {'monotonic_ns':(before+after)//2,'wall_ns':wall,'continuous_ns':continuous,'read_span_ns':after-before}

def boot_identity():
    try:
        if sys.platform=='darwin':value=subprocess.check_output(['sysctl','-n','kern.bootsessionuuid'],text=True).strip()
        else:value=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        return {'boot_id_sha256':hashlib.sha256(value.encode()).hexdigest(),'available':True}
    except Exception as e:return {'available':False,'error':str(e)}

class DurableCSV:
    def __init__(self,path,fields):
        self.path=Path(path);self.f=self.path.open('w',newline='',buffering=1)
        self.writer=csv.DictWriter(self.f,fieldnames=fields);self.writer.writeheader()
        self.rows=0;self.last_sync=time.monotonic();self.closed=False
        self.f.flush();os.fsync(self.f.fileno())
    def write(self,row):
        self.writer.writerow(row);self.rows+=1
        # Line buffering survives a process termination; periodic fsync additionally
        # bounds the uncommitted file-system buffer during a normal running host.
        if time.monotonic()-self.last_sync>=.25:
            self.f.flush();os.fsync(self.f.fileno());self.last_sync=time.monotonic()
    def close(self,complete=True):
        if self.closed:return
        self.f.flush();os.fsync(self.f.fileno());self.f.close();self.closed=True
        atomic_json(self.path.with_suffix(self.path.suffix+'.completion.json'),
                    {'complete':complete,'rows':self.rows,'sha256':sha(self.path),'bytes':self.path.stat().st_size,**clocks()})
    def __enter__(self):return self
    def __exit__(self,typ,value,tb):self.close(typ is None)

class DurableList(list):
    """Retain the existing final CSV interface while journaling each append."""
    def __init__(self,name):super().__init__();self.name=name
    def append(self,value):
        journal(self.name,value);super().append(value)

class StateRecord(dict):
    def __setitem__(self,key,value):
        super().__setitem__(key,value);self.record()
    def update(self,*args,**kwargs):
        super().update(*args,**kwargs);self.record()
    def record(self):journal('gateway_packet_states',{'observed_ns':time.monotonic_ns(),**self})

class StateMap(dict):
    def __setitem__(self,key,value):
        row=StateRecord(value);super().__setitem__(key,row);row.record()

def journal(name,event):
    if name not in _journals:
        path=Path(os.environ['GAR_TRIAL_DIR'])/(name+'.jsonl')
        _journals[name]=[path.open('w',buffering=1),path,0,time.monotonic()]
    entry=_journals[name];entry[0].write(json.dumps(event,separators=(',',':'),allow_nan=False)+'\n');entry[2]+=1
    if time.monotonic()-entry[3]>=.25:
        entry[0].flush();os.fsync(entry[0].fileno());entry[3]=time.monotonic()

def register_child(process,kind):
    _children.append((process,kind))
    journal(mp.current_process().name+'_children',{'event':'spawn','kind':kind,'pid':process.pid,**clocks()})
    return process

def run_role(name,target,args,out):
    os.setsid()
    termination=None
    def terminated(signum,frame):
        nonlocal termination
        termination={'signal':signum,**clocks()}
        raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,terminated)
    start=clocks();status='FAILED';error=None;code=0
    atomic_json(Path(out)/(name+'_started.json'),{'pid':os.getpid(),'pgid':os.getpgrp(),'role':name,**start})
    try:
        target(*args);status='COMPLETE'
    except BaseException as e:
        error=traceback.format_exc();code=e.code if isinstance(e,SystemExit) and isinstance(e.code,int) else 1
        status='TERMINATED' if isinstance(e,SystemExit) else 'FAILED'
    finally:
        child_states=[]
        for process,kind in _children:
            forced=False
            if process.poll() is None:
                forced=True;process.terminate()
                try:process.wait(timeout=.5)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=.5)
            child_states.append({'kind':kind,'pid':process.pid,'exitcode':process.returncode,'cleanup_terminated':forced})
        logs={}
        for name_,(f,path,count,last) in _journals.items():
            f.flush();os.fsync(f.fileno());f.close()
            logs[path.name]={'rows':count,'sha256':sha(path),'bytes':path.stat().st_size}
        atomic_json(Path(out)/(name+'_completion.json'),{'role':name,'status':status,'exitcode':code,
            'start_clocks':start,'termination_received':termination,'end_clocks':clocks(),'error':error,'nested_processes':child_states,'journals':logs})
    if code:raise SystemExit(code)

def bounded_line(process,timeout=.25,request=None):
    """One request/one line reply, bounded on both pipe writes and reads."""
    end=continuous_ns()+int(timeout*1e9);pending=request or b'';reply=bytearray()
    os.set_blocking(process.stdout.fileno(),False)
    if process.stdin:os.set_blocking(process.stdin.fileno(),False)
    while True:
        remaining=(end-continuous_ns())/1e9
        if remaining<=0:raise TimeoutError('Adapter IPC exceeded %.3f s; child exit=%r; response_bytes=%d; unsent_bytes=%d'%(timeout,process.poll(),len(reply),len(pending)))
        rr,ww,_=select.select([process.stdout],([process.stdin] if pending else []),[],remaining)
        if ww:
            try:pending=pending[os.write(process.stdin.fileno(),pending):]
            except BlockingIOError:pass
        if rr:
            block=os.read(process.stdout.fileno(),65536)
            if not block:raise RuntimeError('IPC child closed its response pipe; exit=%r'%process.poll())
            reply.extend(block)
            if b'\n' in reply:
                line,extra=reply.split(b'\n',1)
                if extra or pending:raise RuntimeError('Unexpected pipelined IPC response')
                return line.decode()

class BoundedAdapter:
    def __init__(self,executable):
        role=mp.current_process().name;out=Path(os.environ['GAR_TRIAL_DIR'])
        self.log_name=role+'_adapter';self.seq=0
        self.error_log=(out/(role+'_adapter.stderr')).open('w',buffering=1)
        self.p=register_child(subprocess.Popen([str(executable)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                              stderr=self.error_log,bufsize=0),'adapter')
    def call(self,line):
        self.seq+=1;start=time.monotonic_ns();op=line.split(' ',1)[0]
        # INIT precedes the shared experiment origin. A freshly copied executable
        # may need OS loader/security startup time; operational calls stay 250 ms.
        timeout=2.0 if op=='INIT' else .25
        journal(self.log_name,{'event':'begin','seq':self.seq,'op':op,'monotonic_ns':start,
            'timeout_seconds':timeout,'request_bytes':len(line)+1,'request_sha256':hashlib.sha256(line.encode()).hexdigest()})
        try:
            result=bounded_line(self.p,timeout=timeout,request=(line+'\n').encode());data=json.loads(result)
            if 'error' in data:raise RuntimeError(data['error'])
        except BaseException as e:
            journal(self.log_name,{'event':'failure','seq':self.seq,'op':op,'monotonic_ns':time.monotonic_ns(),'error':str(e)})
            raise
        journal(self.log_name,{'event':'end','seq':self.seq,'op':op,'monotonic_ns':time.monotonic_ns()})
        return data
    def close(self):
        if self.p.poll() is None:
            os.set_blocking(self.p.stdin.fileno(),False)
            try:os.write(self.p.stdin.fileno(),b'QUIT\n')
            except (BlockingIOError,BrokenPipeError):self.p.terminate()
            try:self.p.wait(timeout=.5)
            except subprocess.TimeoutExpired:self.p.kill();self.p.wait(timeout=.5)
        self.error_log.close()
