#!/usr/bin/env python3
"""Pinned GAR download and Slurm workflow. Python 3.8+; no Git or sudo needed."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path,PurePosixPath
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.request import Request,urlopen
REPO="rtqayyum/gar-industry4-6g"
IGNORE={".git",".venv","__pycache__",".DS_Store","runs",".pytest_cache"}
def write(path,value):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 tmp=path.with_name(path.name+".tmp-"+str(os.getpid()));tmp.write_text(json.dumps(value,indent=2,sort_keys=True)+"\n");os.replace(tmp,path)
def read(path):return json.loads(Path(path).read_text())
def sha(path):
 h=hashlib.sha256()
 with Path(path).open("rb") as f:
  for b in iter(lambda:f.read(1048576),b""):h.update(b)
 return h.hexdigest()
def slurm(args):
 if shutil.which(args[0]):return subprocess.check_output(args,text=True,stderr=subprocess.STDOUT).strip()
 return subprocess.check_output(["bash","-lc",'module load slurm >/dev/null 2>&1; exec "$@"',"gar-slurm",*args],text=True,stderr=subprocess.STDOUT).strip()
def verify_source(source):
 expected={x["path"]:x for x in read(source/"manifest.json")["files"]};actual={}
 for p in source.rglob("*"):
  rel=p.relative_to(source)
  if any(x in IGNORE for x in rel.parts) or rel.as_posix()=="manifest.json":continue
  if p.is_symlink():raise RuntimeError("Source symlink prohibited")
  if p.is_file():actual[rel.as_posix()]={"path":rel.as_posix(),"bytes":p.stat().st_size,"sha256":sha(p)}
 if actual!=expected:
  bad=[x for x in set(actual)|set(expected) if actual.get(x)!=expected.get(x)]
  raise RuntimeError("Manifest mismatch: "+", ".join(sorted(bad)[:10]))
 return len(actual)
def unpack(archive,destination):
 destination=Path(destination);destination.mkdir(parents=True,exist_ok=True);total=0;roots=set()
 with tarfile.open(archive,"r:gz") as tf:
  for member in tf:
   parts=PurePosixPath(member.name).parts
   if not parts or PurePosixPath(member.name).is_absolute() or ".." in parts:raise ValueError("Unsafe archive path")
   roots.add(parts[0])
   if len(roots)>1:raise ValueError("Multiple archive roots")
   if len(parts)==1:
    if not member.isdir():raise ValueError("Invalid archive root")
    continue
   target=destination.joinpath(*parts[1:])
   if member.isdir():target.mkdir(parents=True,exist_ok=True);continue
   if not member.isfile():raise ValueError("Archive links and special files prohibited")
   total+=member.size
   if member.size>300*1024**2 or total>2*1024**3:raise ValueError("Archive size limit")
   target.parent.mkdir(parents=True,exist_ok=True);stream=tf.extractfile(member)
   if stream is None:raise ValueError("Unreadable archive member")
   with target.open("xb") as out:shutil.copyfileobj(stream,out)
def download(url,target):
 target=Path(target);target.parent.mkdir(parents=True,exist_ok=True);partial=target.with_name(target.name+".part")
 for attempt in range(3):
  try:
   size=0;reported=0
   with urlopen(Request(url,headers={"User-Agent":"GAR-revision"}),timeout=90) as src,partial.open("wb") as out:
    while True:
     block=src.read(1048576)
     if not block:break
     size+=len(block)
     if size>1024**3:raise RuntimeError("Archive exceeds 1 GiB")
     out.write(block)
     if size-reported>=25*1024**2:print("Downloaded %.1f MiB"%(size/1024**2),flush=True);reported=size
   os.replace(partial,target);return
  except Exception:
   if attempt==2:raise
   time.sleep(2*(attempt+1))
def layout(project,commit,run):
 return dict(project=project,commit=commit,run=run,source=project/"work/code_versions"/commit,env=project/"work/envs"/commit)
def acquire(paths):
 project,commit,run,source,env=[paths[k] for k in ["project","commit","run","source","env"]]
 work=project/"work";work.mkdir(parents=True,exist_ok=True)
 with (work/("acquire-"+commit+".lock")).open("a+") as lock:
  fcntl.flock(lock,fcntl.LOCK_EX)
  if not source.exists():
   archive=work/"downloads"/(commit+".tar.gz");download("https://codeload.github.com/"+REPO+"/tar.gz/"+commit,archive)
   source.parent.mkdir(parents=True,exist_ok=True)
   with tempfile.TemporaryDirectory(prefix=".gar-extract-",dir=str(source.parent)) as tmp:
    stage=Path(tmp)/"source";unpack(archive,stage);count=verify_source(stage);os.replace(stage,source)
   print("Verified source commit",commit,"files",count,flush=True)
  else:print("Verified cached source files",verify_source(source),flush=True)
  if sha(source/"hpc/bootstrap_revision.py")!=sha(Path(__file__)):raise RuntimeError("Bootstrap differs from pinned source")
  tools=work/"bootstrap_env"/commit;tool_python=tools/"bin/python"
  if not tool_python.exists():subprocess.run([sys.executable,"-m","venv",str(tools)],check=True)
  subprocess.run([str(tool_python),"-m","pip","install","uv==0.9.5"],check=True)
  uv=tools/"bin/uv"
  if not (env/"bin/python").exists():
   env.parent.mkdir(parents=True,exist_ok=True);subprocess.run([str(uv),"venv","--python","3.12.12",str(env)],check=True)
  python=env/"bin/python";subprocess.run([str(uv),"pip","install","--python",str(python),"-r",str(source/"requirements.txt")],check=True)
  (run/"environment.txt").write_text(subprocess.check_output([str(uv),"pip","freeze","--python",str(python)],text=True))
  write(run/"source_identity.json",dict(commit=commit,repository=REPO,manifest_sha256=sha(source/"manifest.json"),verified_files=verify_source(source),
    bootstrap_sha256=sha(source/"hpc/bootstrap_revision.py"),python=subprocess.check_output([str(python),"--version"],text=True).strip()))
  write(run/"acquisition.json",dict(status="COMPLETE",commit=commit))
def render_script(command):
 return "#!/bin/bash\nset -euo pipefail\nif type module >/dev/null 2>&1; then module load gcc >/dev/null 2>&1 || true; fi\nexport PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 MPLBACKEND=Agg\nunset PYTHONPATH\nexec "+shlex.join(list(map(str,command)))+"\n"
def submit_workflow(project,commit,cpus=40,concurrent=4,shards=6,partition="longq",submit=False,new=False):
 project=Path(project).expanduser().resolve()
 if not re.fullmatch(r"[0-9a-f]{40}",commit):raise ValueError("A full immutable 40-character commit is required")
 if not 1<=cpus<=40 or not 1<=concurrent<=6 or not 1<=shards<=100:raise ValueError("CPU or concurrency limit")
 if not re.fullmatch(r"[A-Za-z0-9_-]+",partition):raise ValueError("Invalid partition")
 project.mkdir(parents=True,exist_ok=True);(project/"work").mkdir(exist_ok=True)
 with (project/"work/submission.lock").open("a+") as lock:
  fcntl.flock(lock,fcntl.LOCK_EX);latest=project/"work/latest_revision.json"
  if latest.exists() and not new:
   old=read(latest)
   if old.get("commit")==commit and old.get("status")=="SUBMITTED":
    print("Existing submission retained:",old["run"]);status(project);return old
  stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
  run=project/"results/revision"/(stamp+"-"+commit[:8]);run.mkdir(parents=True);logs=run/"logs";logs.mkdir();scripts=run/"slurm";scripts.mkdir()
  bootstrap=project/"work/bootstrap"/(commit+".py");bootstrap.parent.mkdir(parents=True,exist_ok=True);own=Path(__file__).read_bytes()
  if bootstrap.exists() and bootstrap.read_bytes()!=own:raise RuntimeError("Immutable bootstrap changed")
  if not bootstrap.exists():bootstrap.write_bytes(own)
  paths=layout(project,commit,run);python=paths["env"]/"bin/python";runner=paths["source"]/"experiments/revision/campaign.py"
  common=[sys.executable,bootstrap,"--project",project,"--commit",commit,"--run",run]
  stages=[
   ("acquire",common+["--stage","acquire"],4,[],False),
   ("pilot",[python,runner,"pilot","--root",run,"--workers",min(cpus,8)],min(cpus,8),["acquire"],False),
   ("calibrate",[python,runner,"calibrate","--root",run,"--workers",cpus],cpus,["pilot"],False),
   ("experiments",[python,runner,"worker","--root",run,"--workers",cpus,"--shard","ARRAY_INDEX","--shards",shards],cpus,["calibrate"],True),
   ("scaling",[python,runner,"scaling","--root",run,"--workers",1],1,["pilot"],False),
   ("reproduce",[python,runner,"reproduce","--root",run,"--workers",4],4,["acquire"],False),
   ("collect",common+["--stage","collect"],2,["acquire","pilot","calibrate","experiments","scaling","reproduce"],False)]
  receipt=dict(status="SUBMITTING" if submit else "DRY_RUN",commit=commit,run=str(run),jobs={},commands={})
  try:
   for name,command,ncpu,parents,array in stages:
    body=render_script(command).replace("ARRAY_INDEX",'"$'+'{SLURM_ARRAY_TASK_ID}"')
    file=scripts/(name+".sbatch");file.write_text(body)
    args=["sbatch","--parsable","--job-name","gar-"+name,"--partition",partition,"--cpus-per-task",str(ncpu),"--mem-per-cpu","1G",
     "--time","23:00:00" if name in {"acquire","pilot","collect"} else "9-00:00:00",
     "--output",str(logs/(name+"-%A_%a.out")),"--error",str(logs/(name+"-%A_%a.err")),"--kill-on-invalid-dep=yes"]
    if parents:args += ["--dependency",("afterany:" if name=="collect" else "afterok:")+":".join(receipt["jobs"][p] for p in parents)]
    if array:args += ["--array","0-%s%%%s"%(shards-1,concurrent)]
    args.append(str(file));receipt["commands"][name]=args
    if submit:
     raw=slurm(args)
     lines=[x.strip() for x in raw.splitlines() if x.strip()]
     answer=lines[-1] if lines else ""
     if not re.fullmatch(r"[0-9]+(?:;[^\s]+)?",answer):raise RuntimeError("Unrecognized sbatch response: "+raw)
     if len(lines)>1:print("Slurm message: "+" | ".join(lines[:-1]),flush=True)
     job=answer.split(";")[0];print("SUBMITTED",job,"gar-"+name,flush=True)
    else:job="DRY"+str(len(receipt["jobs"])+1)
    receipt["jobs"][name]=job;write(run/"submission.json",receipt);write(latest,receipt)
   receipt["status"]="SUBMITTED" if submit else "DRY_RUN";write(run/"submission.json",receipt);write(latest,receipt)
  except Exception as e:
   receipt.update(status="SUBMISSION_FAILED",error=str(e));write(run/"submission.json",receipt);write(latest,receipt)
   if submit and receipt["jobs"]:
    try:slurm(["scancel",*receipt["jobs"].values()])
    except Exception:pass
   raise
  check=project/"Check_revision"
  if check.exists() and "# GAR_REVISION_CHECK" not in check.read_text():check=project/("Check_revision_"+commit[:8])
  check.write_text("#!/bin/bash\n# GAR_REVISION_CHECK\nexec "+shlex.join([sys.executable,str(bootstrap),"--project",str(project),"--status"])+"\n")
  print("SOURCE_COMMIT="+commit);print("CAMPAIGN="+str(run));print("REPORT="+str(run/"report/SUMMARY.md"));print("CHECK: bash "+shlex.quote(str(check)),flush=True)
  return receipt
def status(project):
 latest=Path(project)/"work/latest_revision.json"
 if not latest.exists():print("No revision submission found");return
 r=read(latest);print(json.dumps(r,indent=2));ids=list(r.get("jobs",{}).values())
 if ids and all(x.isdigit() for x in ids):
  for cmd in [["squeue","-j",",".join(ids),"-o","%.18i %.12T %.50R"],["sacct","-j",",".join(ids),"--format=JobID,JobName,State,ExitCode,Elapsed","-P"]]:
   try:print(slurm(cmd))
   except Exception as e:print("Status lookup:",e)
 p=Path(r["run"])/"report/SUMMARY.md"
 if p.exists():print(p.read_text())
 else:print("Report pending. Logs:",Path(r["run"])/"logs")
def collect(paths):
 run=paths["run"];python=paths["env"]/"bin/python";runner=paths["source"]/"experiments/revision/campaign.py"
 if python.exists() and runner.exists():return subprocess.run([str(python),str(runner),"collect","--root",str(run)]).returncode
 out=run/"report";out.mkdir(exist_ok=True)
 (out/"SUMMARY.md").write_text("# GAR revision campaign\n\nINCOMPLETE: acquisition or setup failed. No scientific completion is claimed.\n\nInspect acquisition logs and Check_revision for Slurm states.\n")
 write(out/"coverage.json",dict(status="ACQUISITION_FAILED",ready_for_resubmission=False));return 3
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument("--project",type=Path,default=Path.home()/"gar-industry4-6g");p.add_argument("--commit");p.add_argument("--run",type=Path)
 p.add_argument("--stage",choices=["submit","acquire","collect"],default="submit");p.add_argument("--submit",action="store_true");p.add_argument("--status",action="store_true");p.add_argument("--new",action="store_true")
 p.add_argument("--cpus",type=int,default=40);p.add_argument("--concurrent",type=int,default=4);p.add_argument("--shards",type=int,default=6);p.add_argument("--partition",default="longq")
 a=p.parse_args();a.project=a.project.expanduser().resolve()
 if a.status:status(a.project);return
 if not a.commit or not re.fullmatch(r"[0-9a-f]{40}",a.commit):p.error("Specify a full --commit")
 if a.stage=="submit":submit_workflow(a.project,a.commit,a.cpus,a.concurrent,a.shards,a.partition,a.submit,a.new)
 else:
  if a.run is None:p.error("--run required for internal stage")
  paths=layout(a.project,a.commit,a.run.resolve())
  if a.stage=="acquire":acquire(paths)
  else:sys.exit(collect(paths))
if __name__=="__main__":main()
