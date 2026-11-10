"""Validate the Slurm DAG without submitting real jobs or downloading runtimes."""
import contextlib
import gzip
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def load_module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
BOOT=load_module("gar_bootstrap_test",ROOT/"hpc/bootstrap_revision.py")
CAMPAIGN=load_module("gar_campaign_test",ROOT/"experiments/revision/campaign.py")
class RevisionSubmission(unittest.TestCase):
 def recovery_fixture(self,source):
  source.mkdir();(source/"build").mkdir()
  sources={str(p.relative_to(ROOT)):CAMPAIGN.sha(p) for p in (ROOT/"experiments").rglob("*") if p.suffix in {".cpp",".hpp",".py"}}
  sources["experiments/revision/campaign.py"]=CAMPAIGN.REUSE_RUNNER_SHA256
  binaries={}
  for name in ["sustained","online","benchmark"]:
   p=source/"build"/name;p.write_bytes(b"test binary identity");binaries[name]=CAMPAIGN.sha(p)
  CAMPAIGN.write(source/"build/identity.json",dict(sources=sources,binaries=binaries))
  CAMPAIGN.write(source/"source_identity.json",dict(commit=sorted(CAMPAIGN.REUSE_COMMITS)[0]))
  CAMPAIGN.write(source/"submission.json",dict(jobs={"calibrate":"700001"}))
  CAMPAIGN.write(source/"pilot.json",dict(status="PASS",quick=False))
  CAMPAIGN.write(source/"pilot_manifest.json",CAMPAIGN.pilot_tasks())
  CAMPAIGN.write(source/"calibration_manifest.json",CAMPAIGN.calibration_tasks())
  tasks=[CAMPAIGN.pilot_tasks()[0],CAMPAIGN.calibration_tasks()[0]]
  for t in tasks:
   d=source/"tasks"/t["id"];d.mkdir(parents=True);CAMPAIGN.write(d/"result.json",dict(status="PASS"))
   r=dict(status="COMPLETE",task=t,result_sha256=CAMPAIGN.sha(d/"result.json"))
   if t["kind"]=="online":
    r["context_sha256"]=CAMPAIGN.sha(CAMPAIGN.context(source,t["config"]))
    (d/"offers.csv").write_text("offers\n")
    r["offer_sha256"]=CAMPAIGN.sha(d/"offers.csv")
    with gzip.open(d/"offers.csv.gz","wb") as f:f.write((d/"offers.csv").read_bytes())
    (d/"offers.csv").unlink()
   CAMPAIGN.write(d/"receipt.json",r)
  failed=CAMPAIGN.calibration_tasks()[1]
  CAMPAIGN.write(source/"tasks"/failed["id"]/"receipt.json",dict(status="FAILED",task=failed,error="input file missing"))
  return tasks,failed
 def test_recovery_imports_success_and_preserves_original(self):
  with tempfile.TemporaryDirectory() as td,contextlib.redirect_stdout(io.StringIO()):
   td=Path(td);source=td/"old";dest=td/"new";dest.mkdir();tasks,failed=self.recovery_fixture(source)
   before={str(p.relative_to(source)):CAMPAIGN.sha(p) for p in source.rglob("*") if p.is_file()}
   r=CAMPAIGN.reuse_completed(dest,source)
   self.assertEqual(r["status"],"COMPLETE");self.assertEqual(len(r["reused"]),2)
   self.assertEqual(r["failed_retained"][0]["id"],failed["id"])
   self.assertFalse((dest/"tasks"/failed["id"]).exists())
   for t in tasks:
    self.assertEqual(CAMPAIGN.sha(dest/"tasks"/t["id"]/"receipt.json"),CAMPAIGN.sha(source/"tasks"/t["id"]/"receipt.json"))
   self.assertTrue(list((dest/"tasks"/tasks[1]["id"]/"contexts").glob("*.csv")))
   self.assertEqual(before,{str(p.relative_to(source)):CAMPAIGN.sha(p) for p in source.rglob("*") if p.is_file()})
   self.assertEqual(CAMPAIGN.reuse_completed(dest,source),r)
 def test_recovery_rejects_changed_simulator_or_result(self):
  for change in ["simulator","result"]:
   with self.subTest(change=change),tempfile.TemporaryDirectory() as td:
    td=Path(td);source=td/"old";dest=td/"new";dest.mkdir();tasks,_=self.recovery_fixture(source)
    if change=="simulator":
     p=source/"build/identity.json";r=CAMPAIGN.read(p);r["sources"]["experiments/online/simulator.cpp"]="0"*64;CAMPAIGN.write(p,r)
    else:(source/"tasks"/tasks[0]["id"]/"result.json").write_text("{}\n")
    with self.assertRaises(RuntimeError):CAMPAIGN.reuse_completed(dest,source)
 def test_recovery_submission_and_active_job_guard(self):
  with tempfile.TemporaryDirectory() as td,contextlib.redirect_stdout(io.StringIO()):
   td=Path(td);source=td/"old";self.recovery_fixture(source)
   r=BOOT.submit_workflow(td/"project","e"*40,reuse_from=source)
   self.assertEqual(r["reuse_from"],str(source.resolve()))
   pilot_script=Path(r["commands"]["pilot"][-1]).read_text()
   self.assertIn("--reuse-from",pilot_script);self.assertIn(str(source),pilot_script)
   with patch.object(BOOT,"slurm",return_value="700001_3"):
    with self.assertRaisesRegex(RuntimeError,"active jobs"):
     BOOT.submit_workflow(td/"blocked","e"*40,submit=True,reuse_from=source)
   self.assertFalse((td/"blocked/work/latest_revision.json").exists())
 def test_context_bytes_match_original_failed_case(self):
  with tempfile.TemporaryDirectory() as td:
   c=dict(family="regime_shift",seed=910001,duration=300000)
   self.assertEqual(CAMPAIGN.sha(CAMPAIGN.context(Path(td),c)),"1c0c75e9d9967aba41f9017a3665764fc9968c68957febc86509f715ccb51008")
 def test_report_counts_failed_preparation_before_production(self):
  with tempfile.TemporaryDirectory() as td,contextlib.redirect_stdout(io.StringIO()):
   td=Path(td);t=CAMPAIGN.calibration_tasks()[0]
   CAMPAIGN.write(td/"calibration_manifest.json",[t])
   CAMPAIGN.write(td/"tasks"/t["id"]/"receipt.json",dict(task=t,status="FAILED",error="input file missing"))
   self.assertEqual(CAMPAIGN.collect(td),3)
   c=CAMPAIGN.read(td/"report/coverage.json")
   self.assertEqual(c["expected"],1);self.assertEqual(c["received"],1)
   self.assertEqual(c["failed"][0]["study"],"E2_CALIBRATION")
   self.assertEqual(len(c["missing_production_manifests"]),3)
   self.assertIn("input file missing",(td/"report/SUMMARY.md").read_text())
 def test_dry_run_dependencies_and_limits(self):
  with tempfile.TemporaryDirectory() as td,contextlib.redirect_stdout(io.StringIO()):
   receipt=BOOT.submit_workflow(Path(td)/"project","a"*40,submit=False)
   self.assertEqual(receipt["status"],"DRY_RUN");self.assertEqual(len(receipt["jobs"]),7)
   for name,args in receipt["commands"].items():
    n=int(args[args.index("--cpus-per-task")+1]);self.assertLessEqual(n,40)
    self.assertIn("--kill-on-invalid-dep=yes",args)
    if name=="collect":self.assertTrue(args[args.index("--dependency")+1].startswith("afterany:"))
    elif name!="acquire":self.assertTrue(args[args.index("--dependency")+1].startswith("afterok:"))
   main=receipt["commands"]["experiments"]
   self.assertEqual(main[main.index("--array")+1],"0-5%4")
   for phase in ["experiments","scaling"]:
    args=receipt["commands"][phase]
    self.assertEqual(args[args.index("--dependency")+1],"afterok:"+receipt["jobs"]["calibrate"])
   body=Path(main[-1]).read_text()
   self.assertIn("SLURM_ARRAY_TASK_ID",body);self.assertNotIn("ARRAY_INDEX",body)
   for p in Path(receipt["run"]).joinpath("slurm").glob("*.sbatch"):
    subprocess.run(["bash","-n",str(p)],check=True,capture_output=True,text=True)
 def test_mock_submission_and_duplicate_prevention(self):
  calls=[]
  def fake(args):
   calls.append(args)
   if args[0]=="sbatch":return str(700000+sum(a[0]=="sbatch" for a in calls))
   return ""
  with tempfile.TemporaryDirectory() as td,patch.object(BOOT,"slurm",side_effect=fake),contextlib.redirect_stdout(io.StringIO()):
   project=Path(td)/"project";r=BOOT.submit_workflow(project,"b"*40,submit=True)
   self.assertEqual(len([a for a in calls if a[0]=="sbatch"]),7)
   old=BOOT.submit_workflow(project,"b"*40,submit=True)
   self.assertEqual(old["run"],r["run"])
   self.assertEqual(len([a for a in calls if a[0]=="sbatch"]),7)
 def test_warning_before_parsable_job_id(self):
  with tempfile.TemporaryDirectory() as td,patch.object(BOOT,"slurm",return_value="sbatch: warning example\n820001;cluster"),contextlib.redirect_stdout(io.StringIO()):
   r=BOOT.submit_workflow(Path(td),"d"*40,submit=True)
   self.assertEqual(set(r["jobs"].values()),{"820001"})
 def test_partial_submission_cancels_only_owned_jobs(self):
  calls=[]
  def fake(args):
   calls.append(args)
   if args[0]=="sbatch":
    if sum(a[0]=="sbatch" for a in calls)==3:raise RuntimeError("injected rejection")
    return str(810000+len(calls))
   return ""
  with tempfile.TemporaryDirectory() as td,patch.object(BOOT,"slurm",side_effect=fake),contextlib.redirect_stdout(io.StringIO()):
   with self.assertRaises(RuntimeError):BOOT.submit_workflow(Path(td),"c"*40,submit=True)
   cancellations=[x for x in calls if x[0]=="scancel"]
   self.assertEqual(len(cancellations),1);self.assertEqual(cancellations[0][1:],["810001","810002"])
 def test_tar_traversal_rejected(self):
  with tempfile.TemporaryDirectory() as td:
   td=Path(td);archive=td/"bad.tar.gz"
   with tarfile.open(archive,"w:gz") as f:
    x=tarfile.TarInfo("root/../../escape");x.size=1;f.addfile(x,io.BytesIO(b"x"))
   with self.assertRaises(ValueError):BOOT.unpack(archive,td/"out")
   self.assertFalse((td/"escape").exists())
 def test_full_primary_and_reserved_seeds(self):
  tasks=CAMPAIGN.primary()
  self.assertEqual(len(tasks),3600);self.assertEqual(len({t["id"] for t in tasks}),3600)
  self.assertEqual({t["config"]["seed"] for t in tasks},set(range(1000001,1000031)))
  self.assertTrue(all(t["config"]["duration"]==300000 for t in tasks))
 def test_source_manifest_guards(self):
  with tempfile.TemporaryDirectory() as td:
   td=Path(td);(td/"a.txt").write_text("hello")
   BOOT.write(td/"manifest.json",{"files":[{"path":"a.txt","bytes":5,"sha256":BOOT.sha(td/"a.txt")}]})
   self.assertEqual(BOOT.verify_source(td),1)
   (td/"a.txt").write_text("changed")
   with self.assertRaises(RuntimeError):BOOT.verify_source(td)
 def test_quick_campaign_and_incomplete_report(self):
  with tempfile.TemporaryDirectory() as td:
   td=Path(td)
   for cmd in [["pilot","--quick","--workers","2"],["calibrate","--quick","--workers","2"]]:
    r=subprocess.run([sys.executable,str(ROOT/"experiments/revision/campaign.py"),cmd[0],"--root",str(td),*cmd[1:]],capture_output=True,text=True,timeout=240)
    self.assertEqual(r.returncode,0,r.stdout+"\n"+r.stderr)
   self.assertTrue(json.loads((td/"pilot.json").read_text())["quick"])
   contexts=[];hashes=set()
   for p in (td/"tasks").glob("*/receipt.json"):
    receipt=CAMPAIGN.read(p)
    if receipt["task"]["kind"]!="online":continue
    command=receipt["command"];ctx=Path(command[command.index("--context")+1])
    self.assertEqual(ctx.parent,p.parent/"contexts")
    contexts.append(ctx);hashes.add(receipt["context_sha256"])
   self.assertGreater(len(contexts),1);self.assertEqual(len(contexts),len(set(contexts)))
   self.assertEqual(len(hashes),1)
   r=subprocess.run([sys.executable,str(ROOT/"experiments/revision/campaign.py"),"collect","--root",str(td)],capture_output=True,text=True,timeout=60)
   self.assertEqual(r.returncode,3,r.stdout+"\n"+r.stderr)
   coverage=json.loads((td/"report/coverage.json").read_text())
   self.assertFalse(coverage["ready_for_resubmission"])
   self.assertEqual(coverage["status"],"INCOMPLETE_OR_FAILED")
   self.assertTrue((td/"report_bundle.zip").exists())
if __name__=="__main__":unittest.main()
