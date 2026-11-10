"""Validate the Slurm DAG without submitting real jobs or downloading runtimes."""
import contextlib
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
   r=subprocess.run([sys.executable,str(ROOT/"experiments/revision/campaign.py"),"collect","--root",str(td)],capture_output=True,text=True,timeout=60)
   self.assertEqual(r.returncode,3,r.stdout+"\n"+r.stderr)
   coverage=json.loads((td/"report/coverage.json").read_text())
   self.assertFalse(coverage["ready_for_resubmission"])
   self.assertEqual(coverage["status"],"INCOMPLETE_OR_FAILED")
   self.assertTrue((td/"report_bundle.zip").exists())
if __name__=="__main__":unittest.main()
