"""Independent checks for the continuous-history revision experiment."""
import csv
import json
from collections import defaultdict
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
ROOT=Path(__file__).resolve().parents[1]
class RevisionHistories(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.tmp=tempfile.TemporaryDirectory(prefix="gar-revision-tests-")
  cls.directory=Path(cls.tmp.name)
  cls.binary=cls.directory/"sustained"
  compiler=shutil.which("c++") or shutil.which("g++")
  if not compiler: raise RuntimeError("C++17 compiler required")
  subprocess.run([compiler,"-std=c++17","-O2",str(ROOT/"experiments/revision/sustained.cpp"),"-o",str(cls.binary)],check=True,capture_output=True,text=True)
 @classmethod
 def tearDownClass(cls): cls.tmp.cleanup()
 def run_case(self,name,*args):
  path=self.directory/(name+".json")
  subprocess.run([str(self.binary),"--output",str(path),"--duration","2500","--seed","800111","--rate","100","--trace","1",*args],check=True,capture_output=True,text=True)
  return json.loads(path.read_text()),path
 def test_common_offers_and_protected_deadlines(self):
  fingerprints=set()
  for method in ["peak","keep_calendar","minimum_floor","gar","gar_h"]:
   p,path=self.run_case(method,"--policy",method)
   self.assertEqual(p["status"],"COMPLETE")
   fingerprints.add(p["input_fingerprint_fnv64"])
   self.assertEqual(p["critical_failed"],0)
   self.assertEqual(p["background_failed"],0)
   self.assertEqual(p["accepted_requests"],p["timely_tenants"])
   self.assertEqual(p["capacity_ledger_violations"],0)
   self.assertEqual(p["envelope_violations"],0)
  self.assertEqual(len(fingerprints),1)
 def test_independent_intervals_releases_and_job_outcomes(self):
  p,path=self.run_case("audit","--policy","gar")
  delta=defaultdict(int);floors={}
  with open(str(path)+".tenants.csv") as f:
   rows=list(csv.DictReader(f))
  self.assertGreater(len(rows),0)
  area=0
  for x in rows:
   r={k:int(v) for k,v in x.items()};s,e=r["start_ms"],r["end_ms"];q=r["units"]
   delta[s]+=q;delta[e]-=q;area+=q*(e-s)
   self.assertEqual(r["remaining_bytes"],0)
   self.assertLessEqual(r["completion_ms"],e)
   rate=p["certified_capacity"]*q//p["units"]
   self.assertLessEqual(r["work_bytes"],rate*(e-s))
   if r["release_pattern"]:
    offset=(e-s)//2 if r["release_pattern"]==1 else (e-s)*9//10
    self.assertLessEqual(r["work_bytes"]-r["work_bytes"]//2,rate*(e-s-offset))
  with open(str(path)+".calendar.csv") as f:
   for r in csv.DictReader(f):floors[int(r["time_ms"])]=int(r["critical_units"])
  rights=0;floor=0
  for t in sorted(set(delta)|set(floors)):
   rights+=delta[t];floor=floors.get(t,floor)
   self.assertGreaterEqual(rights,0)
   self.assertLessEqual(rights+floor+p["background_units"],p["units"])
  self.assertEqual(rights,0);self.assertEqual(area,p["booked_unit_ms"])
  failure=[0]*6
  with open(str(path)+".jobs.csv") as f:
   for r in csv.DictReader(f):
    if int(r["remaining_bytes"]) or not 0<=int(r["completion_ms"])<=int(r["deadline_ms"]):failure[int(r["class_id"])]+=1
  self.assertEqual(sum(failure[:3]),p["critical_failed"])
  self.assertEqual(sum(failure[3:5]),p["background_failed"])
 def test_bounded_service_and_controller_outage(self):
  p,_=self.run_case("bounded","--service",".9","--actual-service",".9","--outage-start","1000","--outage-length","1000")
  self.assertEqual(p["status"],"COMPLETE")
  self.assertEqual(p["supply_shortfall_slots"],0)
  self.assertEqual(p["critical_failed"],0)
  self.assertEqual(p["background_failed"],0)
  self.assertEqual(p["accepted_requests"],p["timely_tenants"])
 def test_unannounced_drop_is_reported_outside_assumptions(self):
  p,_=self.run_case("drop","--actual-service",".1")
  self.assertGreater(p["supply_shortfall_slots"],0)
 def test_causal_history_widths(self):
  fingerprints=set()
  for width in ["50","500"]:
   p,_=self.run_case("history"+width,"--policy","gar_h","--history-window",width)
   fingerprints.add(p["input_fingerprint_fnv64"])
   self.assertEqual(p["critical_failed"],0)
   self.assertEqual(p["background_failed"],0)
  self.assertEqual(len(fingerprints),1)
 def test_invalid_parameter_rejected(self):
  path=self.directory/"invalid.json"
  r=subprocess.run([str(self.binary),"--output",str(path),"--delay","-1"],capture_output=True,text=True)
  self.assertNotEqual(r.returncode,0);self.assertFalse(path.exists())
if __name__=="__main__":unittest.main()
