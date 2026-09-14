import argparse
import json
import threading
import time
import uuid
from collections import defaultdict,deque
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path

class WorkflowEngine:
    def __init__(self,max_workers=4):
        self.tasks={}
        self.children=defaultdict(set)
        self.parents=defaultdict(set)
        self.max_workers=max(1,int(max_workers))
        self.results={}
        self.lock=threading.Lock()
    def add_task(self,name,func,deps=None,retries=0):
        if name in self.tasks:raise ValueError(f"Task already exists: {name}")
        dependencies=set(deps or [])
        self.tasks[name]={"func":func,"deps":dependencies,"retries":max(0,int(retries))}
        for dep in dependencies:
            self.children[dep].add(name)
            self.parents[name].add(dep)
    def validate(self):
        missing={dep for deps in self.parents.values() for dep in deps if dep not in self.tasks}
        if missing:raise ValueError(f"Missing dependencies: {sorted(missing)}")
        degree={name:len(self.parents[name]) for name in self.tasks}
        queue=deque(name for name,count in degree.items() if count==0)
        seen=0
        while queue:
            node=queue.popleft();seen+=1
            for child in self.children[node]:
                degree[child]-=1
                if degree[child]==0:queue.append(child)
        if seen!=len(self.tasks):raise ValueError("Workflow contains a dependency cycle.")
    def ready_tasks(self,completed,failed,blocked,running):
        ready=[]
        for name,task in self.tasks.items():
            if name in completed or name in failed or name in blocked or name in running:continue
            if all(dep in completed for dep in task["deps"]):ready.append(name)
        return ready
    def execute_one(self,name):
        task=self.tasks[name];started=time.perf_counter();last_error=""
        for attempt in range(1,task["retries"]+2):
            try:
                value=task["func"]()
                return {"status":"SUCCESS","task":name,"attempts":attempt,"duration":round(time.perf_counter()-started,3),"result":value}
            except Exception as exc:last_error=str(exc)
        return {"status":"FAILED","task":name,"attempts":task["retries"]+1,"duration":round(time.perf_counter()-started,3),"error":last_error}
    def run(self):
        self.validate();completed=set();failed=set();blocked=set();running=set();started=time.strftime("%Y-%m-%d %H:%M:%S")
        while len(completed)+len(failed)+len(blocked)<len(self.tasks):
            progress=True
            while progress:
                progress=False
                for name,task in self.tasks.items():
                    if name in completed or name in failed or name in blocked:continue
                    if any(dep in failed or dep in blocked for dep in task["deps"]):
                        blocked.add(name);self.results[name]={"status":"BLOCKED","task":name,"attempts":0,"duration":0,"error":"Dependency failed or was blocked."};progress=True
            ready=self.ready_tasks(completed,failed,blocked,running)
            if not ready:
                remaining=[name for name in self.tasks if name not in completed and name not in failed and name not in blocked]
                if remaining:raise RuntimeError(f"No executable tasks remain: {remaining}")
                break
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                future_map={pool.submit(self.execute_one,name):name for name in ready}
                for name in ready:running.add(name)
                for future in as_completed(future_map):
                    name=future_map[future];running.discard(name);result=future.result();self.results[name]=result
                    if result["status"]=="SUCCESS":completed.add(name)
                    else:failed.add(name)
        status="SUCCESS" if not failed and not blocked else "FAILED"
        return {"run_id":uuid.uuid4().hex[:12],"status":status,"started_at":started,"finished_at":time.strftime("%Y-%m-%d %H:%M:%S"),"task_count":len(self.tasks),"completed":sorted(completed),"failed":sorted(failed),"blocked":sorted(blocked),"tasks":self.results}

def demo_engine():
    engine=WorkflowEngine(3);state={"attempts":0}
    def extract():time.sleep(0.2);return "data-extracted"
    def validate():time.sleep(0.2);return "data-validated"
    def transform():time.sleep(0.3);return "data-transformed"
    def flaky():
        state["attempts"]+=1
        if state["attempts"]==1:raise RuntimeError("temporary failure")
        return "recovered"
    def report():time.sleep(0.2);return "report-created"
    def finalize():return "workflow-completed"
    engine.add_task("extract",extract)
    engine.add_task("validate",validate,["extract"])
    engine.add_task("transform",transform,["extract"])
    engine.add_task("flaky_service",flaky,["extract"],2)
    engine.add_task("report",report,["validate","transform"])
    engine.add_task("finalize",finalize,["report","flaky_service"])
    return engine

def load_workflow(path):
    data=json.loads(Path(path).read_text(encoding="utf-8"));engine=WorkflowEngine(data.get("max_workers",4))
    for item in data.get("tasks",[]):
        name=item["name"];command=item.get("command","echo");retries=item.get("retries",0);deps=item.get("deps",[])
        if command=="echo":
            message=item.get("message","");func=lambda message=message:message
        elif command=="sleep":
            seconds=float(item.get("seconds",0));func=lambda seconds=seconds:(time.sleep(seconds) or f"slept {seconds}s")
        elif command=="fail":
            message=item.get("message","Task failed")
            def func(message=message):raise RuntimeError(message)
        else:raise ValueError(f"Unsupported command: {command}")
        engine.add_task(name,func,deps,retries)
    return engine

def show(report):
    print("WORKFLOW ORCHESTRATION ENGINE");print("="*60);print(f"Status: {report['status']}");print(f"Tasks: {report['task_count']}");print(f"Completed: {len(report['completed'])}");print(f"Failed: {len(report['failed'])}");print(f"Blocked: {len(report['blocked'])}");print("\nTASK RESULTS")
    for name in sorted(report["tasks"]):
        item=report["tasks"][name];line=f"[{item['status']}] {name} | attempts={item['attempts']} | duration={item['duration']}s"
        if "error" in item:line+=f" | {item['error']}"
        print(line)

def main():
    parser=argparse.ArgumentParser(prog="workflow_orchestration_engine");parser.add_argument("workflow",nargs="?");parser.add_argument("--demo",action="store_true");parser.add_argument("--workers",type=int,default=4);parser.add_argument("--json",dest="json_file",default="");args=parser.parse_args()
    try:
        engine=demo_engine() if args.demo or not args.workflow else load_workflow(args.workflow);engine.max_workers=max(1,args.workers);report=engine.run();show(report)
        if args.json_file:
            out=Path(args.json_file).expanduser().resolve();out.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding="utf-8");print(f"\nSaved: {out}")
    except Exception as exc:print(f"ERROR: {exc}");raise SystemExit(1)
if __name__=="__main__":main()