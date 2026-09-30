// Exercise the production selector with independently enumerated subsets.
#define main retained_simulator_main
#include "simulator.cpp"
#undef main
#include <cassert>

int main(int argc,char**argv){
    if(argc!=2)return 2;
    Options options;options.context=argv[1];options.policy="luu_joint_style";
    options.duration=100;options.load=.4;options.gate_eta=-1;options.delay=2;
    options.contract_factor=1.4*options.load*options.burst_factor/.9;
    Simulation simulation(options);
    // Direct production priority function, including integer truncation.
    for(int q:{4,8,32,48})for(int L:{5,20,100,137,300})for(int ordinal=0;ordinal<6;++ordinal)
        assert(simulation.declared_priority(q,L,ordinal)==1000*(1000000/L)-10*q-ordinal);
    // Exercise the actual planner and actual atomic batch commit, not a stub.
    Simulation actual(options);TimedPlanExecutor executor(actual.B);
    assert(executor.propose(0,2,actual.B,40,actual.B,true));
    const auto old_id=executor.pending_plan_id();
    const auto old_commands=executor.counters().accepted_plan_commands;
    std::vector<Simulation::TenantProposal> fitting={{4,20,actual.declared_priority(4,20,0),0}};
    const auto fitting_plan=actual.luu_batch_plan(0,fitting,1,executor);
    assert(fitting_plan.feasible&&fitting_plan.target==-1&&fitting_plan.term==0);
    const auto accepted=actual.admit_luu_joint_batch(0,fitting,executor);
    assert(accepted.size()==1&&actual.booked(2)==4);
    assert(executor.pending_plan_id()==old_id&&executor.counters().accepted_plan_commands==old_commands);
    std::vector<Simulation::TenantProposal> too_large={{106,20,1,0}};
    assert(!actual.luu_batch_plan(0,too_large,1,executor).feasible);
    assert(actual.admit_luu_joint_batch(0,too_large,executor).empty());
    assert(actual.booked(2)==4&&executor.pending_plan_id()==old_id);
    int n;
    while(std::cin>>n){
        std::vector<Simulation::TenantProposal> candidates(n);
        for(int i=0;i<n;++i){std::cin>>candidates[i].priority;candidates[i].ordinal=i;}
        std::vector<int> targets(1U<<n);
        for(std::size_t mask=1;mask<targets.size();++mask)std::cin>>targets[mask];
        const auto selected=simulation.select_luu_subset(candidates,[&](unsigned mask){
            Simulation::BatchPlan plan;plan.feasible=targets[mask]!=-999;
            plan.target=targets[mask];plan.term=plan.target==-1?0:20;return plan;
        });
        std::cout<<selected.mask<<' '<<selected.plan.target<<'\n';
    }
}
