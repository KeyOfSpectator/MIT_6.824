package mapreduce
import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here

  log.Printf("RunMaster...\n")

  l := list.New()

// type DoJobArgs struct {
//   File string
//   Operation JobType
//   JobNumber int       // this job's number
//   NumOtherPhase int   // total number of jobs in other phase (map or reduce)
// }

// map
  for cur_m := 0 ; cur_m < mr.nMap ; cur_m ++ {
    worker := <- mr.registerChannel
    //log.Printf("Current Num Index %s \n" , cur_m)
    mr.Workers[worker] = &WorkerInfo{ worker }

    go func(cur_m int){
      // call 
      args := &DoJobArgs{ mr.file , "Map" , cur_m , mr.nReduce}
      var reply DoJobReply;
      ok := call(worker, "Worker.DoJob", args, &reply)
      if ok == false {
        fmt.Printf("DoWork: RPC %s DoJob error\n", worker)
      } else {
        l.PushBack(cur_m)
      }
      //  
      mr.registerChannel <- worker
    }(cur_m)   // !!!
  }

  

  log.Printf("Master Map Done.\n")

// reduce
  for cur_m := 0 ; cur_m < mr.nReduce ; cur_m ++ {
    worker := <- mr.registerChannel
    mr.Workers[worker] = &WorkerInfo{ worker }

    go func(cur_m int){
      // call 
      args := &DoJobArgs{ mr.file , "Reduce" , cur_m , mr.nMap}
      var reply DoJobReply;
      ok := call(worker, "Worker.DoJob", args, &reply)
      if ok == false {
        fmt.Printf("DoWork: RPC %s DoJob error\n", worker)
      } else {
        l.PushBack(cur_m)
      }
      // 
      mr.registerChannel <- worker
    }(cur_m)
  }

  log.Printf("Master Reduce Done.\n")

  return mr.KillWorkers()
}
