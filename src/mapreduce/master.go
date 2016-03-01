package mapreduce
import "container/list"
import "fmt"

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

  //
  l := list.New()

  // map
  cur_miss := make(chan int)
  //var ch [len(mr.Workers)] chan int // mutex channels
  cur_miss <- 0
  for wi, w := range mr.Workers {
    //ch[wi] = make(chan int)
    go func(){
      var cur_map := <- cur_miss
      for cur_map < mr.nMap { //while

        go func(){
          // call
          args := &DoJobArgs{ mr.file , &JobType{ Map } , cur_map , mr.nMap}
          var reply DoJobReply;
          ok := call(w.address, "Worker.DoJob", args, &reply)
          if ok == false {
            fmt.Printf("DoWork: RPC %s DoJob error\n", w.address)
          } else {
           l.PushBack(cur_map)
          }
          cur_miss <- (cur_map+1)
        }()

        cur_map := <- cur_miss
        
      }
    }()

  }

  // reduce
  cur_miss := make(chan int)
  //var ch [len(mr.Workers)] chan int // mutex channels
  cur_miss <- 0
  for wi, w := range mr.Workers {
    //ch[wi] = make(chan int)
    go func(){
      var cur_red := <- cur_miss
      for cur_red < mr.nReduce { //while

        go func(){
          // call
          args := &DoJobArgs{ mr.file , &JobType{ Reduce } , cur_red , mr.nReduce}
          var reply DoJobReply;
          ok := call(w.address, "Worker.DoJob", args, &reply)
          if ok == false {
            fmt.Printf("DoWork: RPC %s DoJob error\n", w.address)
          } else {
           l.PushBack(cur_red)
          }
          cur_miss <- (cur_red+1)
        }()
        
        cur_red := <- cur_miss
        
      }
    }()

  }
  

  // reduce
  for i := 0; i < mr.nReduce; i++ {
    DoReduce(i, mr.file, mr.nMap, Reduce)
  }

  return mr.KillWorkers()
}
