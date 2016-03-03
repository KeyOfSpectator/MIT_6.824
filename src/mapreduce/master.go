package mapreduce
import "container/list"
import "fmt"
import "log"
import "time"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  last_succ_m int
  last_fail_m int
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

//////////////////  map  //////////////////

  timeout_ch := make(chan bool)
  call_ch := make(chan bool)
  failJobChan := make(chan int)
  next_m_chan := make(chan int)
  done_map := make(chan int)

  go func(){
    next_m_chan <- 0
  }()

  cur_m := 0
  // while true
  for  {

    var worker string
    select {
    // succ last time
    case cur_m = <- next_m_chan:
      worker =  <- mr.registerChannel

      // check 
      elem , ok  := mr.Workers[worker]
      if ok == true {
        if elem.last_succ_m >= cur_m {
          go func(){
            mr.registerChannel <- worker
          }()
          continue
        }
      }

    // failed last time
    case cur_m  = <- failJobChan:
      
      worker = <- mr.registerChannel

      // check 
      elem , ok  := mr.Workers[worker]
      if ok == true {
        if elem.last_fail_m >= cur_m {
          go func(){
            mr.registerChannel <- worker
            failJobChan <- cur_m
          }()
          continue
        }
      }
      fmt.Printf("Re handle faild job : %d\n" , cur_m)
    }

    // finish
    if cur_m >= mr.nMap {
      go func(){
        mr.registerChannel <- worker
      }()
      fmt.Printf("MAP END Current Mission num : %d\n" , cur_m)
      break
    }

    fmt.Printf("Current Mission num : %d\n" , cur_m)

    // time out 
    
    go func(){
      time.Sleep(1000 * time.Millisecond)

      timeout_ch <- true
    }()

    //call
    
    go func(cur_m int){
      // call 
      args := &DoJobArgs{ mr.file , "Map" , cur_m , mr.nReduce}
      var reply DoJobReply;

      ok := call(worker, "Worker.DoJob", args, &reply)
      if ok == false {
        fmt.Printf("DoWork: RPC %s DoJob error\n", worker)
      } else{
        l.PushBack(cur_m)
      }
      //  
      call_ch <- ok
      
    }(cur_m)   // !!!

    // check time out
    go func(cur_m int){
      for {
        select {
          // time out
          case <- timeout_ch:

            failJobChan <- cur_m
            fmt.Printf("DoWork: RPC %s DoJob timeout\n", worker)
            mr.registerChannel <- worker
            mr.Workers[worker] = &WorkerInfo{ worker , -1 ,cur_m }
            return

          // call return
          case ok := <-call_ch:

            // call failed
            if ok == false {
              failJobChan <- cur_m
              mr.Workers[worker] = &WorkerInfo{ worker , -1 , cur_m }
              <- timeout_ch
              mr.registerChannel <- worker
              
            // succ
            } else{
              cur_m ++

              // finish map
              if(cur_m == mr.nMap){
                go func(){
                  done_map <- 1
                }()
              }

              next_m_chan <- cur_m
              mr.registerChannel <- worker
              mr.Workers[worker] = &WorkerInfo{ worker , cur_m , -1 }

              <- timeout_ch

            }

            return
        }
      }

    }(cur_m)

  }

  <- done_map
  log.Printf("Master Map Done.\n")

//////////////////  reduce  //////////////////

  // timeout_ch = make(chan bool)
  // call_ch = make(chan bool)
  // failJobChan = make(chan int)
  // next_m_chan = make(chan int)
  reduce_workers := make(map[string]*WorkerInfo)
  done_reduce := make(chan int)

  go func(){
    next_m_chan <- 0
  }()

  cur_m = 0
  // while true
  for  {

    var worker string
    select {
    // succ last time
    case cur_m = <- next_m_chan:
      worker =  <- mr.registerChannel
      // check 
      elem , ok  := reduce_workers[worker]
      if ok == true {
        if elem.last_succ_m >= cur_m {
          go func(){
            mr.registerChannel <- worker
          }()
          continue
        }
      }

    // failed last time
    case cur_m  = <- failJobChan:
      worker = <- mr.registerChannel

      // check 
      elem , ok  := reduce_workers[worker]
      if ok == true {
        if elem.last_fail_m >= cur_m {
          go func(){
            mr.registerChannel <- worker
            failJobChan <- cur_m
          }()
          continue
        }
      }
      fmt.Printf("Re handle faild job : %d\n" , cur_m)
    }

    // finish
    if cur_m >= mr.nReduce {
      go func(){
        mr.registerChannel <- worker
      }()
      fmt.Printf("REDUCE END Current Mission num : %d\n" , cur_m)
      break
    }

    fmt.Printf("Current Mission num : %d\n" , cur_m)

    // time out 
    
    go func(){
      time.Sleep(1000 * time.Millisecond)

      timeout_ch <- true
    }()

    //call
    
    go func(cur_m int){
      // call 
      args := &DoJobArgs{ mr.file , "Reduce" , cur_m , mr.nMap}
      var reply DoJobReply;

      ok := call(worker, "Worker.DoJob", args, &reply)
      if ok == false {
        fmt.Printf("DoWork: RPC %s DoJob error\n", worker)
      } else{
        l.PushBack(cur_m)
      }
      //  
      call_ch <- ok
      
    }(cur_m)   // !!!

    // check time out
    go func(cur_m int){
      for {
        select {
          // time out
          case <- timeout_ch:

            failJobChan <- cur_m
            fmt.Printf("DoWork: RPC %s DoJob timeout\n", worker)
            mr.registerChannel <- worker
            reduce_workers[worker] = &WorkerInfo{ worker , -1 ,cur_m }
            return

          // call return
          case ok := <-call_ch:

            // call failed
            if ok == false {
              failJobChan <- cur_m
              reduce_workers[worker] = &WorkerInfo{ worker , -1 , cur_m }
              <- timeout_ch
              mr.registerChannel <- worker
              
            // succ
            } else{
              cur_m ++

              // finish map
              if(cur_m == mr.nReduce){
                go func(){
                  done_reduce <- 1
                }()
              }

              next_m_chan <- cur_m
              mr.registerChannel <- worker
              reduce_workers[worker] = &WorkerInfo{ worker , cur_m , -1 }

              <- timeout_ch

            }

            return
        }
      }

    }(cur_m)

  }

  <- done_reduce

// // reduce original
//   for cur_m := 0 ; cur_m < mr.nReduce ; cur_m ++ {
//     worker := <- mr.registerChannel
//     mr.Workers[worker] = &WorkerInfo{ worker , -1 , -1}

//     go func(cur_m int){
//       // call 
//       args := &DoJobArgs{ mr.file , "Reduce" , cur_m , mr.nMap}
//       var reply DoJobReply;
//       ok := call(worker, "Worker.DoJob", args, &reply)
//       if ok == false {
//         fmt.Printf("DoWork: RPC %s DoJob error\n", worker)
//       } else {
//         l.PushBack(cur_m)
//       }
//       // 
//       mr.registerChannel <- worker
//     }(cur_m)
//   }

  log.Printf("Master Reduce Done.\n")

  return mr.KillWorkers()
}
