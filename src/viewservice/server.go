
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
// import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  v View

  primary_ready bool
  backup_ready bool

  primary_ack_time int
  backup_ack_time int


  // type View struct {
  //   Viewnum uint
  //   Primary string
  //   Backup string
  // }
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  // args.me
  vn := args.Viewnum

  log.Printf("Server get ping, Viewnum: %d , Me: %s \n" , args.Viewnum , args.Me)
  log.Printf("Server     view, Viewnum: %d , Primary: %s , Backup: %s , PrimaryReady: %s , BackupReady: %s\n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup , vs.primary_ready , vs.backup_ready)

  // first register or reborn

    if vn == 0 {                                 // ping 0
      if vs.v.Primary == "" && vs.v.Backup == "" {         // Primary  first primary

        vs.mu.Lock()
        vs.primary_ready = false
        vs.v.Primary = args.Me
        vs.v.Viewnum ++
        reply.View = vs.v
        vs.primary_ack_time = 0
        vs. mu.Unlock()

        log.Printf("[test] Server login Primary: %d , Primary: %s , Backup: %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup)

        return nil

      } 
      if vs.v.Primary == "" && vs.v.Backup != "" && vs.backup_ready == true /* wait for primary's acked */ {      // Primary empty , backup exist 

        vs.mu.Lock()
        vs.v.Primary = vs.v.Backup
        vs.v.Backup = ""
        vs.primary_ready = false
        vs.v.Viewnum ++
        reply.View = vs.v
        vs.primary_ack_time = 0
        vs.backup_ack_time = 0
        vs.mu.Unlock()

        log.Printf("[test] Backup replace Primary : %d , Primary: %s , Backup: %s , Backup_Ready? %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup , vs.backup_ready)

        return nil

      } 
      if vs.v.Backup == "" && vs.v.Primary != "" {                        // Backup  first backup

        vs.mu.Lock()
        vs.v.Backup = args.Me
        vs.v.Viewnum ++        // wait primary acked
        vs.backup_ready = false
        reply.View = vs.v
        vs.backup_ack_time = 0
        vs.mu.Unlock()

        log.Printf("[test] Server login Backup: %d , Primary: %s , Backup: %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup)

        return nil

      } 

      if vs.v.Primary != "" && vs.v.Primary == args.Me{                   // Primary restart , treat as dead

        vs.mu.Lock()
        vs.v.Primary = ""
        vs.primary_ack_time = 0
        reply.View = vs.v
        vs.primary_ready = false
        vs.mu.Unlock()

        log.Printf("[test] Primary restart , treat as dead: %d , Primary: %s , Backup: %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup)

        return nil

      }

    }

    if vn != vs.v.Viewnum && vn != 0 {                    // pint > 0   not fresh view
      if args.Me == vs.v.Primary {                    // Primary acked
        vs.mu.Lock()
        vs.primary_ready = true
        if vs.v.Backup != "" && vs.backup_ready == false {
          //vs.backup_ready = true                    // !!! Warn !!!     Primary first know the backup exist but not ack confirmed
          vs.primary_ready = false                    // !!! Warn !!!     Primary first know the backup exist but not ack confirmed
          // vs.v.Viewnum++  
        }
        reply.View = vs.v
        vs.primary_ack_time = 0
        vs.mu.Unlock()
        return nil

      } 

      if args.Me == vs.v.Backup {                     // Backup acked
        vs.mu.Lock()
        reply.View = vs.v
        vs.backup_ack_time = 0
        vs.mu.Unlock()
        return nil
      }


    }

  if vn == vs.v.Viewnum {                          // ping > 0   viewnum equle

    //fmt.Printf("[test] ping equil : %d , Primary: %s , Backup: %s , Backup_Ready? %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup , vs.backup_ready)


    if vs.v.Primary != "" && vs.v.Backup != "" && vs.primary_ready == true {    // Peace
      vs.mu.Lock()
      reply.View = vs.v
      if args.Me == vs.v.Primary{
        vs.primary_ready = true
        vs.backup_ready = true
        vs.primary_ack_time = 0
      }
      if args.Me == vs.v.Backup{
        vs.backup_ack_time = 0
      }
      vs.mu.Unlock()
    } else {               
                                                                                // Situation
      if args.Me == vs.v.Primary{                                 // Primary acked

        vs.mu.Lock()
        vs.primary_ready = true
        if vs.v.Backup != "" && vs.backup_ready == false {
          vs.backup_ready = true
          // vs.v.Viewnum++  
        }
        reply.View = vs.v
        vs.primary_ack_time = 0
        vs.mu.Unlock()
        return nil

      }

      if vs.v.Primary == "" && vs.v.Backup == args.Me && vs.backup_ready == true /* wait for primary's acked */ {   // Primary die , backup catch up

        log.Printf("[test] ping equil : %d , Primary: %s , Backup: %s , Backup_Ready? %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup , vs.backup_ready)

        vs.mu.Lock()
        vs.v.Primary = vs.v.Backup
        vs.primary_ready = false
        vs.v.Backup = ""
        vs.v.Viewnum ++
        vs.primary_ack_time = 0
        vs.backup_ack_time = 0
        vs.mu.Unlock()
        return nil  

      }

    }
    
  }


  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.

  vs.mu.Lock()
  reply.View = vs.v
  vs.mu.Unlock()

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  if vs.v.Primary != ""{
    vs.primary_ack_time++
  }
  if vs.v.Backup != ""{
    vs.backup_ack_time++
  }

  if vs.primary_ready == true && vs.primary_ack_time >= DeadPings {           // Primary die

    vs.mu.Lock()
    vs.primary_ack_time = 0
    vs.v.Primary = ""
    vs.mu.Unlock()

    log.Printf("[test] Primary timeout: %d , Primary: %s , Backup: %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup)

  }

  if vs.backup_ready == true && vs.backup_ack_time >= DeadPings {            // Backup die
    vs.mu.Lock()
    vs.backup_ack_time = 0
    vs.v.Backup = ""
    vs.mu.Unlock()

    log.Printf("[test] Backup timeout: %d , Primary: %s , Backup: %s \n" , vs.v.Viewnum , vs.v.Primary , vs.v.Backup)

  }


}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.

  // by fsc
  // init view
  vs.v.Primary = ""
  vs.v.Backup = ""
  vs.v.Viewnum = 0

  vs.primary_ready = false
  vs.backup_ready = false
  vs.primary_ack_time = 0
  vs.backup_ack_time = 0

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        log.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
