package main

import (
        "net"
        "fmt"
        "os"
        "log"
        "io"
        "time"
)

func checkError(err error){
          if err != nil {
              fmt.Fprintf(os.Stderr,"Fatal error: %s",err.Error())
              os.Exit(1)
          }
  }

func handleConnection(conn net.Conn,i int){
  fmt.Println("connect succeed! ID: ",i)
  i += 1
  time.Sleep(3* time.Second)
   _,_ = conn.Write([]byte("message from server"))
  time.Sleep(1* time.Second)
  conn.Close()
}
func server() {
  i := 0
  ln,err := net.Listen("tcp","localhost:8081")
  checkError(err)

  for {
    conn,err := ln.Accept()
    if err != nil {
          fmt.Fprintf(os.Stderr,"Fatal error: %s",err.Error())
          continue
    }
    i += 1
    go handleConnection(conn,i)
  }
}

func main() {
	go server()
  conn,err := net.Dial("tcp","127.0.0.1:8081")
  if err != nil {
  	log.Println(err)
  	return
  }
  var buf [512]byte
  for {
	  conn.SetDeadline(time.Now().Add(time.Second/2))
    _,err := conn.Read(buf[0:])
    if err != nil {
    	log.Println(err)
    }
    if err == io.EOF {
    	break
    }
  }
  time.Sleep(1* time.Millisecond)
  conn.Close()
}
