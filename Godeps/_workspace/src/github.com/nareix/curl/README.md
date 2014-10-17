Curl library for golang
====

* WITHOUT libcurl.so just using "net/http"
* Monitoring progress
* Timeouts and deadline
* Speed limit

## Simple Usage

### Curl string or bytes

    import "github.com/go-av/curl"
    
  	err, str := curl.String("http://www.baidu.com")
  	err, b := curl.Bytes("http://www.baidu.com")
  	
### Save to file or writer

    err := curl.File("www.baidu.com", "/tmp/index.html")
  
  	var w io.Writer
  	err := curl.Write("http://www.baidu.com", w)
  
### With timeout (both dial timeout and read timeout set)

  	curl.String("http://www.baidu.com", "timeout=10")
  	curl.String("http://www.baidu.com", "timeout=", 10)
  	curl.String("http://www.baidu.com", "timeout=", time.Second*10)
  
### With different dial timeout and read timeout

    curl.String("http://www.baidu.com", "dialtimeout=10", "readtimeout=20")
  	curl.String("http://www.baidu.com", "dialtimeout=", 10, "readtimeout=", time.Second*20)
  
### With deadline (if cannot download in 10s then die)

    curl.File("http://www.baidu.com", "index.html", "deadline=", time.Now().Add(time.Second*10))
  	curl.File("http://www.baidu.com", "index.html", "deadline=10")
  	curl.File("http://www.baidu.com", "index.html", "deadline=", 10.0)
  	curl.File("http://www.baidu.com", "index.html", "deadline=", time.Second*10)
  
### With speed limit 

    curl.File("http://www.baidu.com", "index.html", "maxspeed=", 30*1024)
  
### With custom http header

    header := http.Header {
  		"User-Agent" : {"curl/7.29.0"},
  	}
  	curl.File("http://www.baidu.com", "file", header)
  
### These params can be use in any function and in any order

    curl.File("http://www.baidu.com", "index.html", "timeout=", 10, header)
  	curl.String("http://www.baidu.com", index.html", timeout=", 10)

## Advanced Usage

### Get detail info

    var st curl.IocopyStat
    curl.File(
      "http://tk.wangyuehd.com/soft/skycn/WinRAR.exe_2.exe", 
    	"a.exe",
    	&st)
    fmt.Println("size=", st.Sizestr, "average speed=", st.Speedstr)
    
outputs

    size= 1307880 average speed= 118898
    
### Monitor progress

    curl.File(
  		"http://tk.wangyuehd.com/soft/skycn/WinRAR.exe_2.exe",
  		"a.exe",
  		func (st curl.IocopyStat) error {
  			fmt.Println(st.Perstr, st.Sizestr, st.Lengthstr, st.Speedstr, st.Durstr)
        // return errors.New("I want to stop")
  			return nil
  		},
  	)

outputs

    5.1% 65.2K 1.2M 65.2K/s 0:01
    9.3% 119.1K 1.2M 53.9K/s 0:02
    14.0% 178.7K 1.2M 59.6K/s 0:03
    18.7% 238.2K 1.2M 59.6K/s 0:04
    23.9% 304.9K 1.2M 66.6K/s 0:05
    32.4% 414.0K 1.2M 109.2K/s 0:06
    35.0% 446.7K 1.2M 32.6K/s 0:07
    43.1% 550.2K 1.2M 103.5K/s 0:08
    48.3% 616.8K 1.2M 66.6K/s 0:09
    58.2% 743.0K 1.2M 126.2K/s 0:10
    71.3% 910.3K 1.2M 167.3K/s 0:11
    73.6% 940.1K 1.2M 29.8K/s 0:12
    73.6% 940.1K 1.2M 0B/s 0:13
    81.4% 1.0M 1.2M 99.3K/s 0:14
    86.5% 1.1M 1.2M 65.2K/s 0:15
    94.8% 1.2M 1.2M 106.3K/s 0:16
    100.0% 1.2M 1.2M 79.8K/s 0:16

### Set monitor callback interval

  	curl.File("xxxx", "xxx", cb, "cbinterval=", 0.5) // 0.5 second

### Curl in goroutine

  	con := &curl.Control{}
  	go curl.File("xxx", "xxx", con)
  	// and then get stat
  	st := con.Stat() 
  	// or stop
  	con.Stop()
    // set max speed
    con.MaxSpeed(1024*10)
    // cancel max speed
    con.MaxSpeed(0)
  
### Just dial

    err, r, length := curl.Dial("http://weibo.com", "timeout=11")
    fmt.Println("contentLength=", length)
  
## Useful Functions

### Functions format size, speed pretty

  	curl.PrettySize(13500) // 13.5K
  	curl.PrettySize(2500000) // 2.5M
  	curl.PrettyPer(0.345) // 34.5%
  	curl.PrettySpeed(1200) // 1.2K/s
  	curl.PrettyDur(time.Second*66) // 1:06
  
### Progressed io.Copy

    r, _ := os.Open("infile")
    w, _ := os.Create("outfile")
    length := 1024*888
    cb := func (st curl.IocopyStat) error {
    		fmt.Println(st.Perstr, st.Sizestr, st.Lengthstr, st.Speedstr, st.Durstr)
  			return nil
  	}
  	curl.IoCopy(r, length, w, "readtimeout=12", cb)


