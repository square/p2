
/*

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

*/
package curl

import (
	"os"
	"log"
	"errors"
	"fmt"
	"net"
	"net/http"
	"bytes"
	"io"
	"time"
	"strings"
)

type IocopyStat struct {
	Stat string					// dial,download
	Done bool 					// download is done
	Begin time.Time 		// download begin time
	Dur time.Duration 	// download elapsed time
	Per float64 				// complete percent. range 0.0 ~ 1.0
	Size int64 					// bytes downloaded
	Speed int64 				// bytes per second
	Length int64 				// content length
	Durstr string 			// pretty format of Dur. like: 10:11
	Perstr string 			// pretty format of Per. like: 3.9%
	Sizestr string  		// pretty format of Size. like: 1.1M, 3.5G, 33K
	Speedstr string 		// pretty format of Speed. like 1.1M/s
	Lengthstr string 		// pretty format of Length. like: 1.1M, 3.5G, 33K
}

type Control struct {
	stop bool
	maxSpeed int64
	st *IocopyStat
	readTimeout time.Duration
	dialTimeout time.Duration
	deadline time.Time
}

type IocopyCb func (st IocopyStat) error

type deadlineS interface {
	SetReadDeadline(t time.Time) error
}

func (c *Control) Stop() {
	c.stop = true
}

func (c *Control) Stat() (IocopyStat) {
	c.st.update()
	return *c.st
}

func (c *Control) MaxSpeed(s int64) {
	c.maxSpeed = s
}

func toFloat(o interface{}) (can bool, f float64) {
	switch o.(type) {
	case string:
		str := o.(string)
		n, _ := fmt.Sscanf(str, "%f", &f)
		if n == 1 {
			can = true
		}
	case float64:
		f = o.(float64)
		can = true
	case int:
		f = float64(o.(int))
		can = true
	case int64:
		f = float64(o.(int64))
		can = true
	}
	return
}

func optGet(name string, opts []interface{}) (got bool, val interface{}) {
	for i, o := range opts {
		switch o.(type) {
		case string:
			stro := o.(string)
			if strings.HasPrefix(stro, name) {
				if len(stro) == len(name) {
					if i+1 < len(opts) {
						val = opts[i+1]
					}
				} else {
					val = stro[len(name):]
				}
				got = true
				return
			}
		}
	}
	return
}

func optDuration(name string, opts []interface{}) (got bool, dur time.Duration) {
	var val interface{}
	var f float64
	if got, val = optGet(name, opts); !got { return }
	if dur, got = val.(time.Duration); got { return }
	if got, f = toFloat(val); !got { return }
	dur = time.Duration(float64(time.Second)*f)
	return
}

func optTime(name string, opts []interface{}) (got bool, tm time.Time) {
	var val interface{}
	got, val = optGet(name, opts)
	tm, got = val.(time.Time)
	return
}

func optInt64(name string, opts []interface{}) (got bool, i int64) {
	var val interface{}
	var f float64
	got, val = optGet(name, opts)
	if got, f = toFloat(val); !got { return }
	i = int64(f)
	return
}

func dbp(opts ...interface{}) {
	if false {
		log.Println(opts...)
	}
}

func (st *IocopyStat) update() {
	st.Stat = "downloading"
	if st.Length > 0 {
		st.Per = float64(st.Size)/float64(st.Length)
	}
	st.Dur = time.Since(st.Begin)
	st.Perstr = PrettyPer(st.Per)
	st.Sizestr = PrettySize(st.Size)
	st.Lengthstr = PrettySize(st.Length)
	st.Speedstr = PrettySpeed(st.Speed)
	st.Durstr = PrettyDur(st.Dur)
}

func (st *IocopyStat) finish() {
	dur := float64(time.Since(st.Begin))/float64(time.Second)
	st.Speed = int64(float64(st.Size) / dur)
	st.Per = 1.0
	st.update()
}

func optIntv(opts ...interface{}) (intv time.Duration) {
	var hasintv bool
	hasintv, intv = optDuration("cbinterval=", opts)
	if !hasintv {
		intv = time.Second
	}
	return
}

type mywriter struct {
	io.Writer
	n int64
	curn int64
	maxn int64
	maxtm time.Time
}

func (m *mywriter) Write(p []byte) (n int, err error) {
	n, err = m.Writer.Write(p)
	m.n += int64(n)
	m.curn += int64(n)
	if m.maxn != 0 && m.curn > m.maxn {
		time.Sleep(m.maxtm.Sub(time.Now()))
	}
	return
}

func IoCopy(
	r io.ReadCloser,
	length int64,
	w io.Writer,
	opts ...interface{},
) (err error) {
	var st *IocopyStat
	var cb IocopyCb
	var ct *Control

	for _, o := range opts {
		switch o.(type) {
		case *IocopyStat:
			st = o.(*IocopyStat)
		case *Control:
			ct = o.(*Control)
		case func(IocopyStat)error:
			cb = o.(func(IocopyStat)error)
		}
	}

	myw := &mywriter{Writer:w}
	if st == nil {
		st = &IocopyStat{}
	}
	if ct == nil {
		ct = &Control{st:st}
	}

	var rto time.Duration
	var hasrto bool
	hasrto, rto = optDuration("readtimeout=", opts)
	if !hasrto {
		hasrto, rto = optDuration("timeout=", opts)
	}

	var deadtm time.Time
	var deaddur time.Duration
	var hasdeadtm bool
	var hasdeaddur bool
	hasdeadtm, deadtm = optTime("deadline=", opts)
	if !hasdeadtm {
		hasdeaddur, deaddur = optDuration("deadline=", opts)
	}
	if hasdeaddur {
		hasdeadtm = true
		deadtm = time.Now().Add(deaddur)
	}

	intv := optIntv(opts)

	_, ct.maxSpeed = optInt64("maxspeed=", opts)

	st.Begin = time.Now()
	st.Length = length

	done := make(chan int, 0)
	go func () {
		if ct.maxSpeed == 0 {
			_, err = io.Copy(myw, r)
		} else {
			tm := time.Now()
			for {
				var nn int64
				nn, err = io.CopyN(myw, r, ct.maxSpeed)
				dur := time.Since(tm)
				if dur < time.Second {
					time.Sleep(time.Second - dur)
				}
				tm = time.Now()
				if nn != ct.maxSpeed || err != nil {
					break
				}
			}
		}
		if err == io.EOF {
			err = nil
		}
		done <- 1
	}()

	defer r.Close()

	var n, idle int64

	myw.maxn = ct.maxSpeed * int64(intv) / int64(time.Second)
	for {
		myw.maxtm = time.Now().Add(intv)
		myw.curn = 0
		select {
		case <-done:
			st.Size = myw.n
			st.Speed = myw.n - n
			st.finish()
			if cb != nil { err = cb(ct.Stat())	}
			if err != nil { return }
			return
		case <-time.After(intv):
			if ct.stop {
				err = errors.New("user stops")
				return
			}
			st.Size = myw.n
			st.Speed = myw.n - n
			if cb != nil { err = cb(ct.Stat())	}
			if err != nil { return }
			if myw.n != n {
				n = myw.n
				idle = 0
			} else {
				idle++
			}
			//log.Println("idle", idle, myw.n, n, time.Duration(3)*intv)
			if hasrto && time.Duration(idle)*intv > rto {
				err = errors.New("read timeout")
				return
			}
			if hasdeadtm && time.Now().After(deadtm) {
				err = errors.New("deadline reached")
				return
			}
		}
	}

	return
}

func Dial(url string, opts ...interface{}) (
	err error, r io.ReadCloser, length int64,
) {
	var req *http.Request
	var cb IocopyCb

	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}

	var header http.Header
	for _, o := range opts {
		switch o.(type) {
		case http.Header:
			header = o.(http.Header)
		case func(IocopyStat)error:
			cb = o.(func(IocopyStat)error)
		}
	}

	hasdto, dto := optDuration("dialtimeout=", opts)
	if !hasdto {
		hasdto, dto = optDuration("timeout=", opts)
	}

	intv := optIntv(opts)

	if header == nil {
		header = http.Header {
			"Accept" : {"*/*"},
			"User-Agent" : {"curl/7.21.6 (i686-pc-linux-gnu) libcurl/7.21.6 OpenSSL/1.0.0e zlib/1.2.3.4 libidn/1.22"},
		}
	}
	req.Header = header

	var resp *http.Response
	var conn net.Conn

	tr := &http.Transport {
		//DisableCompression: true,
		Dial: func(network, addr string) (c net.Conn, e error) {
			if hasdto {
				c, e = net.DialTimeout(network, addr, dto)
			} else {
				c, e = net.Dial(network, addr)
			}
			conn = c
			return
		},
	}
	client := &http.Client{
		Transport: tr,
	}

	callcb := func (st IocopyStat) bool {
		if cb != nil {
			err = cb(st)
		}
		return err != nil
	}

	done := make(chan int, 1)
	go func() {
		defer func() {
			recover()
		}()
		resp, err = client.Do(req)
		done <- 1
	}()

	starttm := time.Now()

	if callcb(IocopyStat{Stat:"connecting"}) { return }
	out: for {
		select {
		case <-done:
			break out
		case <-time.After(intv):
			if hasdto && time.Now().After(starttm.Add(dto)) {
				err = errors.New("dial timeout")
				return
			}
			if callcb(IocopyStat{Stat:"connecting"}) {
				return
			}
		}
	}

	if err != nil {
		return
	}

	r = resp.Body
	length = resp.ContentLength
	return
}

func String(url string, opts ...interface{}) (err error, body string) {
	var b bytes.Buffer
	err = Write(url, &b, opts...)
	body = string(b.Bytes())
	return
}

func Bytes(url string, opts ...interface{}) (err error, body []byte) {
	var b bytes.Buffer
	err = Write(url, &b, opts...)
	body = b.Bytes()
	return
}

func File(url string, path string, opts ...interface{}) (err error) {
	var w io.WriteCloser
	w, err = os.Create(path)
	if err != nil {
		return
	}
	defer w.Close()
	err = Write(url, w, opts...)
	return
}

func Write(url string, w io.Writer, opts ...interface{}) (err error) {
	var r io.ReadCloser
	var length int64
	err, r, length = Dial(url, opts...)
	if err != nil {
		return
	}
	err = IoCopy(r, length, w, opts...)
	return
}

func PrettyDur(dur time.Duration) string {
	d := float64(dur)/float64(time.Second)
	if d < 60*60 {
		return fmt.Sprintf("%d:%.2d", int(d/60), int(d)%60)
	}
	return fmt.Sprintf("%d:%.2d:%.2d", int(d/3600), int(d/60)%60, int(d)%60)
}

func PrettyPer(f float64) string {
	return fmt.Sprintf("%.1f%%", f*100)
}

func prettySize(_size interface{}, mul float64, tag []string) (ret string) {
	var size float64
	switch _size.(type) {
	case int64:
		size = float64(_size.(int64))
	case int:
		size = float64(_size.(int))
	case float64:
		size = _size.(float64)
	default:
		return
	}
	size *= mul
	if size < 1024 {
		return fmt.Sprintf("%d%s", size, tag[0])
	}
	if size < 1024*1024 {
		return fmt.Sprintf("%.1fKbits", size/1024)
	}
	if size < 1024*1024*1024 {
		return fmt.Sprintf("%.1fMbits", size/1024/1024)
	}
	return fmt.Sprintf("%.1fGbits", size/1024/1024/1024)

}

func PrettySize2(size interface{}) string {
	return prettySize(size, 8, []string{
		"Bits", "KBits", "MBits", "GBits",
	})
}

func PrettySize(size interface{}) string {
	return prettySize(size, 1, []string{
		"B", "K", "M", "G",
	})
}

func PrettySpeed(s int64) string {
	return fmt.Sprintf("%s/s", PrettySize(s))
}

