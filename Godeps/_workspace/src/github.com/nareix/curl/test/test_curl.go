
package main

import (
	"github.com/go-av/curl"
	"fmt"
	//"time"
)
		//"http://www.kernel.org/pub/linux/kernel/v3.x/linux-3.9.4.tar.xz",
		//"http://youku.com",
		//"http://dldir1.qq.com/qqfile/qq/QQ2013/2013Beta3/6565/QQ2013Beta3.exe",

func test1(url string, opts ...interface{}) {
  var st curl.IocopyStat
	err := curl.File(
		url,
		"a.exe",
		append(opts, &st)...)
  fmt.Println(err, "size=", st.Size, "average speed=", st.Speed)
}

func test2(url string, opts ...interface{}) {
	curl.File(
		url,
		"a.exe",
		append(opts,
		func (st curl.IocopyStat) error {
			fmt.Println(st.Stat, st.Perstr, st.Sizestr, st.Lengthstr, st.Speedstr, st.Durstr)
			return nil
		}, "timeout=10")...,
	)
}

func test3() {
	curl.File(
		"http://tk.wangyuehd.com/soft/skycn/WinRAR.exe_2.exe",
		"a.exe",
		func (st curl.IocopyStat) error {
			fmt.Println(st.Perstr, st.Sizestr, st.Lengthstr, st.Speedstr, st.Durstr)
			return nil
		},
		"maxspeed=", 30*1000,
	)
}

func main() {
	test2(
		"http://tk.wangyuehd.com/soft/skycn/WinRAR.exe_2.exe",
		"maxspeed=", 1000)
}

