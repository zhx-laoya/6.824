package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import "6.824/src/mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	//加载map函数以及reduce函数
	mapf, reducef := loadPlugin(os.Args[1])
	//将这两个分发给工作机
	mr.Worker(mapf, reducef)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//加载插件文件如.so文件，返回一个keyvalue,以及一个func
//实验中为加载map函数和reduce函数
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 动态加载共享对象文件（插件）
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	//在插件中查找并加载名为Map的函数，返回的xmapf为一个plugin.Symbol对象，本质上是一个接口
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	//断言这个接口为一个func(string, string) []mr.KeyValue的函数
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	//在插件中查找并加载名为Reduce的函数，返回的xreducef为一个plugin.symbol对象，本质上是一个接口
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	//断言这个接口为一个func(string, []string) string函数
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
