package main

import (
	"github.com/liusl104/go-mydumper/mydumper"
)

func main() {
	/*	var cpuProfile = "cpuProfile.prof"
		// 启动 CPU 性能采集
		var err error
		var cpu *os.File
		cpu, err = os.Create(cpuProfile)
		if err != nil {
			// 处理错误（如无法创建文件）
			panic("无法创建 CPU 性能文件: " + err.Error())
		}
		defer cpu.Close() // 程序退出时关闭文件
		if err = pprof.StartCPUProfile(cpu); err != nil {
			panic("无法启动 CPU 性能分析: " + err.Error())
		}
		defer pprof.StopCPUProfile() // 程序退出时停止采集
	*/
	mydumper.CommandDump()
	// 生成内存性能分析报告（在核心逻辑执行后）
	/*var memProfile = "memProfile.prof"
	var mem *os.File
	mem, err = os.Create(memProfile)
	if err != nil {
		panic("无法创建内存性能文件: " + err.Error())
	}
	defer mem.Close()
	// 写入当前内存使用情况（inuse_space）
	if err = pprof.WriteHeapProfile(mem); err != nil {
		panic("无法写入内存性能分析: " + err.Error())
	}
	// 生成阻塞分析报告（可选）
	var block *os.File
	blockProfile := "blockProfile.prof"
	block, err = os.Create(blockProfile)
	if err != nil {
		panic("无法创建阻塞分析文件: " + err.Error())
	}
	defer block.Close()
	if err = pprof.Lookup("block").WriteTo(block, 0); err != nil {
		panic("无法写入阻塞分析: " + err.Error())
	}*/

}
