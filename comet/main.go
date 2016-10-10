// Copyright © 2014 Terry Mao, LiuDing All rights reserved.
// This file is part of gopush-cluster.

// gopush-cluster is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// gopush-cluster is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with gopush-cluster.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"flag"
	"github.com/Terry-Mao/gopush-cluster/perf"
	"github.com/Terry-Mao/gopush-cluster/process"
	"github.com/Terry-Mao/gopush-cluster/ver"
	log "github.com/alecthomas/log4go"
	"runtime"
)

func main() {
	// parse cmd-line arguments
	flag.Parse()
	log.Info("comet ver: \"%s\" start", ver.Version)
	// init config
	if err := InitConfig(); err != nil {
		panic(err)
	}
	// set max routine
	runtime.GOMAXPROCS(Conf.MaxProc)
	// init log
	log.LoadConfiguration(Conf.Log)
	defer log.Close()
	// start pprof
	// go 内置的性能查看工具，使用http
	perf.Init(Conf.PprofBind)
	// create channel
	// if process exit, close channel
	// 初始化通道池
	UserChannel = NewChannelList()
	defer UserChannel.Close()
	// start stats
	// 启动服务状态查看服务，用于通过http即可查看当前服务的工作状态
	StartStats()
	// start rpc
	// rpc服务初始化,用于和message、web模块通信
	if err := StartRPC(); err != nil {
		panic(err)
	}
	// start comet
	if err := StartComet(); err != nil {
		panic(err)
	}
	// init zookeeper
	zkConn, err := InitZK()
	if err != nil {
		if zkConn != nil {
			zkConn.Close()
		}
		panic(err)
	}
	// process init
	if err = process.Init(Conf.User, Conf.Dir, Conf.PidFile); err != nil {
		panic(err)
	}
	// init signals, block wait signals
	signalCH := InitSignal()
	HandleSignal(signalCH)
	// exit
	log.Info("comet stop")
}
