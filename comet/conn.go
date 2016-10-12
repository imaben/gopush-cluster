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
	"fmt"
	log "github.com/alecthomas/log4go"
	"net"
)

// Connection
type Connection struct {
	Conn    net.Conn
	Proto   uint8
	Version string
	Buf     chan []byte // 所有的发送内容均是写入到此channel
}

// HandleWrite start a goroutine get msg from chan, then send to the conn.
// 正式负责connection循环写入的goroutine
func (c *Connection) HandleWrite(key string) {
	go func() {
		var (
			n   int
			err error
		)
		log.Debug("user_key: \"%s\" HandleWrite goroutine start", key)
		// 循环等待可写信号
		for {
			msg, ok := <-c.Buf
			if !ok {
				log.Debug("user_key: \"%s\" HandleWrite goroutine stop", key)
				return
			}
			if c.Proto == WebsocketProto {
				// raw
				n, err = c.Conn.Write(msg)
			} else if c.Proto == TCPProto {
				// redis protocol 此处用的redis协议
				msg = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), string(msg)))
				n, err = c.Conn.Write(msg)
			} else {
				log.Error("unknown connection protocol: %d", c.Proto)
				panic(ErrConnProto)
			}
			// update stat
			if err != nil {
				log.Error("user_key: \"%s\" conn.Write() error(%v)", key, err)
				MsgStat.IncrFailed(1)
			} else {
				log.Debug("user_key: \"%s\" write \r\n========%s(%d)========", key, string(msg), n)
				MsgStat.IncrSucceed(1)
			}
		}
	}()
}

// Write different message to client by different protocol
func (c *Connection) Write(key string, msg []byte) {
	select {
	case c.Buf <- msg:
	default:
		c.Conn.Close()
		log.Warn("user_key: \"%s\" discard message: \"%s\" and close connection", key, string(msg))
	}
}
