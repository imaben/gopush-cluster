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
	"bufio"
	"errors"
	log "github.com/alecthomas/log4go"
	"io"
	"net"
	"strconv"
	"time"
)

// 协议中规定的最小必选参数个数貌似是3个，不知道这里为何是1个
const (
	minCmdNum = 1
	maxCmdNum = 5
)

var (
	// cmd parse failed
	ErrProtocol = errors.New("cmd format error")
)

// tcpBuf cache.
type tcpBufCache struct {
	// 这是个channel, channel, channel!!!
	instance []chan *bufio.Reader
	round    int
}

// newTCPBufCache return a new tcpBuf cache.
func newtcpBufCache() *tcpBufCache {
	// instance默认是CPU的个数
	// 假如instance=4 bufio.num=128
	// 则inst = 4 * 128
	inst := make([]chan *bufio.Reader, 0, Conf.BufioInstance)
	log.Debug("create %d read buffer instance", Conf.BufioInstance)
	for i := 0; i < Conf.BufioInstance; i++ {
		inst = append(inst, make(chan *bufio.Reader, Conf.BufioNum))
	}
	return &tcpBufCache{instance: inst, round: 0}
}

// Get return a chan bufio.Reader (round-robin).
func (b *tcpBufCache) Get() chan *bufio.Reader {
	rc := b.instance[b.round]
	// split requets to diff buffer chan
	if b.round++; b.round == Conf.BufioInstance {
		b.round = 0
	}
	return rc
}

// newBufioReader get a Reader by chan, if chan empty new a Reader.
func newBufioReader(c chan *bufio.Reader, r io.Reader) *bufio.Reader {
	select {
	case p := <-c:
		p.Reset(r)
		return p
	default:
		log.Warn("tcp bufioReader cache empty")
		return bufio.NewReaderSize(r, Conf.RcvbufSize)
	}
}

// putBufioReader pub back a Reader to chan, if chan full discard it.
func putBufioReader(c chan *bufio.Reader, r *bufio.Reader) {
	r.Reset(nil)
	select {
	case c <- r:
	default:
		log.Warn("tcp bufioReader cache full")
	}
}

// StartTCP Start tcp listen.
func StartTCP() error {
	for _, bind := range Conf.TCPBind {
		log.Info("start tcp listen addr:\"%s\"", bind)
		go tcpListen(bind)
	}

	return nil
}

func tcpListen(bind string) {
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		log.Error("net.ResolveTCPAddr(\"tcp\"), %s) error(%v)", bind, err)
		panic(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Error("net.ListenTCP(\"tcp4\", \"%s\") error(%v)", bind, err)
		panic(err)
	}
	// free the listener resource
	defer func() {
		log.Info("tcp addr: \"%s\" close", bind)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()
	// init reader buffer instance
	// 创建一个tcp buffer
	rb := newtcpBufCache()
	for {
		// 开始监听端口
		log.Debug("start accept")
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Error("listener.AcceptTCP() error(%v)", err)
			continue
		}
		// 设置keepalive
		if err = conn.SetKeepAlive(Conf.TCPKeepalive); err != nil {
			log.Error("conn.SetKeepAlive() error(%v)", err)
			conn.Close()
			continue
		}
		// 设置连接读取buffer大小
		if err = conn.SetReadBuffer(Conf.RcvbufSize); err != nil {
			log.Error("conn.SetReadBuffer(%d) error(%v)", Conf.RcvbufSize, err)
			conn.Close()
			continue
		}
		// 设置连接写入buffer大小
		if err = conn.SetWriteBuffer(Conf.SndbufSize); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)", Conf.SndbufSize, err)
			conn.Close()
			continue
		}
		// first packet must sent by client in specified seconds
		// 设置心跳包连接检查机制,防止对方突然拨掉网线或崩溃
		// 主要是解决检测网络掉线问题
		// 每隔fitstPacketTimedoutSec发送一次心跳包
		if err = conn.SetReadDeadline(time.Now().Add(fitstPacketTimedoutSec)); err != nil {
			log.Error("conn.SetReadDeadLine() error(%v)", err)
			conn.Close()
			continue
		}
		rc := rb.Get()
		// one connection one routine
		go handleTCPConn(conn, rc)
		log.Debug("accept finished")
	}
}

// hanleTCPConn handle a long live tcp connection.
func handleTCPConn(conn net.Conn, rc chan *bufio.Reader) {
	addr := conn.RemoteAddr().String()
	log.Debug("<%s> handleTcpConn routine start", addr)
	rd := newBufioReader(rc, conn)
	if args, err := parseCmd(rd); err == nil {
		// return buffer bufio.Reader
		putBufioReader(rc, rd)
		switch args[0] {
		case "sub": // 订阅指令
			SubscribeTCPHandle(conn, args[1:]) // args:将除`cmd`以外的所有参数传入
			break
		default:
			conn.Write(ParamReply) // 返回错误信息 此处-p为参数错误
			log.Warn("<%s> unknown cmd \"%s\"", addr, args[0])
			break
		}
	} else {
		// return buffer bufio.Reader
		putBufioReader(rc, rd)
		log.Error("<%s> parseCmd() error(%v)", addr, err)
	}
	// close the connection
	if err := conn.Close(); err != nil {
		log.Error("<%s> conn.Close() error(%v)", addr, err)
	}
	log.Debug("<%s> handleTcpConn routine stop", addr)
	return
}

// SubscribeTCPHandle handle the subscribers's connection.
// 处理订阅者相关信息
func SubscribeTCPHandle(conn net.Conn, args []string) {
	argLen := len(args)
	addr := conn.RemoteAddr().String()
	if argLen < 2 { // 参数错误
		conn.Write(ParamReply)
		log.Error("<%s> subscriber missing argument", addr)
		return
	}
	// key, heartbeat
	// key, heartbeat均为必选项
	key := args[0]
	if key == "" {
		conn.Write(ParamReply)
		log.Warn("<%s> key param error", addr)
		return
	}
	heartbeatStr := args[1]
	i, err := strconv.Atoi(heartbeatStr)
	if err != nil {
		conn.Write(ParamReply)
		log.Error("<%s> user_key:\"%s\" heartbeat:\"%s\" argument error (%v)", addr, key, heartbeatStr, err)
		return
	}
	if i < minHearbeatSec { // 默认最小心跳时长为30秒
		conn.Write(ParamReply)
		log.Warn("<%s> user_key:\"%s\" heartbeat argument error, less than %d", addr, key, minHearbeatSec)
		return
	}
	heartbeat := i + delayHeartbeatSec
	token := ""
	if argLen > 2 {
		token = args[2]
	}
	version := ""
	if argLen > 3 {
		version = args[3]
	}
	log.Info("<%s> subscribe to key = %s, heartbeat = %d, token = %s, version = %s", addr, key, heartbeat, token, version)
	// fetch subscriber from the channel
	// key在此处应该可以理解为是一个唯一ID,用来标识不同的客户端
	c, err := UserChannel.Get(key, true)
	if err != nil {
		log.Warn("<%s> user_key:\"%s\" can't get a channel (%s)", addr, key, err)
		if err == ErrChannelKey {
			conn.Write(NodeReply)
		} else {
			conn.Write(ChannelReply)
		}
		return
	}
	// auth token
	// 验证token正确性
	if ok := c.AuthToken(key, token); !ok {
		conn.Write(AuthReply)
		log.Error("<%s> user_key:\"%s\" auth token \"%s\" failed", addr, key, token)
		return
	}
	// add a conn to the channel
	// 将连接添加至channel
	connElem, err := c.AddConn(key, &Connection{Conn: conn, Proto: TCPProto, Version: version})
	if err != nil {
		log.Error("<%s> user_key:\"%s\" add conn error(%v)", addr, key, err)
		return
	}
	// blocking wait client heartbeat
	reply := []byte{0}
	// reply := make([]byte, HeartbeatLen)
	begin := time.Now().UnixNano()
	end := begin + Second
	// 维护长连接机制，负责发送心跳包、检测长连接是否健康
	// 所有可能出错的地方，全部break并移除conn
	// 看到这个地方，comet模块的长连接只可以对客户端推送消息
	// 客户端除了开始时的订阅和心跳包以外，不允许对服务端发送任何内容
	for {
		// more then 1 sec, reset the timer
		if end-begin >= Second {
			if err = conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(heartbeat))); err != nil {
				log.Error("<%s> user_key:\"%s\" conn.SetReadDeadLine() error(%v)", addr, key, err)
				break
			}
			begin = end
		}
		// 读取一个字节
		if _, err = conn.Read(reply); err != nil {
			if err != io.EOF {
				log.Warn("<%s> user_key:\"%s\" conn.Read() failed, read heartbeat timedout error(%v)", addr, key, err)
			} else {
				// client connection close
				log.Warn("<%s> user_key:\"%s\" client connection close error(%v)", addr, key, err)
			}
			break
		}
		// 如果读取的字节是心跳包，则回复相应的一个心跳包
		if string(reply) == Heartbeat {
			if _, err = conn.Write(HeartbeatReply); err != nil {
				log.Error("<%s> user_key:\"%s\" conn.Write() failed, write heartbeat to client error(%v)", addr, key, err)
				break
			}
			log.Debug("<%s> user_key:\"%s\" receive heartbeat (%s)", addr, key, reply)
		} else {
			log.Warn("<%s> user_key:\"%s\" unknown heartbeat protocol (%s)", addr, key, reply)
			break
		}
		end = time.Now().UnixNano()
	}
	// remove exists conn
	if err := c.RemoveConn(key, connElem); err != nil {
		log.Error("<%s> user_key:\"%s\" remove conn error(%v)", addr, key, err)
	}
	return
}

// parseCmd parse the tcp request command.
// 解析协议
// 具体协议说明见https://github.com/Terry-Mao/gopush-cluster/blob/master/wiki/comet/client_proto_zh.textile
func parseCmd(rd *bufio.Reader) ([]string, error) {
	// get argument number
	// 格式：*参数个数\r\n
	argNum, err := parseCmdSize(rd, '*')
	if err != nil {
		log.Error("tcp:cmd format error when find '*' (%v)", err)
		return nil, err
	}
	if argNum < minCmdNum || argNum > maxCmdNum {
		log.Error("tcp:cmd argument number length error")
		return nil, ErrProtocol
	}
	args := make([]string, 0, argNum)
	// 得到参数个数之后，依次读取具体参数
	for i := 0; i < argNum; i++ {
		// get argument length
		// 读取参数占用的字节数
		cmdLen, err := parseCmdSize(rd, '$')
		if err != nil {
			log.Error("tcp:parseCmdSize(rd, '$') error(%v)", err)
			return nil, err
		}
		// get argument data
		// 获取具体参数内容
		d, err := parseCmdData(rd, cmdLen)
		if err != nil {
			log.Error("tcp:parseCmdData() error(%v)", err)
			return nil, err
		}
		// append args
		args = append(args, string(d))
	}
	// 这里貌似没有区分是cmd key heartbeat token还是version,继续向下走
	return args, nil
}

// parseCmdSize get the request protocol cmd size.
func parseCmdSize(rd *bufio.Reader, prefix uint8) (int, error) {
	// get command size
	// 参数个数格式：*参数个数\r\n
	// 参数具体格式：$参数长度\r\n参数数据\r\n
	// 读取到第一个\n结束
	cs, err := rd.ReadBytes('\n')
	if err != nil {
		log.Error("tcp:rd.ReadBytes('\\n') error(%v)", err)
		return 0, err
	}
	csl := len(cs)
	if csl <= 3 || cs[0] != prefix || cs[csl-2] != '\r' {
		log.Error("tcp:\"%v\"(%d) number format error, length error or prefix error or no \\r", cs, csl)
		return 0, ErrProtocol
	}
	// skip the \r\n
	// 去除第一个字符(*|$)和最后的\r\n
	// 转换为整型，即是参数个数
	cmdSize, err := strconv.Atoi(string(cs[1 : csl-2]))
	if err != nil {
		log.Error("tcp:\"%v\" number parse int error(%v)", cs, err)
		return 0, ErrProtocol
	}
	return cmdSize, nil
}

// parseCmdData get the sub request protocol cmd data not included \r\n.
func parseCmdData(rd *bufio.Reader, cmdLen int) ([]byte, error) {
	d, err := rd.ReadBytes('\n')
	if err != nil {
		log.Error("tcp:rd.ReadBytes('\\n') error(%v)", err)
		return nil, err
	}
	dl := len(d)
	// check last \r\n
	if dl != cmdLen+2 || d[dl-2] != '\r' {
		log.Error("tcp:\"%v\"(%d) number format error, length error or no \\r", d, dl)
		return nil, ErrProtocol
	}
	// skip last \r\n
	return d[0:cmdLen], nil
}
