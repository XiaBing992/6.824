/*
 * @Author: WXB 1763567512@qq.com
 * @Date: 2024-04-25 14:39:47
 * @LastEditors: WXB 1763567512@qq.com
 * @LastEditTime: 2024-05-20 16:34:09
 * @FilePath: /6.824/labrpc/labrpc.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package labrpc

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
)

// RPC请求消息
type reqMsg struct {
	endname  interface{}   // 对端
	svcMeth  string        // 函数名
	argsType reflect.Type  // 请求参数类型
	args     []byte        // 请求参数
	replyCh  chan replyMsg // 返回值
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type ClientEnd struct {
	endname interface{} // 接收方
	ch      chan reqMsg // 请求数据
	done    chan struct{}
}

// send RPC
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{} // 请求消息
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args) // 获取参数类型
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)     // 数据缓冲区
	qe := labgob.NewEncoder(qb) // 进行二进制编码
	if err := qe.Encode(args); err != nil {
		panic(err)
	}
	req.args = qb.Bytes()

	select {
	case e.ch <- req: // 写入数据
	case <-e.done:
		return false
	}

	// 等待请求被处理
	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := labgob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool
	longReordering bool
	ends           map[interface{}]*ClientEnd // 对端
	enabled        map[interface{}]bool
	servers        map[interface{}]*Server
	connections    map[interface{}]interface{}
	endCh          chan reqMsg // 请求消息
	done           chan struct{}
	count          int32
	bytes          int64
}

// 创建network数据结构
func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	rn.endCh = make(chan reqMsg)
	rn.done = make(chan struct{})

	go func() {
		for {
			select {
			case xreq := <-rn.endCh:
				atomic.AddInt32(&rn.count, 1)
				atomic.AddInt64(&rn.bytes, int64(len(xreq.args)))
				go rn.processReq(xreq)
			case <-rn.done:
				return
			}
		}
	}()

	return rn
}

func (rn *Network) Cleanup() {
	close(rn.done)
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

func (rn *Network) readEndnameInfo(endname interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longreordering bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

func (rn *Network) isServerDead(endname interface{}, servername interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.enabled[endname] == false || rn.servers[servername] != server {
		return true
	}

	return false
}

func (rn *Network) processReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.readEndnameInfo(req.endname)

	if enabled && servername != nil && server != nil {
		if reliable == false {
			//等待
			ms := (rand.Int() % 27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		// 超时后失败
		if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
			return
		}

		ech := make(chan replyMsg)

		// 执行请求
		go func() {
			r := server.dispatch(req) // 分发请求
			ech <- r                  // 写入结果
		}()

		var reply replyMsg
		replyOK := false
		serverDead := false
		for replyOK == false && serverDead == false {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = rn.isServerDead(req.endname, servername, server)
				if serverDead {
					go func() {
						<-ech
					}()
				}

			}
		}

		serverDead = rn.isServerDead(req.endname, servername, server)

		if replyOK == false || serverDead == true {
			req.replyCh <- replyMsg{false, nil}
		} else if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering == true && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
				req.replyCh <- reply
			})
		} else {
			atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
			req.replyCh <- reply
		}
	} else {
		ms := 0
		if rn.longDelays {
			ms = (rand.Int() % 7000)
		} else {
			ms = (rand.Int() % 100)
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
	}

}

func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	e := &ClientEnd{}
	e.endname = endname
	e.ch = rn.endCh
	e.done = rn.done
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	return e
}

func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

// 删除server
func (rn *Network) DeleteServer(servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = nil
}

func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.connections[endname] = servername
}

func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.enabled[endname] = enabled
}

func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

func (rn *Network) GetTotalCount() int {
	x := atomic.LoadInt32(&rn.count)
	return int(x)
}

func (rn *Network) GetTotalBytes() int64 {
	x := atomic.LoadInt64(&rn.bytes)
	return x
}

type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.services[svc.name] = svc
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()
	rs.count += 1

	dot := strings.LastIndex(req.svcMeth, ".") // 分离
	serviceName := req.svcMeth[:dot] // 服务名
	methodName := req.svcMeth[dot+1:] // 方法名

	service, ok := rs.services[serviceName] // 获取服务名

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		// 失败了返回所有可能的server
		choices := []string{}
		for k, _ := range rs.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

// 一个有函数的对象
type Service struct {
	name string
	rcvr reflect.Value
	typ reflect.Type
	methods map[string]reflect.Method // string - 方法
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m) // 拿到第m个方法
		mtype := method.Type
		mname := method.Name

		if method.PkgPath != "" || mtype.NumIn() != 3 || mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 0 {
				// 函数不满足要求
				fmt.Printf("bad method:%v\n", mname)
		} else {
			svc.methods[mname] = method // 建立函数名到函数实际实现的映射
		}
	}

	return svc
}

// 处理rpc请求
func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		args := reflect.New(req.argsType)

		// 参数反序列化
		ab := bytes.NewBuffer(req.args)
		ad := labgob.NewDecoder(ab)
		ad.Decode(args.Interface())

		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		// 调用函数
		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		rb := new(bytes.Buffer)
		re := labgob.NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}

