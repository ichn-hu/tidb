package globalkilltest

import (
	"context"
	"fmt"
	"net"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&TestPdProxySuite{})

type TestPdProxySuite struct{}

func (s *TestPdProxySuite) TestPdProxy(c *C) {
	srcPort, dstPort := "3334", "3333"
	l, err := net.Listen("tcp", "localhost"+":"+dstPort)
	c.Assert(err, IsNil)
	defer l.Close()

	var p pdProxy
	p.AddRoute("localhost"+":"+srcPort, to("localhost"+":"+dstPort))
	err = p.Start()
	c.Assert(err, IsNil)

	ch := make(chan []byte, 10)
	defer close(ch)

	go func(ch chan []byte) {
		conn, err := l.Accept()
		for {
			c.Assert(err, IsNil)
			buf := make([]byte, 1)
			// Read the incoming connection into the buffer.
			_, err = conn.Read(buf)
			if err != nil {
				break
			}
			fmt.Println(buf)
			ch <- buf
		}
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "EOF")
		fmt.Println("EOF")
	}(ch)

	var d net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := d.DialContext(ctx, "tcp", "localhost"+":"+srcPort)
	c.Assert(err, IsNil)
	defer conn.Close()

	_, err = conn.Write([]byte{0x33})
	c.Assert(err, IsNil)
	c.Assert(<-ch, DeepEquals, []byte{0x33})

	//p.Close()
	//p.closeAllConnections()

	_, err = conn.Write([]byte{0x34})
	//c.Assert(err, NotNil)
	c.Assert(<-ch, DeepEquals, []byte{0x34})
}
