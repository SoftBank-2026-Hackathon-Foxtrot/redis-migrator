package main

// statemig-latency: TCP forwarder that adds a fixed one-way delay, to measure
// a cutover as if the source Redis were in another cloud.
//
// env: LISTEN (default :6379), TARGET (host:port), DELAY_MS (one way)

import (
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

type chunk struct {
	data []byte
	due  time.Time
}

// relay copies src → dst, delaying every chunk without reordering or throttling.
func relay(src, dst net.Conn, delay time.Duration) {
	q := make(chan chunk, 4096)
	go func() {
		defer dst.Close()
		for c := range q {
			time.Sleep(time.Until(c.due))
			if _, err := dst.Write(c.data); err != nil {
				return
			}
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			q <- chunk{append([]byte(nil), buf[:n]...), time.Now().Add(delay)}
		}
		if err != nil {
			if err != io.EOF {
				log.Print(err)
			}
			close(q)
			return
		}
	}
}

func main() {
	listen, target := os.Getenv("LISTEN"), os.Getenv("TARGET")
	if listen == "" {
		listen = ":6379"
	}
	ms, err := strconv.Atoi(os.Getenv("DELAY_MS"))
	if err != nil || target == "" {
		log.Fatal("TARGET and DELAY_MS are required")
	}
	delay := time.Duration(ms) * time.Millisecond
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s → %s, %v each way", listen, target, delay)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			up, err := net.Dial("tcp", target)
			if err != nil {
				log.Print(err)
				c.Close()
				return
			}
			go relay(c, up, delay)
			relay(up, c, delay)
		}()
	}
}
