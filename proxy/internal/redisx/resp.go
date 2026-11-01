package redisx

// Minimal RESP codec (RESP2, plus RESP3 frame boundaries for relaying). The proxy relays reply frames as raw bytes; the syncer
// uses the parsed Value form.

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
)

var ErrProtocol = errors.New("resp protocol error")

type Value struct {
	Type byte // '+', '-', ':', '$', '*'
	Str  []byte
	Int  int64
	Arr  []Value
	Null bool
}

func (v Value) IsErr() bool { return v.Type == '-' }
func (v Value) Err() error {
	if v.Type == '-' {
		return errors.New(string(v.Str))
	}
	return nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrProtocol
	}
	return line, nil
}

// ReadCommand reads one client command (multibulk or inline). Returned slices
// are owned by the caller.
func ReadCommand(r *bufio.Reader) ([][]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		var args [][]byte
		for _, f := range splitFields(line[:len(line)-2]) {
			args = append(args, append([]byte(nil), f...))
		}
		if len(args) == 0 {
			return ReadCommand(r)
		}
		return args, nil
	}
	n, err := strconv.Atoi(string(line[1 : len(line)-2]))
	if err != nil {
		return nil, ErrProtocol
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if hdr[0] != '$' {
			return nil, ErrProtocol
		}
		size, err := strconv.Atoi(string(hdr[1 : len(hdr)-2]))
		if err != nil || size < 0 {
			return nil, ErrProtocol
		}
		buf := make([]byte, size+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, buf[:size])
	}
	return args, nil
}

func splitFields(b []byte) [][]byte {
	var out [][]byte
	start := -1
	for i, c := range b {
		if c == ' ' || c == '\t' {
			if start >= 0 {
				out = append(out, b[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		out = append(out, b[start:])
	}
	return out
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// WriteCommand encodes args as a multibulk command.
func WriteCommand(w *bufio.Writer, args ...[]byte) {
	w.WriteString("*")
	w.WriteString(strconv.Itoa(len(args)))
	w.WriteString("\r\n")
	for _, a := range args {
		w.WriteString("$")
		w.WriteString(strconv.Itoa(len(a)))
		w.WriteString("\r\n")
		w.Write(a)
		w.WriteString("\r\n")
	}
}

// ReadRawReply appends one complete reply frame to dst and returns it.
func ReadRawReply(r *bufio.Reader, dst []byte) ([]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return dst, err
	}
	dst = append(dst, line...)
	switch line[0] {
	case '+', '-', ':', '_', '#', ',', '(': // RESP3 null/bool/double/bignum are one line too
		return dst, nil
	case '$', '!', '=': // bulk string, RESP3 blob error, verbatim string
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return dst, ErrProtocol
		}
		if n < 0 {
			return dst, nil
		}
		start := len(dst)
		dst = append(dst, make([]byte, n+2)...)
		_, err = readFull(r, dst[start:])
		return dst, err
	case '*', '~', '>', '%', '|': // array, RESP3 set/push, map/attribute (2 frames per entry)
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return dst, ErrProtocol
		}
		if line[0] == '%' || line[0] == '|' {
			n *= 2
		}
		if line[0] == '|' { // an attribute precedes the actual reply
			n++
		}
		for i := 0; i < n; i++ {
			if dst, err = ReadRawReply(r, dst); err != nil {
				return dst, err
			}
		}
		return dst, nil
	}
	return dst, fmt.Errorf("%w: unsupported reply type %q", ErrProtocol, line[0])
}

// ReadValue parses one reply frame.
func ReadValue(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	body := line[1 : len(line)-2]
	v := Value{Type: line[0]}
	switch line[0] {
	case '+', '-':
		v.Str = append([]byte(nil), body...)
	case ':':
		v.Int, err = strconv.ParseInt(string(body), 10, 64)
	case '$':
		n, e := strconv.Atoi(string(body))
		if e != nil {
			return v, ErrProtocol
		}
		if n < 0 {
			v.Null = true
			return v, nil
		}
		buf := make([]byte, n+2)
		_, err = readFull(r, buf)
		v.Str = buf[:n]
	case '*':
		n, e := strconv.Atoi(string(body))
		if e != nil {
			return v, ErrProtocol
		}
		if n < 0 {
			v.Null = true
			return v, nil
		}
		v.Arr = make([]Value, n)
		for i := 0; i < n && err == nil; i++ {
			v.Arr[i], err = ReadValue(r)
		}
	default:
		return v, fmt.Errorf("%w: unsupported reply type %q", ErrProtocol, line[0])
	}
	return v, err
}

func SimpleReply(s string) []byte { return []byte("+" + s + "\r\n") }
func ErrorReply(s string) []byte  { return []byte("-" + s + "\r\n") }
