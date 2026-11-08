// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package guest

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// echoGuest echoes stdin to stdout. It sends the exit only after stdin closes.
type echoGuest struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	start  *ateenvv1alpha.StartProcessRequest
	input  chan []byte
	closed chan struct{}
}

func (g *echoGuest) StartProcess(_ context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	g.start = req
	return &ateenvv1alpha.Process{ProcessId: "p1"}, nil
}

func (g *echoGuest) WriteProcessInput(s ateenvv1alpha.ProcessService_WriteProcessInputServer) error {
	var n int64
	for {
		req, err := s.Recv()
		if err != nil {
			return err
		}
		if req.GetProcessId() != "" && req.GetProcessId() != "p1" {
			return io.ErrUnexpectedEOF
		}
		n += int64(len(req.GetData()))
		g.input <- append([]byte(nil), req.GetData()...)
		if req.GetClose() {
			close(g.closed)
			return s.SendAndClose(&ateenvv1alpha.WriteProcessInputResponse{BytesWritten: n})
		}
	}
}

func (g *echoGuest) StreamProcessOutput(_ *ateenvv1alpha.StreamProcessOutputRequest, s ateenvv1alpha.ProcessService_StreamProcessOutputServer) error {
	for {
		select {
		case b := <-g.input:
			if len(b) > 0 {
				if err := s.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: b}}); err != nil {
					return err
				}
			}
		case <-g.closed:
			// Drain input that arrived with the close message.
			for len(g.input) > 0 {
				b := <-g.input
				_ = s.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: b}})
			}
			return s.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{ExitCode: 3}}})
		}
	}
}

func TestExecStdinRoundTrip(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &echoGuest{input: make(chan []byte, 64), closed: make(chan struct{})}
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(srv, g)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	code, err := c.Exec(ctx, ExecOptions{
		Command: []string{"sshd", "-i"},
		Stdin:   strings.NewReader("hello\nworld\n"),
		Stdout:  &out,
		Timeout: 48 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if code != 3 {
		t.Errorf("exit code = %d, want 3", code)
	}
	if out.String() != "hello\nworld\n" {
		t.Errorf("stdout = %q, want echo of stdin", out.String())
	}
	if !g.start.GetStdin() {
		t.Error("StartProcessRequest.stdin = false, want true")
	}
	if got := g.start.GetTimeout().AsDuration(); got != 48*time.Hour {
		t.Errorf("timeout = %v, want 48h", got)
	}
}

// hangGuest runs a process that never exits and records signals.
type hangGuest struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	signals chan ateenvv1alpha.Signal
}

func (g *hangGuest) StartProcess(context.Context, *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	return &ateenvv1alpha.Process{ProcessId: "p1"}, nil
}

func (g *hangGuest) StreamProcessOutput(_ *ateenvv1alpha.StreamProcessOutputRequest, s ateenvv1alpha.ProcessService_StreamProcessOutputServer) error {
	<-s.Context().Done()
	return s.Context().Err()
}

func (g *hangGuest) SignalProcess(_ context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	g.signals <- req.GetSignal()
	return &ateenvv1alpha.Process{ProcessId: req.GetProcessId()}, nil
}

// A cancelled Exec (ssh sends SIGHUP to its ProxyCommand) must stop the remote
// process, or it stays as an orphan that holds a guest process slot.
func TestExecCancelSignalsRemote(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &hangGuest{signals: make(chan ateenvv1alpha.Signal, 1)}
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(srv, g)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	if _, err := c.Exec(ctx, ExecOptions{Command: []string{"sshd", "-i"}}); err == nil {
		t.Error("Exec returned no error after cancel")
	}
	select {
	case sig := <-g.signals:
		if sig != ateenvv1alpha.Signal_SIGNAL_TERM {
			t.Errorf("signal = %v, want SIGNAL_TERM", sig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote process got no signal after cancel")
	}
}

// resetGuest breaks the first output stream after "abc", as the 300 s proxy
// reset does, and checks that the second stream resumes at offset 3.
type resetGuest struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	calls   int
	offsets []int64
}

func (g *resetGuest) StartProcess(context.Context, *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	return &ateenvv1alpha.Process{ProcessId: "p1"}, nil
}

func (g *resetGuest) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, s ateenvv1alpha.ProcessService_StreamProcessOutputServer) error {
	g.calls++
	g.offsets = append(g.offsets, req.GetStdoutOffset())
	all := "abcdef"
	if err := s.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(all[req.GetStdoutOffset() : 3*g.calls])}}); err != nil {
		return err
	}
	if g.calls == 1 {
		return status.Error(codes.Internal, "stream terminated by RST_STREAM")
	}
	return s.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{}}})
}

func TestExecResumesOutputAfterReset(t *testing.T) {
	g := &resetGuest{}
	c := serve(t, func(s *grpc.Server) { ateenvv1alpha.RegisterProcessServiceServer(s, g) })
	var out bytes.Buffer
	code, err := c.Exec(context.Background(), ExecOptions{Command: []string{"x"}, Stdout: &out})
	if err != nil || code != 0 {
		t.Fatalf("Exec = %d, %v; want 0, nil", code, err)
	}
	if out.String() != "abcdef" {
		t.Errorf("stdout = %q, want %q (no loss, no repeat)", out.String(), "abcdef")
	}
	if len(g.offsets) != 2 || g.offsets[1] != 3 {
		t.Errorf("stdout offsets = %v, want [0 3]", g.offsets)
	}
}

// rotateGuest collects stdin over all WriteProcessInput streams.
type rotateGuest struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	mu      sync.Mutex
	streams int
	data    []byte
	closed  chan struct{}
}

func (g *rotateGuest) StartProcess(context.Context, *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	return &ateenvv1alpha.Process{ProcessId: "p1"}, nil
}

func (g *rotateGuest) WriteProcessInput(s ateenvv1alpha.ProcessService_WriteProcessInputServer) error {
	g.mu.Lock()
	g.streams++
	g.mu.Unlock()
	for {
		req, err := s.Recv()
		if err == io.EOF {
			return s.SendAndClose(&ateenvv1alpha.WriteProcessInputResponse{})
		}
		if err != nil {
			return err
		}
		g.mu.Lock()
		g.data = append(g.data, req.GetData()...)
		g.mu.Unlock()
		if req.GetClose() {
			close(g.closed)
		}
	}
}

func (g *rotateGuest) StreamProcessOutput(_ *ateenvv1alpha.StreamProcessOutputRequest, s ateenvv1alpha.ProcessService_StreamProcessOutputServer) error {
	<-g.closed
	return s.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{}}})
}

func TestExecRotatesIdleStdinStream(t *testing.T) {
	old := stdinRotate
	stdinRotate = 50 * time.Millisecond
	defer func() { stdinRotate = old }()

	g := &rotateGuest{closed: make(chan struct{})}
	c := serve(t, func(s *grpc.Server) { ateenvv1alpha.RegisterProcessServiceServer(s, g) })
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("a"))
		time.Sleep(300 * time.Millisecond) // idle for several rotations
		_, _ = pw.Write([]byte("b"))
		_ = pw.Close()
	}()
	if _, err := c.Exec(context.Background(), ExecOptions{Command: []string{"x"}, Stdin: pr}); err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if string(g.data) != "ab" {
		t.Errorf("stdin = %q, want %q", g.data, "ab")
	}
	if g.streams < 2 {
		t.Errorf("WriteProcessInput streams = %d, want >= 2 (rotation)", g.streams)
	}
}

// serve starts a gRPC server on a free port and returns a Client for it.
func serve(t *testing.T, register func(*grpc.Server)) *Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
