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
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
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
