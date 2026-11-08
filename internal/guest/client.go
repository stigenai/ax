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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Client interacts with the in-actor guest daemon.
type Client struct {
	grpcConn   *grpc.ClientConn
	process    ateenvv1alpha.ProcessServiceClient
	filesystem ateenvv1alpha.FileSystemServiceClient
}

// Dial connects to the guest daemon at the specified target (e.g., "127.0.0.1:80").
func Dial(target string) (*Client, error) {
	return DialTarget(target, "")
}

// DialTarget connects to the guest daemon at the specified target, optionally injecting
// the 'ate-target-actor' metadata header on all calls (required when routing via atenet-router).
func DialTarget(target string, targetActor string) (*Client, error) {
	target = strings.TrimPrefix(target, "http://")
	target = strings.TrimPrefix(target, "https://")

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if targetActor != "" {
		dialOpts = append(dialOpts,
			grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, "ate-target-actor", targetActor)
				return invoker(ctx, method, req, reply, cc, opts...)
			}),
			grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				ctx = metadata.AppendToOutgoingContext(ctx, "ate-target-actor", targetActor)
				return streamer(ctx, desc, cc, method, opts...)
			}),
		)
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to guest at %s: %w", target, err)
	}

	return &Client{
		grpcConn:   conn,
		process:    ateenvv1alpha.NewProcessServiceClient(conn),
		filesystem: ateenvv1alpha.NewFileSystemServiceClient(conn),
	}, nil
}

// Close terminates the underlying gRPC client connection.
func (c *Client) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}

// ExecOptions holds parameters for running a command in the task actor.
type ExecOptions struct {
	Command []string
	Cwd     string
	Env     map[string]string
	Stdout  io.Writer
	Stderr  io.Writer
	// Stdin, if set, feeds the process stdin. EOF on Stdin closes the remote stdin.
	Stdin io.Reader
	// Timeout overrides the guest watchdog (default 1h). Zero keeps the default.
	Timeout time.Duration
}

// Exec runs a command inside the task container and streams stdout/stderr until completion.
// It returns the process exit code.
func (c *Client) Exec(ctx context.Context, opts ExecOptions) (code int, err error) {
	if len(opts.Command) == 0 {
		return 1, errors.New("exec: command cannot be empty")
	}

	proc, err := c.process.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command: opts.Command,
		Cwd:     opts.Cwd,
		Env:     opts.Env,
		Stdin:   opts.Stdin != nil,
		Timeout: timeout(opts.Timeout),
	})
	if err != nil {
		return 1, fmt.Errorf("starting process: %w", err)
	}

	pid := proc.GetProcessId()
	// If Exec fails or ctx ends first (ssh sends SIGHUP to its ProxyCommand),
	// stop the remote process. Else it stays as an orphan that holds a guest
	// process slot until its timeout.
	defer func() {
		if err == nil {
			return
		}
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = c.process.SignalProcess(sctx, &ateenvv1alpha.SignalProcessRequest{ProcessId: pid, Signal: ateenvv1alpha.Signal_SIGNAL_TERM})
	}()
	if opts.Stdin != nil {
		go c.pumpStdin(ctx, pid, opts.Stdin, opts.Stderr)
	}

	// The guest spools output, so a broken stream (a proxy on the path resets
	// streams after 300 s) resumes at the byte offsets already received.
	var outOff, errOff int64
	failures := 0
	for {
		stream, err := c.process.StreamProcessOutput(ctx, &ateenvv1alpha.StreamProcessOutputRequest{
			ProcessId:    pid,
			StdoutOffset: outOff,
			StderrOffset: errOff,
			Follow:       true,
		})
		for err == nil {
			var out *ateenvv1alpha.ProcessOutput
			if out, err = stream.Recv(); err != nil {
				break
			}
			failures = 0
			if exit := out.GetExit(); exit != nil {
				return int(exit.GetExitCode()), nil
			}
			if data := out.GetStdout(); len(data) > 0 {
				outOff += int64(len(data))
				if opts.Stdout != nil {
					_, _ = opts.Stdout.Write(data)
				}
			}
			if data := out.GetStderr(); len(data) > 0 {
				errOff += int64(len(data))
				if opts.Stderr != nil {
					_, _ = opts.Stderr.Write(data)
				}
			}
		}
		if errors.Is(err, io.EOF) {
			break // Closed with no exit message; poll below.
		}
		if failures++; ctx.Err() != nil || failures > 3 {
			return 1, fmt.Errorf("receiving output stream: %w", err)
		}
		select {
		case <-ctx.Done():
			return 1, ctx.Err()
		case <-time.After(time.Duration(failures) * 200 * time.Millisecond):
		}
	}

	// The stream closed without reporting an exit; fall back to polling.
	for {
		proc, err := c.process.GetProcess(ctx, &ateenvv1alpha.GetProcessRequest{ProcessId: pid})
		if err != nil {
			return 1, fmt.Errorf("getting process state: %w", err)
		}
		if proc.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING {
			return int(proc.GetExitCode()), nil
		}
		select {
		case <-ctx.Done():
			return 1, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func timeout(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}
	return durationpb.New(d)
}

// stdinRotate is how long one WriteProcessInput stream lives. A proxy on the
// path resets streams after 300 s, and an idle stdin stream would lose the
// next keystroke, so pumpStdin closes each stream cleanly before that.
var stdinRotate = 2 * time.Minute

// pumpStdin copies r to the process stdin and closes the remote stdin on EOF.
// The process exit ends Exec, so pumpStdin only reports errors to errOut.
func (c *Client) pumpStdin(ctx context.Context, pid string, r io.Reader, errOut io.Writer) {
	type chunk struct {
		data []byte
		err  error
	}
	chunks := make(chan chunk)
	go func() {
		for {
			buf := make([]byte, 32*1024)
			n, err := r.Read(buf)
			select {
			case chunks <- chunk{buf[:n], err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	var in ateenvv1alpha.ProcessService_WriteProcessInputClient
	send := func(req *ateenvv1alpha.WriteProcessInputRequest) error {
		if in == nil {
			var err error
			if in, err = c.process.WriteProcessInput(ctx); err != nil {
				return err
			}
			req.ProcessId = pid
		}
		return in.Send(req)
	}
	rotate := time.NewTicker(stdinRotate)
	defer rotate.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-rotate.C:
			if in != nil {
				_, _ = in.CloseAndRecv() // Ends the stream; the remote stdin stays open.
				in = nil
			}
		case ch := <-chunks:
			req := &ateenvv1alpha.WriteProcessInputRequest{Data: ch.data, Close: ch.err != nil}
			if len(ch.data) > 0 || req.Close {
				if err := send(req); err != nil {
					return // The process exited; the output stream reports it.
				}
			}
			if ch.err != nil {
				if !errors.Is(ch.err, io.EOF) && errOut != nil {
					fmt.Fprintf(errOut, "reading stdin: %v\n", ch.err)
				}
				_, _ = in.CloseAndRecv()
				return
			}
		}
	}
}
