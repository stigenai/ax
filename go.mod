module github.com/google/ax

go 1.27.1

require (
	github.com/agent-substrate/env v0.0.11-0.20260912052224-4468a200b170
	github.com/agent-substrate/substrate v0.0.0-20260911232748-672533541dbf
	github.com/redis/go-redis/v9 v9.22.0
	golang.org/x/net v0.58.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/apimachinery v0.37.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
)

replace github.com/agent-substrate/substrate => github.com/stigenai/substrate v0.3.1-0.20261014142909-6fbe6ab2aeed
