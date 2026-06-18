package client

import (
	"context"
	"fmt"
	"time"

	pb "github.com/cedar-graph/cedargraph-cli/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GraphClient struct {
	conn   *grpc.ClientConn
	client pb.QueryServiceClient
	addr   string
}

type QueryResult struct {
	Columns []string
	Rows    [][]string
	Error   string
	Stats   *ExecutionStats
}

type ExecutionStats struct {
	ExecutionTimeUs  uint64
	RowsScanned      uint64
	RowsReturned     uint64
	StorageNodesUsed uint32
}

type ConnectOptions struct {
	Timeout     time.Duration
	MaxRetries  int
	RetryDelay  time.Duration
}

func DefaultConnectOptions() ConnectOptions {
	return ConnectOptions{
		Timeout:    5 * time.Second,
		MaxRetries: 3,
		RetryDelay: 1 * time.Second,
	}
}

func NewGraphClient(addr string) (*GraphClient, error) {
	return NewGraphClientWithOptions(addr, DefaultConnectOptions())
}

func NewGraphClientWithOptions(addr string, opts ConnectOptions) (*GraphClient, error) {
	var lastErr error

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(opts.RetryDelay)
		}

		ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err == nil {
			return &GraphClient{
				conn:   conn,
				client: pb.NewQueryServiceClient(conn),
				addr:   addr,
			}, nil
		}

		lastErr = err
	}

	return nil, fmt.Errorf("connect to %s after %d retries: %w", addr, opts.MaxRetries, lastErr)
}

func (c *GraphClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *GraphClient) Addr() string {
	return c.addr
}

func (c *GraphClient) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	resp, err := c.client.ExecuteQuery(ctx, &pb.ExecuteQueryRequest{
		Query: query,
	})
	if err != nil {
		return nil, fmt.Errorf("rpc: %w", err)
	}

	result := &QueryResult{}

	if !resp.Success {
		result.Error = resp.GetErrorMsg()
		return result, nil
	}

	rs := resp.GetResultSet()
	if rs == nil {
		return result, nil
	}

	result.Columns = rs.Columns

	for _, row := range rs.GetRows() {
		var cells []string
		for _, val := range row.GetValues() {
			cells = append(cells, formatValue(val))
		}
		result.Rows = append(result.Rows, cells)
	}

	if stats := resp.GetStats(); stats != nil {
		result.Stats = &ExecutionStats{
			ExecutionTimeUs:  stats.ExecutionTimeUs,
			RowsScanned:      stats.RowsScanned,
			RowsReturned:     stats.RowsReturned,
			StorageNodesUsed: stats.StorageNodesAccessed,
		}
	}

	return result, nil
}

func formatValue(v *pb.Value) string {
	switch val := v.GetValueType().(type) {
	case *pb.Value_BoolVal:
		if val.BoolVal {
			return "true"
		}
		return "false"
	case *pb.Value_IntVal:
		return fmt.Sprintf("%d", val.IntVal)
	case *pb.Value_FloatVal:
		return fmt.Sprintf("%g", val.FloatVal)
	case *pb.Value_StringVal:
		return val.StringVal
	case *pb.Value_BytesVal:
		return fmt.Sprintf("<bytes:%d>", len(val.BytesVal))
	case *pb.Value_NullVal:
		return "NULL"
	case *pb.Value_ListVal:
		return "[...]"
	case *pb.Value_MapVal:
		return "{...}"
	default:
		return "?"
	}
}
