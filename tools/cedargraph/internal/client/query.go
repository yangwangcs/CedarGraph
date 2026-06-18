package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GraphClient struct {
	conn   *grpc.ClientConn
	addr   string
}

type QueryResult struct {
	Columns []string
	Rows    [][]string
	Error   string
}

func NewGraphClient(addr string) (*GraphClient, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &GraphClient{conn: conn, addr: addr}, nil
}

func (c *GraphClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *GraphClient) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	// TODO: Wire up actual GraphD gRPC client once query_service.proto is generated.
	// For now, return a placeholder that shows the connection works.
	return &QueryResult{
		Columns: []string{"status"},
		Rows:    [][]string{{"connected to " + c.addr + " — gRPC query not yet wired"}},
	}, nil
}
