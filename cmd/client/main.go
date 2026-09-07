package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var endpoints string

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	var rootCmd = &cobra.Command{
		Use:   "client",
		Short: "CLI client for distributed Raft KV store",
	}

	rootCmd.PersistentFlags().StringVar(&endpoints, "endpoints", "localhost:5000,localhost:5001,localhost:5002", "Comma-separated list of server endpoints")

	var putCmd = &cobra.Command{
		Use:   "put [key] [value]",
		Short: "Put a key-value pair into the store",
		Args:  cobra.ExactArgs(2),
		Run:   runPut,
	}

	var getCmd = &cobra.Command{
		Use:   "get [key]",
		Short: "Get a value by key from the store",
		Args:  cobra.ExactArgs(1),
		Run:   runGet,
	}

	rootCmd.AddCommand(putCmd, getCmd)

	if err := rootCmd.Execute(); err != nil {
		slog.Error("Command failed", "error", err)
		os.Exit(1)
	}
}

func getClientConn(endpoint string) (*grpc.ClientConn, pb.KVStoreClient, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewKVStoreClient(conn), nil
}

func runPut(cmd *cobra.Command, args []string) {
	key := args[0]
	val := args[1]
	serverAddrs := strings.Split(endpoints, ",")

	for _, addr := range serverAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		conn, client, err := getClientConn(addr)
		if err != nil {
			slog.Warn("Failed to connect", "endpoint", addr, "error", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req := &pb.PutArgs{
			Key:     key,
			Value:   val,
			Version: 0,
		}

		res, err := client.Put(ctx, req)
		cancel()
		conn.Close()

		if err != nil {
			slog.Warn("Network error", "endpoint", addr, "error", err)
			continue
		}

		if res.Err == "ErrWrongLeader" {
			slog.Info("Not the leader, trying next endpoint...", "endpoint", addr)
			continue
		}

		if res.Err == "OK" {
			slog.Info("Successfully put key-value pair", "key", key, "value", val)
			return
		} else {
			slog.Error("Server returned error", "error", res.Err)
			os.Exit(1)
		}
	}

	slog.Error("Failed to put: could not find the leader or all endpoints failed")
	os.Exit(1)
}

func runGet(cmd *cobra.Command, args []string) {
	key := args[0]
	serverAddrs := strings.Split(endpoints, ",")

	for _, addr := range serverAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		conn, client, err := getClientConn(addr)
		if err != nil {
			slog.Warn("Failed to connect", "endpoint", addr, "error", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req := &pb.GetArgs{Key: key}

		res, err := client.Get(ctx, req)
		cancel()
		conn.Close()

		if err != nil {
			slog.Warn("Network error", "endpoint", addr, "error", err)
			continue
		}

		if res.Err == "ErrWrongLeader" {
			slog.Info("Not the leader, trying next endpoint...", "endpoint", addr)
			continue
		}

		if res.Err == "OK" {
			fmt.Printf("SUCCESS! Found value: '%s' (Version: %d)\n", res.Value, res.Version)
			return
		} else if res.Err == "ErrNoKey" {
			slog.Error("Key not found", "key", key)
			os.Exit(1)
		} else {
			slog.Error("Server returned error", "error", res.Err)
			os.Exit(1)
		}
	}

	slog.Error("Failed to get: could not find the leader or all endpoints failed")
	os.Exit(1)
}
