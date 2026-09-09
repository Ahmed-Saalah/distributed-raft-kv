package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Ahmed-Saalah/distributed-raft-kv/internal/kvstore"
	"github.com/Ahmed-Saalah/distributed-raft-kv/internal/raft"
	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
	"github.com/Ahmed-Saalah/distributed-raft-kv/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	var rootCmd = &cobra.Command{
		Use:   "kvserver",
		Short: "Starts the distributed Raft KV server",
		Run:   runServer,
	}

	rootCmd.Flags().Int("id", 0, "Unique ID for this node")
	rootCmd.Flags().Int("port", 5000, "Port to listen on")
	rootCmd.Flags().String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:5001,localhost:5002)")
	rootCmd.Flags().Int("max-raft-state", 10000, "Max log size in bytes before snapshotting (-1 to disable)")

	viper.BindPFlag("id", rootCmd.Flags().Lookup("id"))
	viper.BindPFlag("port", rootCmd.Flags().Lookup("port"))
	viper.BindPFlag("peers", rootCmd.Flags().Lookup("peers"))
	viper.BindPFlag("max-raft-state", rootCmd.Flags().Lookup("max-raft-state"))

	viper.SetEnvPrefix("RAFT")
	viper.AutomaticEnv()

	if err := rootCmd.Execute(); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) {
	nodeID := viper.GetInt("id")
	port := viper.GetInt("port")
	peersFlag := viper.GetString("peers")

	dataDir := filepath.Join(".", "data", fmt.Sprintf("node-%d", nodeID))
	fs, err := storage.NewFileStorage(dataDir)
	if err != nil {
		slog.Error("Failed to initialize storage", "error", err)
		os.Exit(1)
	}
	slog.Info("Storage initialized", "nodeID", nodeID, "dataDir", dataDir)

	peerAddrs := strings.Split(peersFlag, ",")
	var raftPeers []pb.RaftClient

	for i, addr := range peerAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" || i == nodeID {
			raftPeers = append(raftPeers, nil)
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("Failed to connect to peer", "peer", addr, "error", err)
			os.Exit(1)
		}
		raftPeers = append(raftPeers, pb.NewRaftClient(conn))
		slog.Info("Established connection to peer", "nodeID", nodeID, "peerID", i, "addr", addr)
	}

	applyCh := make(chan raft.ApplyMsg)
	raftNode := raft.NewRaftNode(raftPeers, nodeID, fs, applyCh)
	
	maxRaftState := viper.GetInt("max-raft-state")
	kvServer := kvstore.NewKVServer(nodeID, raftNode, applyCh, maxRaftState)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		slog.Error("Failed to listen", "port", port, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, raftNode)
	pb.RegisterKVStoreServer(grpcServer, kvServer)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("Node is fully operational", "nodeID", nodeID, "port", port)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server crashed", "error", err)
		}
	}()

	<-stopChan
	slog.Info("Shutting down gracefully...")
	grpcServer.GracefulStop()
	slog.Info("Server stopped")
}
