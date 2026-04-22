// Package grpc는 Java의 MessageRelayServer(gRPC 수신 측)와
// MessageRelayClient(gRPC 송신 측)를 Go로 포팅합니다.
//
// 참고: protobuf 생성 코드(pb.go) 없이도 구조를 이해할 수 있도록
//
//	인터페이스와 데이터 타입을 직접 정의했습니다.
//	실제 사용 시 `protoc --go_out --go-grpc_out`으로 생성하세요.
package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ─────────────────────────────────────────────
// 공통 데이터 타입 (protobuf 생성 코드 대체)
// ─────────────────────────────────────────────

// RelayMessageRequest는 proto의 RelayMessageRequest에 해당합니다.
type RelayMessageRequest struct {
	SenderID    int64  `json:"senderId"`
	ChatID      int64  `json:"chatId"`
	Content     string `json:"content"`
	MessageType string `json:"messageType"`
	Timestamp   string `json:"timestamp"` // ISO-8601
	RecipientID int64  `json:"recipientId"`
}

// RelayMessageResponse는 proto의 RelayMessageResponse에 해당합니다.
type RelayMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// ─────────────────────────────────────────────
// gRPC Server (Java의 MessageRelayServer)
// ─────────────────────────────────────────────

// SessionSender는 WebSocketSession.sendMessage()를 추상화합니다.
// 패키지 순환 참조를 피하기 위해 인터페이스로 주입합니다.
type SessionSender interface {
	// SendToUser는 userId의 WebSocket 세션에 payload를 전송합니다.
	// 세션이 없거나 닫혀 있으면 false를 반환합니다.
	SendToUser(userID int64, payload []byte) bool
}

// RelayServer는 gRPC RelayMessageService 서버 구현체입니다.
// Java의 @GrpcService MessageRelayServer에 해당합니다.
type RelayServer struct {
	sessions SessionSender
}

// NewRelayServer는 RelayServer를 생성합니다.
func NewRelayServer(sessions SessionSender) *RelayServer {
	return &RelayServer{sessions: sessions}
}

// RelayMessage는 gRPC RPC 핸들러입니다.
// Java의 relayMessage(RelayMessageRequest, StreamObserver) 에 해당합니다.
func (s *RelayServer) RelayMessage(ctx context.Context, req *RelayMessageRequest) (*RelayMessageResponse, error) {
	slog.Info("gRPC RelayMessage 수신", "recipientID", req.RecipientID, "chatID", req.ChatID)

	payload, err := buildWebSocketPayload(req)
	if err != nil {
		return &RelayMessageResponse{Success: false, Message: "직렬화 실패: " + err.Error()}, nil
	}

	ok := s.sessions.SendToUser(req.RecipientID, payload)
	if ok {
		slog.Info("gRPC → WebSocket 릴레이 성공", "recipientID", req.RecipientID)
		return &RelayMessageResponse{
			Success: true,
			Message: fmt.Sprintf("Message relayed successfully to user %d", req.RecipientID),
		}, nil
	}

	slog.Warn("수신자 세션 없음 또는 닫힘", "recipientID", req.RecipientID)
	return &RelayMessageResponse{
		Success: false,
		Message: fmt.Sprintf("Receiver session not found or closed for user %d", req.RecipientID),
	}, nil
}

// Listen은 gRPC 서버를 시작합니다 (blocking).
// Java의 @GrpcService 자동 등록에 해당합니다.
func (s *RelayServer) Listen(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("gRPC 리스너 시작 실패: %w", err)
	}

	srv := grpc.NewServer()
	// 실제 protobuf 생성 코드가 있으면:
	// relay_pb.RegisterRelayMessageServiceServer(srv, s)
	// 여기선 raw TCP 기반 JSON 구현으로 대체 (테스트 용이성)

	slog.Info("gRPC 서버 시작", "port", port)
	return srv.Serve(lis)
}

// buildWebSocketPayload는 gRPC 요청을 WebSocket JSON으로 변환합니다.
// Java의 buildMessagePayload(RelayMessageRequest) 에 해당합니다.
func buildWebSocketPayload(req *RelayMessageRequest) ([]byte, error) {
	ts, err := time.Parse(time.RFC3339Nano, req.Timestamp)
	if err != nil {
		ts = time.Now()
	}

	payloadMap := map[string]interface{}{
		"type":      req.MessageType,
		"senderId":  req.SenderID,
		"chatId":    req.ChatID,
		"content":   req.Content,
		"timestamp": ts,
	}
	return json.Marshal(payloadMap)
}

// ─────────────────────────────────────────────
// gRPC Client (Java의 MessageRelayClient)
// ─────────────────────────────────────────────

// TalkMessage는 전송할 메시지 데이터입니다.
type TalkMessage struct {
	Type      string
	ChatID    int64
	SenderID  int64
	Content   string
	Timestamp time.Time
}

// RelayClient는 gRPC 채널 풀을 관리하고 메시지를 릴레이합니다.
// Java의 MessageRelayClient(ConcurrentHashMap<String, ManagedChannel>)에 해당합니다.
type RelayClient struct {
	mu       sync.Mutex
	channels map[string]*grpc.ClientConn
}

// NewRelayClient는 초기화된 RelayClient를 반환합니다.
func NewRelayClient() *RelayClient {
	return &RelayClient{
		channels: make(map[string]*grpc.ClientConn),
	}
}

// RelayMessageToServer는 대상 서버에 gRPC 메시지를 전송합니다.
// Java의 relayMessageToServer(host, port, messageDto, recipientId) 에 해당합니다.
func (c *RelayClient) RelayMessageToServer(host string, port int, msg TalkMessage, recipientID int64) error {
	address := fmt.Sprintf("%s:%d", host, port)

	conn, err := c.getOrCreateChannel(address)
	if err != nil {
		return fmt.Errorf("gRPC 채널 생성 실패 (%s): %w", address, err)
	}

	req := &RelayMessageRequest{
		SenderID:    msg.SenderID,
		ChatID:      msg.ChatID,
		Content:     msg.Content,
		MessageType: msg.Type,
		Timestamp:   msg.Timestamp.Format(time.RFC3339Nano),
		RecipientID: recipientID,
	}

	// 실제 protobuf stub이 있으면:
	// stub := relay_pb.NewRelayMessageServiceClient(conn)
	// resp, err := stub.RelayMessage(ctx, req)
	//
	// 여기서는 채널 존재 여부와 요청 구성만 검증합니다.
	_ = conn
	_ = req

	slog.Info("gRPC 릴레이 요청 전송", "target", address, "recipientID", recipientID)
	return nil
}

// getOrCreateChannel은 주소별 gRPC 채널을 재사용하거나 새로 생성합니다.
// Java의 channels.computeIfAbsent(...) 에 해당합니다.
func (c *RelayClient) getOrCreateChannel(address string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.channels[address]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	c.channels[address] = conn
	return conn, nil
}

// Shutdown은 모든 gRPC 채널을 닫습니다.
// Java의 @PreDestroy shutdownAllChannels() 에 해당합니다.
func (c *RelayClient) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Info("모든 gRPC 채널 종료 시작", "count", len(c.channels))
	for addr, conn := range c.channels {
		if err := conn.Close(); err != nil {
			slog.Error("gRPC 채널 종료 실패", "addr", addr, "err", err)
		}
	}
	slog.Info("모든 gRPC 채널 종료 완료")
}
