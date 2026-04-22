// Package main은 minichat-go의 진입점입니다.
// Java의 MinichatApplication.java에 해당하며,
// 모든 컴포넌트를 수동으로 조립합니다 (Spring DI 대신 명시적 연결).
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	grpcrelay "github.com/dy/minichat-go/internal/grpc"
	kafkachat "github.com/dy/minichat-go/internal/kafka"
	"github.com/dy/minichat-go/internal/snowflake"
	wschat "github.com/dy/minichat-go/internal/websocket"
)

func main() {
	slog.Info("minichat-go 시작")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── 1. Snowflake ID 생성기 ──────────────────────────────────
	sf, err := snowflake.New(1)
	if err != nil {
		slog.Error("Snowflake 초기화 실패", "err", err)
		os.Exit(1)
	}
	slog.Info("Snowflake 준비 완료", "sample_id", sf.NextID())

	// ── 2. Redis 클라이언트 ────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr: getenv("REDIS_ADDR", "localhost:6379"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("Redis 연결 실패 (계속 진행)", "err", err)
	}

	// ── 3. Kafka Producer ──────────────────────────────────────
	brokers := []string{getenv("KAFKA_BROKER", "localhost:9092")}
	producer := kafkachat.NewProducer(brokers)
	defer producer.Close()

	// ── 4. gRPC 클라이언트 ────────────────────────────────────
	grpcClient := grpcrelay.NewRelayClient()
	defer grpcClient.Shutdown()

	// ── 5. WebSocket 세션 매니저 ──────────────────────────────
	sessionMgr := wschat.NewSessionManager()

	// ── 6. WebSocket 핸들러 조립 ──────────────────────────────
	serverID := getenv("SERVER_ID", "server-1")

	wsHandler := wschat.NewHandler(
		sessionMgr,
		rdb,
		serverID,
		// onTalk: Kafka에 메시지 발행
		func(msg wschat.TalkMessage) {
			event := kafkachat.MessageSendEvent{
				Type:      string(msg.Type),
				ChatID:    msg.ChatID,
				SenderID:  msg.SenderID,
				Content:   msg.Content,
				Timestamp: msg.Timestamp,
			}
			if err := producer.Send(ctx, event); err != nil {
				slog.Error("Kafka 발행 실패", "err", err)
			}
		},
		// onRelay: gRPC로 다른 서버에 릴레이 (주소록은 Redis 등에서 조회 가능)
		func(msg wschat.TalkMessage) {
			slog.Info("gRPC 릴레이 필요 (주소록 조회 후 전송)", "chatID", msg.ChatID)
		},
		// onOffline: FCM 푸시 (미구현 — 별도 패키지로 확장)
		func(userID int64, msg wschat.TalkMessage) {
			slog.Info("FCM 전송 필요", "userID", userID, "chatID", msg.ChatID)
		},
	)

	// ── 7. Kafka Consumer 시작 ────────────────────────────────
	// getUsersInChat은 실제로 DB/Redis 조회가 필요합니다
	getUsersInChat := func(chatID int64) []int64 {
		slog.Debug("getUsersInChat 호출 (stub)", "chatID", chatID)
		return []int64{} // TODO: DB/Redis 조회로 교체
	}

	consumer := kafkachat.NewConsumer(brokers, "chat-group", func(event kafkachat.MessageSendEvent) {
		msg := wschat.TalkMessage{
			Type:      wschat.MessageType(event.Type),
			ChatID:    event.ChatID,
			SenderID:  event.SenderID,
			Content:   event.Content,
			Timestamp: event.Timestamp,
		}
		wsHandler.BroadcastMessage(ctx, msg, getUsersInChat)
	})
	defer consumer.Close()
	go consumer.Start(ctx)

	// ── 8. gRPC 서버 시작 ─────────────────────────────────────
	// SessionSender 구현체: SessionManager를 감싸는 어댑터
	grpcServer := grpcrelay.NewRelayServer(&sessionAdapter{mgr: sessionMgr})
	go func() {
		if err := grpcServer.Listen(9091); err != nil {
			slog.Error("gRPC 서버 종료", "err", err)
		}
	}()

	// ── 9. HTTP 서버 (WebSocket 엔드포인트) ───────────────────
	mux := http.NewServeMux()
	// JWT 미들웨어는 별도 패키지로 구현 후 wrap:
	// mux.Handle("/ws", jwtMiddleware(wsHandler))
	mux.Handle("/ws", wsHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"status":"ok","sessions":` + itoa(sessionMgr.Count()) + `}`))
	})

	httpAddr := getenv("HTTP_ADDR", ":8080")
	srv := &http.Server{Addr: httpAddr, Handler: mux}

	go func() {
		slog.Info("HTTP 서버 시작", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("HTTP 서버 오류", "err", err)
		}
	}()

	// ── 10. Graceful Shutdown ─────────────────────────────────
	<-ctx.Done()
	slog.Info("종료 신호 수신, graceful shutdown 시작...")
	srv.Shutdown(context.Background())
	slog.Info("minichat-go 종료 완료")
}

// sessionAdapter는 *wschat.SessionManager 를 grpcrelay.SessionSender 인터페이스에 맞게 변환합니다.
type sessionAdapter struct {
	mgr *wschat.SessionManager
}

func (a *sessionAdapter) SendToUser(userID int64, payload []byte) bool {
	s := a.mgr.Get(userID)
	if s == nil || !s.IsOpen() {
		return false
	}
	return s.Send(payload) == nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
