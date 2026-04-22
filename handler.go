package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// MessageType은 TalkMessageDTO.MessageType 에 대응합니다.
type MessageType string

const (
	MessageTypeTalk MessageType = "TALK"
)

// TalkMessage는 Java의 TalkMessageDTO에 해당합니다.
type TalkMessage struct {
	Type      MessageType `json:"type"`
	ChatID    int64       `json:"chatId"`
	SenderID  int64       `json:"senderId"`
	Content   string      `json:"content"`
	Timestamp time.Time   `json:"timestamp"`
}

// BroadcastFunc는 메시지를 채팅방 전체에 방송하는 함수 타입입니다.
// 실제 구현체(Kafka consumer 등)를 주입 받아 사용합니다.
type BroadcastFunc func(msg TalkMessage)

// Handler는 WebSocket 연결을 처리합니다.
// Java의 WebSocketHandler에 해당하며, 의존성 주입 방식으로 구성합니다.
type Handler struct {
	upgrader  websocket.Upgrader
	sessions  *SessionManager
	redis     *redis.Client
	serverID  string
	onTalk    BroadcastFunc                       // Kafka producer 로 연결
	onRelay   BroadcastFunc                       // gRPC relay 로 연결 (다른 서버 수신자용)
	onOffline func(userID int64, msg TalkMessage) // FCM 등 오프라인 처리
}

const userServerKeyPrefix = "ws:user:server:"

// NewHandler는 Handler를 생성합니다.
func NewHandler(
	sessions *SessionManager,
	rdb *redis.Client,
	serverID string,
	onTalk BroadcastFunc,
	onRelay BroadcastFunc,
	onOffline func(userID int64, msg TalkMessage),
) *Handler {
	return &Handler{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		sessions:  sessions,
		redis:     rdb,
		serverID:  serverID,
		onTalk:    onTalk,
		onRelay:   onRelay,
		onOffline: onOffline,
	}
}

// ServeHTTP는 HTTP → WebSocket 업그레이드 후 연결을 처리합니다.
// Java의 afterConnectionEstablished + handleTextMessage + afterConnectionClosed 를 통합합니다.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. JWT 등으로 검증된 userId를 컨텍스트에서 꺼냅니다 (미들웨어에서 주입)
	userID, ok := UserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "userId 없음: 인증 미들웨어 확인 필요", http.StatusUnauthorized)
		return
	}

	// 2. WebSocket 업그레이드
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket 업그레이드 실패", "err", err)
		return
	}

	session := NewSession(conn)
	ctx := context.Background()

	// 3. afterConnectionEstablished 로직
	h.onConnected(ctx, userID, session)
	defer h.onDisconnected(ctx, userID)

	// 4. 메시지 수신 루프 (Java의 handleTextMessage)
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Warn("WebSocket 읽기 오류", "userID", userID, "err", err)
			}
			break
		}
		h.handleMessage(ctx, userID, payload)
	}
}

// onConnected는 Java의 afterConnectionEstablished에 해당합니다.
func (h *Handler) onConnected(ctx context.Context, userID int64, session *Session) {
	userKey := userKey(userID)

	chatIDStr, err := h.redis.HGet(ctx, userKey, "chatId").Result()
	if err != nil || chatIDStr == "" {
		slog.Warn("Redis에 chatId 없음 — API 미호출 가능성", "userID", userID)
		return
	}

	h.sessions.Add(userID, session)
	h.redis.HSet(ctx, userKey, "serverId", h.serverID, "lastActive", time.Now().String())
	h.redis.Set(ctx, userServerKeyPrefix+i64str(userID), h.serverID, 0)

	slog.Info("WebSocket 연결", "userID", userID, "chatID", chatIDStr, "server", h.serverID)
}

// handleMessage는 Java의 handleTextMessage에 해당합니다.
func (h *Handler) handleMessage(ctx context.Context, senderID int64, payload []byte) {
	var msg TalkMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		slog.Warn("메시지 파싱 실패", "senderID", senderID, "err", err)
		return
	}

	// Redis에서 현재 chatId 확인
	currentChatID := h.getChatIDFromRedis(ctx, senderID)
	if currentChatID == 0 || currentChatID != msg.ChatID {
		slog.Warn("chatId 불일치 또는 미입장 상태", "senderID", senderID, "redisChatID", currentChatID, "msgChatID", msg.ChatID)
		return
	}

	msg.SenderID = senderID
	msg.Timestamp = time.Now()

	switch msg.Type {
	case MessageTypeTalk:
		// Kafka로 이벤트 발행 (Java와 동일한 흐름)
		h.onTalk(msg)
		slog.Info("메시지 Kafka 전송", "senderID", senderID, "chatID", msg.ChatID)
	default:
		slog.Warn("처리 불가 메시지 타입", "type", msg.Type)
	}
}

// BroadcastMessage는 Java의 broadcastMessage(Kafka Consumer 호출용 public 메서드)에 해당합니다.
// Kafka consumer가 이 메서드를 호출해 채팅방 전체에 방송합니다.
func (h *Handler) BroadcastMessage(ctx context.Context, msg TalkMessage, getUsersInChat func(chatID int64) []int64) {
	userIDs := getUsersInChat(msg.ChatID)
	if len(userIDs) == 0 {
		slog.Warn("전송할 사용자 없음", "chatID", msg.ChatID)
		return
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		slog.Error("메시지 직렬화 실패", "chatID", msg.ChatID, "err", err)
		return
	}

	// goroutine으로 각 유저에게 전송 (Java의 parallelStream에 해당)
	for _, userID := range userIDs {
		go h.deliverToUser(ctx, userID, payload, msg)
	}
}

// deliverToUser는 수신자별 전달 전략을 결정합니다.
// Java의 sendMessageToChatRoom 내부 로직에 해당합니다.
func (h *Handler) deliverToUser(ctx context.Context, userID int64, payload []byte, msg TalkMessage) {
	session := h.sessions.Get(userID)

	// Case 1: 같은 서버에 WebSocket 연결 있음
	if session != nil && session.IsOpen() {
		if err := session.Send(payload); err != nil {
			slog.Error("로컬 WebSocket 전송 실패", "userID", userID, "err", err)
		} else {
			slog.Info("로컬 전송 성공", "userID", userID)
		}
		return
	}

	// Redis에서 대상 서버 확인
	targetServerID, _ := h.redis.Get(ctx, userServerKeyPrefix+i64str(userID)).Result()

	// Case 2-1: 다른 서버에 있음 → gRPC 릴레이
	if targetServerID != "" && targetServerID != h.serverID {
		slog.Info("gRPC 릴레이", "userID", userID, "targetServer", targetServerID)
		h.onRelay(msg)
		return
	}

	// Case 2-2: 오프라인 → FCM
	slog.Info("오프라인 → FCM 전송", "userID", userID)
	h.onOffline(userID, msg)
}

// onDisconnected는 Java의 afterConnectionClosed에 해당합니다.
func (h *Handler) onDisconnected(ctx context.Context, userID int64) {
	h.sessions.Remove(userID)

	userKey := userKey(userID)
	chatIDStr, _ := h.redis.HGet(ctx, userKey, "chatId").Result()
	if chatIDStr != "" {
		h.redis.SRem(ctx, "chatId:"+chatIDStr+":userId", i64str(userID))
	}

	h.redis.Del(ctx, userServerKeyPrefix+i64str(userID))
	h.redis.HDel(ctx, userKey, "serverId", "lastActive")

	slog.Info("WebSocket 연결 종료 및 Redis 정리", "userID", userID, "server", h.serverID)
}

func (h *Handler) getChatIDFromRedis(ctx context.Context, userID int64) int64 {
	val, err := h.redis.HGet(ctx, userKey(userID), "chatId").Result()
	if err != nil || val == "" {
		return 0
	}
	var id int64
	for _, c := range val {
		id = id*10 + int64(c-'0')
	}
	return id
}

// contextKey는 컨텍스트 키 타입입니다.
type contextKey string

const userIDKey contextKey = "userID"

// WithUserID는 userID를 컨텍스트에 주입합니다 (미들웨어에서 사용).
func WithUserID(ctx context.Context, userID int64) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// UserIDFromContext는 컨텍스트에서 userID를 꺼냅니다.
func UserIDFromContext(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(userIDKey).(int64)
	return id, ok
}

func userKey(userID int64) string { return "userId:" + i64str(userID) + ":state" }

func i64str(n int64) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
