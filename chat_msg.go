// Package kafka는 Java의 ChatMessageProducer / ChatMessageConsumer를 Go로 포팅합니다.
// segmentio/kafka-go 라이브러리를 사용합니다.
package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	TopicChatMessage = "chat-message" // Java와 동일한 토픽명
)

// MessageSendEvent는 Java의 MessageSendEvent에 해당합니다.
type MessageSendEvent struct {
	Type      string    `json:"type"`
	ChatID    int64     `json:"chatId"`
	SenderID  int64     `json:"senderId"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// ─────────────────────────────────────────────
// ChatMessageProducer (Java의 ChatMessageProducer)
// ─────────────────────────────────────────────

// Producer는 Kafka에 채팅 메시지 이벤트를 발행합니다.
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer는 지정된 브로커로 Producer를 생성합니다.
// Java의 KafkaTemplate<String, MessageSendEvent>에 해당합니다.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        TopicChatMessage,
			Balancer:     &kafkago.Hash{}, // Key 기반 파티셔닝 (chatId → 순서 보장)
			RequiredAcks: kafkago.RequireOne,
			Async:        false, // 안정성 우선 (Java 기본값과 동일)
		},
	}
}

// Send는 MessageSendEvent를 Kafka에 발행합니다.
// chatId를 메시지 Key로 사용해 같은 채팅방의 메시지 순서를 보장합니다.
func (p *Producer) Send(ctx context.Context, event MessageSendEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("[Kafka] 직렬화 실패", "content", event.Content, "err", err)
		return err
	}

	// Java: String key = String.valueOf(event.getTalkMessage().getChatId());
	key := i64bytes(event.ChatID)

	err = p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   key,
		Value: payload,
	})
	if err != nil {
		slog.Error("[Kafka] 프로듀서 전송 실패", "content", event.Content, "err", err)
		return err
	}

	slog.Info("[Kafka] 메시지 발행 완료", "chatID", event.ChatID)
	return nil
}

// Close는 Producer를 닫습니다.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// ─────────────────────────────────────────────
// ChatMessageConsumer (Java의 ChatMessageConsumer)
// ─────────────────────────────────────────────

// ConsumerHandler는 소비된 메시지를 처리하는 함수 타입입니다.
// Java의 webSocketHandler.broadcastMessage(message)에 해당합니다.
type ConsumerHandler func(event MessageSendEvent)

// Consumer는 Kafka에서 채팅 메시지를 구독합니다.
type Consumer struct {
	reader  *kafkago.Reader
	handler ConsumerHandler
}

// NewConsumer는 Consumer를 생성합니다.
// Java의 @KafkaListener(topics = "chat-message", groupId = "chat-group")에 해당합니다.
func NewConsumer(brokers []string, groupID string, handler ConsumerHandler) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:  brokers,
			Topic:    TopicChatMessage,
			GroupID:  groupID, // "chat-group"
			MinBytes: 10e3,    // 10KB
			MaxBytes: 10e6,    // 10MB
			MaxWait:  500 * time.Millisecond,
		}),
		handler: handler,
	}
}

// Start는 메시지를 소비하는 블로킹 루프를 시작합니다.
// ctx를 통해 graceful shutdown을 지원합니다.
// 별도 고루틴에서 호출하세요: go consumer.Start(ctx)
func (c *Consumer) Start(ctx context.Context) {
	slog.Info("[Kafka] Consumer 시작", "topic", TopicChatMessage)

	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("[Kafka] Consumer 정상 종료")
				return
			}
			slog.Error("[Kafka] 메시지 읽기 실패", "err", err)
			continue
		}

		var event MessageSendEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			slog.Warn("[Kafka] 역직렬화 실패", "err", err)
			continue
		}

		// Java: webSocketHandler.broadcastMessage(message)
		c.handler(event)
	}
}

// Close는 Consumer를 닫습니다.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

func i64bytes(n int64) []byte {
	return []byte(func() string {
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
	}())
}
