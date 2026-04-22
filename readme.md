# minichat-go

Java Spring Boot `minichat` 프로젝트의 핵심 기능을 **Go**로 포팅한 구현입니다.
JWT 인증을 제외한 4개 모듈을 담고 있습니다.

---

## 포팅된 모듈

| Java 원본 | Go 구현 | 패키지 |
|---|---|---|
| `Snowflake.java` | `snowflake.go` | `internal/snowflake` |
| `WebSocketHandler.java` + `WebSocketSessionManager.java` | `handler.go` + `session.go` | `internal/websocket` |
| `ChatMessageProducer.java` + `ChatMessageConsumer.java` | `chat_message.go` | `internal/kafka` |
| `MessageRelayServer.java` + `MessageRelayClient.java` | `relay.go` | `internal/grpc` |

---

## Java → Go 주요 변환 포인트

### Snowflake
| Java | Go |
|---|---|
| `synchronized` 메서드 | `sync.Mutex` |
| `volatile long` | `int64` (Lock 내부라서 volatile 불필요) |

### WebSocket
| Java | Go |
|---|---|
| `ConcurrentHashMap<Long, WebSocketSession>` | `sync.RWMutex` + `map[int64]*Session` |
| `parallelStream().forEach(...)` | `for _, id := range ... { go deliver(...) }` |
| `TextWebSocketHandler` 상속 | `http.Handler` 구현 |
| Spring DI (`@Autowired`) | 생성자에 함수/인터페이스 주입 |

### Kafka
| Java | Go |
|---|---|
| `KafkaTemplate<String, MessageSendEvent>` | `kafkago.Writer` (segmentio/kafka-go) |
| `@KafkaListener(topics=..., groupId=...)` | `kafkago.NewReader(ReaderConfig{...})` |
| `ObjectMapper` 직렬화 | `encoding/json` |

### gRPC
| Java | Go |
|---|---|
| `@GrpcService` | `grpc.RegisterXxx` |
| `ManagedChannelBuilder.forAddress(...)` | `grpc.NewClient(address, ...)` |
| `ConcurrentHashMap<String, ManagedChannel>` | `sync.Mutex` + `map[string]*grpc.ClientConn` |
| `@PreDestroy shutdownAllChannels()` | `defer client.Shutdown()` |

---

## 실행

```bash
# 의존성 설치
go mod tidy

# 테스트
go test ./internal/snowflake/... -v

# 빌드
go build -o minichat-go ./cmd

# 실행 (환경변수 설정)
SERVER_ID=server-1 \
REDIS_ADDR=localhost:6379 \
KAFKA_BROKER=localhost:9092 \
./minichat-go
```

## 환경변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `SERVER_ID` | `server-1` | 이 인스턴스의 고유 식별자 (Redis 서버 위치 추적용) |
| `REDIS_ADDR` | `localhost:6379` | Redis 주소 |
| `KAFKA_BROKER` | `localhost:9092` | Kafka 브로커 주소 |
| `HTTP_ADDR` | `:8080` | WebSocket + HTTP 헬스체크 바인딩 주소 |

## 엔드포인트

- `GET /ws` — WebSocket 연결 (JWT 미들웨어 wrap 필요)
- `GET /health` — 헬스체크 (현재 세션 수 포함)