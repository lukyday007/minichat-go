// Package websocket은 WebSocketSessionManager와 WebSocketHandler를 Go로 구현합니다.
// Java의 ConcurrentHashMap → sync.RWMutex + map 으로 대체합니다.
package websocket

import (
	"sync"

	"github.com/gorilla/websocket"
)

// Session은 gorilla/websocket 연결을 래핑합니다.
// Java의 WebSocketSession에 해당합니다.
type Session struct {
	conn   *websocket.Conn
	mu     sync.Mutex // 단일 Conn에 대한 동시 쓰기 방지
	closed bool
}

// Send는 텍스트 메시지를 WebSocket으로 안전하게 전송합니다.
func (s *Session) Send(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	return s.conn.WriteMessage(websocket.TextMessage, payload)
}

// Close는 세션을 닫습니다.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.conn.Close()
	}
}

// IsOpen은 세션이 열려있는지 확인합니다.
func (s *Session) IsOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed
}

// NewSession은 gorilla Conn 으로부터 Session을 생성합니다.
func NewSession(conn *websocket.Conn) *Session {
	return &Session{conn: conn}
}

// SessionManager는 userId → Session 매핑을 관리합니다.
// Java의 WebSocketSessionManager(ConcurrentHashMap)에 해당합니다.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[int64]*Session
}

// NewSessionManager는 초기화된 SessionManager를 반환합니다.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[int64]*Session),
	}
}

// Add는 userId에 대한 세션을 등록합니다.
func (m *SessionManager) Add(userID int64, s *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[userID] = s
}

// Get은 userId에 대한 세션을 반환합니다. 없으면 nil.
func (m *SessionManager) Get(userID int64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[userID]
}

// Remove는 userId의 세션 등록을 해제합니다.
func (m *SessionManager) Remove(userID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, userID)
}

// Count는 현재 연결된 세션 수를 반환합니다.
func (m *SessionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}
