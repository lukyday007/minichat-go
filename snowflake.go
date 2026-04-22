// Package snowflake는 Java 버전의 Snowflake ID 생성기를 Go로 포팅한 구현입니다.
//
// 구조: [41비트 타임스탬프] + [10비트 nodeId] + [12비트 시퀀스]
// 커스텀 에포크: 2025-01-01 00:00:00 UTC (Java와 동일)
package snowflake

import (
	"fmt"
	"sync"
	"time"
)

const (
	nodeIDBits    = 10
	sequenceBits  = 12
	customEpochMs = int64(1735689600000) // 2025-01-01 00:00:00 UTC (Java와 동일)

	maxSequence = int64(1)<<sequenceBits - 1 // 4095
)

// Snowflake는 분산 고유 ID 생성기입니다.
// Java 원본과 동일하게 synchronized → sync.Mutex 로 대체해 스레드(고루틴) 안전을 보장합니다.
type Snowflake struct {
	mu            sync.Mutex
	nodeID        int64
	lastTimestamp int64
	sequence      int64
}

// New는 지정된 nodeID로 Snowflake 생성기를 초기화합니다.
func New(nodeID int64) (*Snowflake, error) {
	maxNodeID := int64(1)<<nodeIDBits - 1 // 1023
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("nodeID는 0 ~ %d 범위여야 합니다 (입력: %d)", maxNodeID, nodeID)
	}
	return &Snowflake{
		nodeID:        nodeID,
		lastTimestamp: -1,
		sequence:      0,
	}, nil
}

// NextID는 유니크한 int64 ID를 생성합니다.
// Java의 synchronized nextId()와 동일한 로직입니다.
func (s *Snowflake) NextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentTimestamp := time.Now().UnixMilli() - customEpochMs

	if currentTimestamp == s.lastTimestamp {
		s.sequence++
		// 시퀀스 오버플로우: 다음 밀리초까지 대기
		if s.sequence > maxSequence {
			for currentTimestamp <= s.lastTimestamp {
				currentTimestamp = time.Now().UnixMilli() - customEpochMs
			}
			s.sequence = 0
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = currentTimestamp

	return currentTimestamp<<(nodeIDBits+sequenceBits) |
		(s.nodeID << sequenceBits) |
		s.sequence
}

// Parse는 Snowflake ID를 [timestamp(ms), nodeID, sequence] 로 분해합니다.
// Java의 parse() 메서드와 동일합니다.
func Parse(id int64) (timestampMs int64, nodeID int64, sequence int64) {
	maskNodeID := ((int64(1) << nodeIDBits) - 1) << sequenceBits
	maskSeq := (int64(1) << sequenceBits) - 1

	timestampMs = (id >> (nodeIDBits + sequenceBits)) + customEpochMs
	nodeID = (id & maskNodeID) >> sequenceBits
	sequence = id & maskSeq
	return
}
