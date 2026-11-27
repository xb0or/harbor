// Package key 管理 Harbor Key：鉴权、模型白名单、状态。
// Key 是纯访问令牌：不含任何限流额度（并发/RPM/TPM/TPH/TPD 属于上游 Provider）。
package key

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
)

// 鉴权哨兵错误（server 层映射为收敛错误）。
var (
	ErrMissing = errors.New("missing bearer token")
	ErrUnknown = errors.New("unknown key")
	ErrBanned  = errors.New("key banned")
	ErrExpired = errors.New("key expired")
)

// Status 是 Harbor Key 的状态。
type Status string

const (
	StatusActive  Status = "active"
	StatusBanned  Status = "banned"
	StatusExpired Status = "expired"
)

// Key 是一个 Harbor Key 的运行时表示（纯访问令牌）。
type Key struct {
	ID            string
	Prefix        string // 展示用前缀，如 hr-live
	Hash          string // sha256(secret)，Key 本身只保存哈希（计划书 §11）
	Status        Status
	AllowedModels []string
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

// ModelAllowed 判断 key 是否允许该模型。allowed_models 为空 = 全部允许。
func (k *Key) ModelAllowed(model string) bool {
	if len(k.AllowedModels) == 0 {
		return true
	}
	for _, m := range k.AllowedModels {
		if m == model {
			return true
		}
	}
	return false
}

// Hash 计算 Harbor Key 的 sha256 哈希。
func Hash(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

// Prefix 从 secret 提取展示前缀（前 12 字符）。
func Prefix(secret string) string {
	if len(secret) <= 12 {
		return secret
	}
	return secret[:12]
}

// Store 是内存 Key 存储。
type Store struct {
	mu   sync.RWMutex
	keys map[string]*Key // key = hash
}

// NewStore 构造空 Store。
func NewStore() *Store {
	return &Store{keys: map[string]*Key{}}
}

// clone 返回不可被外部修改的 Key 快照。
func clone(k *Key) *Key {
	if k == nil {
		return nil
	}
	copy := *k
	copy.AllowedModels = append([]string(nil), k.AllowedModels...)
	return &copy
}

// Add 添加一个 Key（配置导入或管理 API 创建）。
func (s *Store) Add(secret string, allowedModels []string) *Key {
	k := &Key{
		ID:            Prefix(secret) + "-" + Hash(secret)[:8],
		Prefix:        Prefix(secret),
		Hash:          Hash(secret),
		Status:        StatusActive,
		AllowedModels: append([]string(nil), allowedModels...),
		CreatedAt:     time.Now(),
	}
	s.mu.Lock()
	s.keys[k.Hash] = k
	s.mu.Unlock()
	return clone(k)
}

// Restore 从持久化密文恢复一个 Key，并保留其 ID、状态和时间元数据。
// secret 只在启动恢复期间短暂存在，运行时 Key 仍只保存哈希。
func (s *Store) Restore(secret, id, prefix string, status Status, allowedModels []string, createdAt, expiresAt time.Time) *Key {
	if secret == "" {
		return nil
	}
	hash := Hash(secret)
	if id == "" {
		id = Prefix(secret) + "-" + hash[:8]
	}
	if prefix == "" {
		prefix = Prefix(secret)
	}
	if status == "" {
		status = StatusActive
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	k := &Key{
		ID:            id,
		Prefix:        prefix,
		Hash:          hash,
		Status:        status,
		AllowedModels: append([]string(nil), allowedModels...),
		CreatedAt:     createdAt,
		ExpiresAt:     expiresAt,
	}
	s.mu.Lock()
	s.keys[hash] = k
	s.mu.Unlock()
	return clone(k)
}

// RestoreMetadata 将仅有哈希的旧版/配置 Key 的持久化元数据恢复到已有 Key。
func (s *Store) RestoreMetadata(hash, id, prefix string, status Status, allowedModels []string, createdAt, expiresAt time.Time) bool {
	if hash == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[hash]
	if !ok {
		return false
	}
	if id != "" {
		k.ID = id
	}
	if prefix != "" {
		k.Prefix = prefix
	}
	if status != "" {
		k.Status = status
	}
	k.AllowedModels = append([]string(nil), allowedModels...)
	if !createdAt.IsZero() {
		k.CreatedAt = createdAt
	}
	k.ExpiresAt = expiresAt
	return true
}

// Authenticate 校验 Bearer secret，返回对应 Key.
// 返回的错误为哨兵错误（ErrMissing/ErrUnknown/ErrBanned/ErrExpired）。
func (s *Store) Authenticate(bearer string) (*Key, error) {
	token := strings.TrimSpace(bearer)
	if len(token) >= len("Bearer ") && strings.EqualFold(token[:len("Bearer ")], "Bearer ") {
		token = strings.TrimSpace(token[len("Bearer "):])
	}
	if token == "" {
		return nil, ErrMissing
	}
	hash := Hash(token)
	s.mu.RLock()
	stored, ok := s.keys[hash]
	if !ok {
		s.mu.RUnlock()
		return nil, ErrUnknown
	}
	if stored.Status == StatusBanned {
		s.mu.RUnlock()
		return nil, ErrBanned
	}
	if stored.Status == StatusExpired || (!stored.ExpiresAt.IsZero() && time.Now().After(stored.ExpiresAt)) {
		s.mu.RUnlock()
		return nil, ErrExpired
	}
	k := clone(stored)
	s.mu.RUnlock()
	return k, nil
}

// Get 按 ID 获取 Key（管理接口用）。
func (s *Store) Get(id string) (*Key, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, k := range s.keys {
		if k.ID == id {
			return clone(k), true
		}
	}
	return nil, false
}

// List 返回所有 Key（哈希不可逆，可安全暴露）。
func (s *Store) List() []*Key {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Key, 0, len(s.keys))
	for _, k := range s.keys {
		out = append(out, clone(k))
	}
	return out
}

// Ban / Unban 封禁与解封。
func (s *Store) Ban(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys {
		if k.ID == id {
			k.Status = StatusBanned
			return true
		}
	}
	return false
}

func (s *Store) Unban(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys {
		if k.ID == id {
			k.Status = StatusActive
			return true
		}
	}
	return false
}

// Delete 按 ID 删除 Key（管理 API 用）。删除后该 secret 立即失效。
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, k := range s.keys {
		if k.ID == id {
			delete(s.keys, hash)
			return true
		}
	}
	return false
}

// Update 更新 Key 的模型白名单（管理 API 用）。
func (s *Store) Update(id string, allowedModels []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys {
		if k.ID == id {
			k.AllowedModels = append([]string(nil), allowedModels...)
			return true
		}
	}
	return false
}
