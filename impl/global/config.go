// impl/global/config.go
package global

import "sync"

var (
	dbConfigName string
	mu           sync.RWMutex // 用于保护 dbConfigName 的并发访问，虽然主要在启动时设置，但读取可能并发
)

// SetDBConfigName 用于在应用初始化时设置全局数据库配置名。
// 这个函数应该在 impl.S.Conf.Db 被加载后立即调用。
func SetDBConfigName(name string) {
	mu.Lock()
	defer mu.Unlock()
	dbConfigName = name
}

// GetDBConfigName 返回全局设置的数据库配置名。
// 其他包可以通过调用 global.GetDBConfigName() 来获取。
func GetDBConfigName() string {
	mu.RLock()
	defer mu.RUnlock()
	return dbConfigName
}
