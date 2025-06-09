package retry

import (
	"fmt"
	"time"

	"git.pinquest.cn/base/log"
)

// RetryFunc 定义重试函数类型
type RetryFunc func() error

// Option 定义重试选项类型
type Option func(*options)

type options struct {
	retryTimes int
	interval   time.Duration
	logPrefix  string
}

// WithRetryTimes 设置重试次数
func WithRetryTimes(times int) Option {
	return func(o *options) {
		o.retryTimes = times
	}
}

// WithInterval 设置重试间隔
func WithInterval(d time.Duration) Option {
	return func(o *options) {
		o.interval = d
	}
}

// WithLogPrefix 设置日志前缀
func WithLogPrefix(prefix string) Option {
	return func(o *options) {
		o.logPrefix = prefix
	}
}

// DefaultOptions 默认重试选项
func DefaultOptions() *options {
	return &options{
		retryTimes: 3,                 // 默认重试 3 次
		interval:   time.Second * 2,   // 默认间隔 2 秒
		logPrefix:  "MilvusOperation", // 默认日志前缀
	}
}

// Do 执行重试机制
func Do(fn RetryFunc, opts ...Option) error {
	cfg := DefaultOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	var err error
	for i := 0; i < cfg.retryTimes; i++ {
		err = fn()
		if err == nil {
			log.Infof("[%s] Operation succeeded after %d retries.", cfg.logPrefix, i)
			return nil
		}
		log.Warnf("[%s] Retry %d/%d failed: %v", cfg.logPrefix, i+1, cfg.retryTimes, err)
		if i < cfg.retryTimes-1 {
			time.Sleep(cfg.interval)
		}
	}
	log.Errorf("[%s] All %d retries failed. Last error: %v", cfg.logPrefix, cfg.retryTimes, err)
	return fmt.Errorf("%s: all %d retries failed, last error: %w", cfg.logPrefix, cfg.retryTimes, err)
}
