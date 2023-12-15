package time_wheel

import (
	"context"
	"go-timewheel/src/http"
	"go-timewheel/src/redis"
	"testing"
	"time"
)

const (
	// redis 服务器信息
	network  = "tcp"
	address  = "127.0.0.1:6379"
	password = "hmis1234."
)

var (
	// 定时任务回调信息
	callbackURL    = "http://127.0.0.1:8888/ping"
	callbackMethod = "GET"
	callbackReq    interface{}
	callbackHeader map[string]string
)

func Test_timeWheel(t *testing.T) {
	timeWheel := NewDefaultTimeWheel()
	defer timeWheel.Stop()
	timeWheel.AddTask("test1", func() {
		t.Errorf("test1, %v", time.Now())
	}, time.Now().Add(time.Second))
	timeWheel.AddTask("test2", func() {
		t.Errorf("test2, %v", time.Now())
	}, time.Now().Add(5*time.Second))
	timeWheel.AddTask("test3", func() {
		t.Errorf("test3, %v", time.Now())
	}, time.Now().Add(3*time.Second))
	time.Sleep(6 * time.Second)
}

func Test_redis_timeWheel(t *testing.T) {
	rTimeWheel := NewRTimeWheel(
		redis.NewClient(network, address, password),
		http.NewClient(),
	)
	defer rTimeWheel.Stop()
	ctx := context.Background()

	if err := rTimeWheel.AddTask(ctx, "test1", &RScheduledTask{
		CallbackURL: callbackURL,
		Method:      callbackMethod,
		Req:         callbackReq,
		Header:      callbackHeader,
	}, time.Now().Add(time.Second)); err != nil {
		t.Error(err)
		return
	}

	if err := rTimeWheel.AddTask(ctx, "test2", &RScheduledTask{
		CallbackURL: callbackURL,
		Method:      callbackMethod,
		Req:         callbackReq,
		Header:      callbackHeader,
	}, time.Now().Add(4*time.Second)); err != nil {
		t.Error(err)
		return
	}

	if err := rTimeWheel.LazyRemoveTask(ctx, "test2", time.Now().Add(4*time.Second)); err != nil {
		t.Error(err)
		return
	}

	<-time.After(5 * time.Second)
	t.Log("ok")
}
