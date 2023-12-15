package time_wheel

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/demdxx/gocast"
	"go-timewheel/src/http"
	"go-timewheel/src/redis"
	"go-timewheel/src/util"
	netHttp "net/http"
	"strings"
	"time"

	"sync"
)

// RTimeWheel 分布式时间轮
type RTimeWheel struct {
	// 内置的单例工具，用于保证 stopCh 只被关闭一次
	sync.Once
	// 封装的redis客户端
	redisClient *redis.Client
	// 用于执行定时任务设定时的执行回调
	httpClient *http.Client
	// 用于停止时间轮的 channel
	stopCh chan struct{}
	// 触发定时扫描任务的定时器
	ticker *time.Ticker
}

// RScheduledTask 定时任务
type RScheduledTask struct {
	// 定时任务全局唯一 key
	Key string `json:"key"`
	// 定时任务执行时，回调的 http url
	CallbackURL string `json:"callback_url"`
	// 回调时使用的 http 方法
	Method string `json:"method"`
	// 回调时传递的请求参数
	Req interface{} `json:"req"`
	// 回调时使用的 http 请求头
	Header map[string]string `json:"header"`
}

// NewRTimeWheel 创建时间轮的构造器函数
func NewRTimeWheel(redisClient *redis.Client, httpClient *http.Client) *RTimeWheel {
	r := &RTimeWheel{
		// 创建一个每隔 1s 执行一次的定时器
		ticker:      time.NewTicker(time.Second),
		redisClient: redisClient,
		httpClient:  httpClient,
		stopCh:      make(chan struct{}),
	}
	go r.run()
	return r
}

// run 时间轮监听协程
func (t *RTimeWheel) run() {
	for {
		select {
		case <-t.stopCh:
			return
		case <-t.ticker.C:
			// 又到达了一个新时间点，获取该时间点下的所有任务执行
			go t.executeTasks()
		}
	}
}

// Stop 停止时间轮
func (t *RTimeWheel) Stop() {
	t.Do(func() {
		close(t.stopCh)
		// 终止定时器 ticker
		t.ticker.Stop()
	})
}

// AddTask 创建定时任务
func (t *RTimeWheel) AddTask(ctx context.Context, key string, task *RScheduledTask, executeAt time.Time) error {
	// 定时任务的参数进行校验
	if err := t.checkAddTaskParams(task); err != nil {
		return err
	}

	task.Key = key
	// 序列化定时任务为字节数组
	taskBody, _ := json.Marshal(task)
	// 执行 lua 脚本使用 zAdd 指令，实现将定时任务添加 redis zSet 中
	_, err := t.redisClient.Eval(ctx, LuaAddTasks, 2, []interface{}{
		// keys1-2 zSet时间片-分钟级别
		t.getMinuteSlice(executeAt),
		t.getDeleteSetKey(executeAt),
		// 以执行时刻的秒级时间戳作为 zSet 中的 score
		executeAt.Unix(),
		// RScheduledTask回调细节字符串
		string(taskBody),
		// 标识删除的key
		key,
	})
	return err
}

func (t *RTimeWheel) LazyRemoveTask(ctx context.Context, key string, executeAt time.Time) error {
	// 执行 lua 脚本，将被删除的任务追加到 set 中
	_, err := t.redisClient.Eval(ctx, LuaDeleteTask, 1, []interface{}{
		t.getDeleteSetKey(executeAt),
		key,
	})
	return err
}

// executeTasks 执行一个时间片中的所有定时任务
func (t *RTimeWheel) executeTasks() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("%s", err)
		}
	}()

	// 并发控制，保证 30 s 内完成该批次全量任务的执行，及时回收 goroutine，避免发生 goroutine 泄漏
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	// 根据当前时间条件扫描 redis zSet，获取所有满足执行条件的定时任务
	tasks, err := t.getExecutableTasks(timeoutCtx)
	if err != nil {
		fmt.Printf("%s", err)
		return
	}
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		// 闭包
		task := task
		go func() {
			defer func() {
				if err := recover(); err != nil {
					wg.Done()
				}
			}()
			if err := t.executeTask(timeoutCtx, task); err != nil {
				fmt.Printf("%s", err)
			}
		}()
	}
	wg.Wait()
}

// getExecutableTasks 获取所有满足执行条件的定时任务
func (t *RTimeWheel) getExecutableTasks(ctx context.Context) ([]*RScheduledTask, error) {
	now := time.Now()
	// 根据当前时间，推算出其从属的分钟级时间片
	minuteRedisSliceKey := t.getMinuteSlice(now)
	minuteRedisDelKey := t.getDeleteSetKey(now)
	nowSecond := util.GetTimeSecond(now)
	score1 := nowSecond.Unix()
	score2 := nowSecond.Add(time.Second).Unix()
	// 执行 lua 脚本，本质上是通过 zRange 指令结合秒级时间戳对应的 score 进行定时任务检索
	rawReply, err := t.redisClient.Eval(ctx, LuaZRangeTasks, 2, []interface{}{
		minuteRedisSliceKey, minuteRedisDelKey, score1, score2,
	})
	if err != nil {
		return nil, err
	}
	// 结果中，首个元素对应为已删除任务的 key 集合，后续元素对应为各笔定时任务
	replies := gocast.ToInterfaceSlice(rawReply)
	if len(replies) == 0 {
		return nil, fmt.Errorf("invalid replies: %v", replies)
	}

	deletedTasks := gocast.ToStringSlice(replies[0])
	deletedTasksSet := make(map[string]struct{}, len(deletedTasks))
	// 放入deletedTasksSet集合
	for _, deleted := range deletedTasks {
		deletedTasksSet[deleted] = struct{}{}
	}

	// 遍历各笔定时任务，若其存在于删除集合中，则跳过，否则追加到 list 中返回，用于后续执行
	tasks := make([]*RScheduledTask, 0, len(replies)-1)
	for i := 1; i < len(replies); i++ {
		var task RScheduledTask
		if err := json.Unmarshal([]byte(gocast.ToString(replies[i])), &task); err != nil {
			continue
		}

		if _, ok := deletedTasksSet[task.Key]; ok {
			continue
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

// executeTask 调用http请求回调具体的函数
func (t *RTimeWheel) executeTask(ctx context.Context, task *RScheduledTask) interface{} {
	return t.httpClient.JSONDo(ctx, task.Method, task.CallbackURL, task.Header, task.Req, nil)
}

// checkAddTaskParams 添加定时任务前置参数校验
func (t *RTimeWheel) checkAddTaskParams(task *RScheduledTask) error {
	if task.Method != netHttp.MethodGet && task.Method != netHttp.MethodPost {
		return fmt.Errorf("invalid method: %s", task.Method)
	}
	if !strings.HasPrefix(task.CallbackURL, "http://") && !strings.HasPrefix(task.CallbackURL, "https://") {
		return fmt.Errorf("invalid url: %s", task.CallbackURL)
	}
	return nil
}

// getMinuteSlice 获得分钟级的redis-key
func (t *RTimeWheel) getMinuteSlice(executeAt time.Time) string {
	return fmt.Sprintf("go_timewheel_{%s}", util.GetTimeMinuteStr(executeAt))
}

// getDeleteSetKey 获得分钟级的redis-delete-key
func (t *RTimeWheel) getDeleteSetKey(executeAt time.Time) string {
	return fmt.Sprintf("go_timewheel_delset_{%s}", util.GetTimeMinuteStr(executeAt))
}
