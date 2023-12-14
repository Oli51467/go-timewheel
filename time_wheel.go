package go_timewheel

import (
	"container/list"
	"sync"
	"time"
)

// TimeWheel 时间轮
type TimeWheel struct {
	// 单例工具 保证时间轮停止操作只能执行一次
	sync.Once
	// 时间轮运行时间间隔
	interval time.Duration
	// 时间轮定时器 固定时间触发。它会以一个interval往通道发送当前时间，而通道的接收者可以以固定的时间间隔从通道中读取时间。
	// ticker中一旦有信号传出就代表又经过了一个interval的时间间隔，时间轮就要移动指针去下一个时间间隔中执行对应的任务
	ticker *time.Ticker
	// 停止时间轮的 channel
	stopCh chan struct{}
	// 新增定时任务的入口 channel
	addTaskCh chan *scheduledTask
	// 删除定时任务的入口 channel
	removeTaskCh chan string
	// 定长环状数组 每个slot槽内可能存在多个定时任务 存在List中
	slots []*list.List
	// 当前遍历到的环状数组的索引
	curSlot int
	// 定时任务key到任务节点的映射，便于在list中删除任务节点
	keyToETask map[string]*list.Element
}

// scheduledTask 定时任务
type scheduledTask struct {
	// 内聚了定时任务执行逻辑的闭包函数
	task func()
	// 定时任务挂载在环形数组中的索引位置
	pos int
	// 定时任务的延迟轮次
	rounds int
	// 定时任务的唯一标识键
	key string
	// 任务的执行时间
	executeAt time.Time
}

// NewDefaultTimeWheel 创建时间轮的构造器函数
// 完成定时器 ticker 以及各个 channel 的初始化，以及数组中的各个 slot 的初始化，每个 slot 位置都需要填充一个 list
func NewDefaultTimeWheel() *TimeWheel {
	t := TimeWheel{
		interval:     time.Second,
		ticker:       time.NewTicker(time.Second),
		stopCh:       make(chan struct{}),
		addTaskCh:    make(chan *scheduledTask),
		removeTaskCh: make(chan string),
		slots:        make([]*list.List, 0, 60),
		keyToETask:   make(map[string]*list.Element),
	}
	for i := 0; i < 60; i++ {
		t.slots = append(t.slots, list.New())
	}
	// 异步调用 run 方法，启动一个常驻 goroutine 用于接收和处理定时任务
	go t.run()
	return &t
}

// NewTimeWheel 创建时间轮的构造器函数
// slotNum：slot 的个数，默认为 10
// interval：每个 slot 对应的时间范围，默认为 1 秒
// 完成定时器 ticker 以及各个 channel 的初始化，以及数组中的各个 slot 的初始化，每个 slot 位置都需要填充一个 list
func NewTimeWheel(slotNum int, interval time.Duration) *TimeWheel {
	if slotNum <= 0 {
		slotNum = 60
	}
	if interval <= 0 {
		interval = time.Second
	}

	t := TimeWheel{
		interval:     interval,
		ticker:       time.NewTicker(interval),
		stopCh:       make(chan struct{}),
		addTaskCh:    make(chan *scheduledTask),
		removeTaskCh: make(chan string),
		slots:        make([]*list.List, 0, slotNum),
		keyToETask:   make(map[string]*list.Element),
	}
	for i := 0; i < slotNum; i++ {
		t.slots = append(t.slots, list.New())
	}
	// 异步调用 run 方法，启动一个常驻 goroutine 用于接收和处理定时任务
	go t.run()
	return &t
}

// run 使用 for 循环结合 select 多路复用从以下四类 channel 中接收不同的信号，并进行逻辑的分发处理：
// stopCh:		 停止时间轮，使得当前 goroutine 退出
// ticker:		 接收到 ticker 的信号说明时间由往前推进了一个 interval，则需要批量检索并执行当前 slot 中的定时任务. 并推进指针 curSlot 往前偏移
// addTaskCh:	 接收创建定时任务的指令
// removeTaskCh: 接收删除定时任务的指令
func (t *TimeWheel) run() {
	defer func() {
		if err := recover(); err != nil {

		}
	}()

	for {
		select {
		// 停止时间轮
		case <-t.stopCh:
			return
		case <-t.ticker.C:
			// 批量执行定时任务
			t.tick()
		case task := <-t.addTaskCh:
			t.addTask(task)
		case removeKey := <-t.removeTaskCh:
			t.removeTask(removeKey)
		}
	}
}

// Stop 主动关闭时间轮
func (t *TimeWheel) Stop() {
	// 通过单例工具，保证 channel 只能被关闭一次，避免 panic
	t.Do(func() {
		// 关闭定时器
		t.ticker.Stop()
		// 关闭定时器运行的 stopCh 在t.run()方法中会接收到信号
		close(t.stopCh)
	})
}

// addTask 异步协程接收到创建定时任务后的处理逻辑
func (t *TimeWheel) addTask(task *scheduledTask) {
	// 获取到定时任务从属的环状数组 index 以及对应的 list
	taskList := t.slots[task.pos]
	// 若定时任务 key 之前已存在，则需要先删除定时任务
	if _, ok := t.keyToETask[task.key]; ok {
		t.removeTask(task.key)
	}
	// 将定时任务追加到对应 slot 位置的 list 尾部 返回结果是包装后的List节点
	eTask := taskList.PushBack(task)
	// 建立定时任务 key 到将定时任务所处的节点
	t.keyToETask[task.key] = eTask
}

// AddTask 添加定时任务到时间轮中
func (t *TimeWheel) AddTask(key string, task func(), executeAt time.Time) {
	// 根据执行时间，推算出定时任务所处的 slot 位置以及需要延迟的轮次 cycle
	pos, rounds := t.getPositionAndCircle(executeAt)
	// 使用方往 addTaskCh 中投递定时任务，由常驻 goroutine 接收定时任务
	t.addTaskCh <- &scheduledTask{
		pos:    pos,
		rounds: rounds,
		task:   task,
		key:    key,
	}
}

// removeTask 异步协程接收到删除任务信号后，执行的删除任务逻辑
func (t *TimeWheel) removeTask(key string) {
	eTask, ok := t.keyToETask[key]
	if !ok {
		return
	}
	// 将定时任务节点从映射 map 中移除
	delete(t.keyToETask, key)
	// 获取到定时任务节点后，将其从 list 中移除
	task, _ := eTask.Value.(*scheduledTask)
	t.slots[task.pos].Remove(eTask)
}

// RemoveTask 删除一个定时任务
func (t *TimeWheel) RemoveTask(key string) {
	t.removeTaskCh <- key
}

// getPositionAndCircle 获得根据执行时间推算得到定时任务对应的slot，以及需要延迟的rounds
func (t *TimeWheel) getPositionAndCircle(executeAt time.Time) (pos, rounds int) {
	// 计算当前时间与执行时间的差值
	gap := int(time.Until(executeAt))
	// 计算定时任务的延迟轮次
	rounds = gap / (len(t.slots) * int(t.interval))
	// 计算该时间点在环形数组中的位置
	pos = (t.curSlot + gap/int(t.interval)) % len(t.slots)
	return
}

// tick 检索并批量执行定时任务
// 每当接收到 ticker 信号时，会根据当前的 curSlot 指针，获取到对应 slot 位置挂载的定时任务 list，调用 execute 方法执行其中的定时任务
// 最后通过 incrCircularPtr 方法推进 curSlot 指针向前移动.
func (t *TimeWheel) tick() {
	// 根据 curSlot 获取到当前所处的环状数组索引位置，取出对应的 list
	taskList := t.slots[t.curSlot]
	// 在方法返回前，推进 curSlot 指针的位置，进行环状遍历
	defer t.incrCircularPtr()
	// 批量处理该列表中的所有定时任务
	t.execute(taskList)
}

// incrCircularPtr 环形指针+1
func (t *TimeWheel) incrCircularPtr() {
	t.curSlot = (t.curSlot + 1) % len(t.slots)
}

// execute 执行列表中的所有应该被执行的定时任务
func (t *TimeWheel) execute(taskList *list.List) {
	for e := taskList.Front(); e != nil; {
		// 获取到每个节点对应的定时任务信息
		task, _ := e.Value.(*scheduledTask)
		// 如果该任务不应该在本轮中执行 则跳过
		if task.rounds > 0 {
			task.rounds--
			e = e.Next()
			continue
		}
		// 开启一个 goroutine 执行任务
		go func() {
			defer func() {
				if err := recover(); err != nil {

				}
			}()
			task.task()
		}()
		next := e.Next()
		taskList.Remove(e)
		delete(t.keyToETask, task.key)
		e = next
	}
}
