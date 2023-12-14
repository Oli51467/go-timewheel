package go_timewheel

import (
	"testing"
	"time"
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
