package gotest

import (
	"testing"
	"time"
	"fmt"
	"math"
)

func TestTime_ticket(tst *testing.T) {
	const (
		interval = 1 * time.Second
	)
	c := time.Tick(interval)
	//tik := time.NewTicker(interval)
	//defer tik.Stop()
	done := make(chan struct{})
	go func() {
		time.Sleep(interval * 3)
		// fmt.Printf("%v wake and start call Stop!\n", tik)
		// close(tik.C.(chan time.Time))
		close(done)
	}()
	loop1st := true
	var prev time.Time
	for {
		select {
		case <-done:
			return
			break // just for select
			// case t, ok := <-tik.C:
			//if !ok {
			//	fmt.Printf("%v channel closed!\n", tik)
			//	return
			//}
		case t := <-c:
			fmt.Printf("%v\n", t)
			if loop1st {
				loop1st = false
				prev = t
			} else {
				fmt.Printf("[%v]-[%v]=[%v] want [%v]\n", t, prev, t.Sub(prev), interval)
				itv := t.Sub(prev) - interval
				itvf := math.Abs(float64(itv))
				if time.Duration(itvf) > time.Millisecond {
					tst.Errorf("[%v]-[%v]=[%v] want [%v]\n", t, prev, t.Sub(prev), interval)
				}
				prev = t
			}
		}
	}
}

func TestTime_NewTicker(tst *testing.T) {
	const (
		interval = 1 * time.Second
	)
	tik := time.NewTicker(interval)
	defer tik.Stop() // go语言圣经#326
	done := make(chan struct{})
	go func() {
		time.Sleep(interval * 3)
		// tik.Stop()
		close(done)
	}()
	loop1st := true
	var prev time.Time
	for {
		select {
		case t, ok := <-tik.C:
			fmt.Printf("%v,ok=%v\n", t, ok)
			if !ok {
				return
			}
			if loop1st {
				loop1st = false
				prev = t
			} else {
				fmt.Printf("[%v]-[%v]=[%v] want [%v]\n", t, prev, t.Sub(prev), interval)
				itv := t.Sub(prev) - interval
				itvf := math.Abs(float64(itv))
				if time.Duration(itvf) > time.Millisecond {
					tst.Errorf("[%v]-[%v]=[%v] want [%v]\n", t, prev, t.Sub(prev), interval)
				}
				prev = t
			}
		case <-done:
			return
		}
	}

}
