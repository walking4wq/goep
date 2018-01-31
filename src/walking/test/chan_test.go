package gotest

import (
	"testing"
	"reflect"
)

type ValueObject16 struct {
	nbr01, nbr15 int64 // 16 B
	bln01        bool
}

func (vo *ValueObject16) Set1stInt64(i int64) { vo.nbr01 = i }
func (vo *ValueObject16) SetMidInt64(i int64) { vo.nbr15 = i }
func (vo ValueObject16) GetMidInt64() int64   { return vo.nbr15 }
func (vo *ValueObject16) SetLastBool(b bool)  { vo.bln01 = b }

type ValueObjectProf struct {
	// nbr01 int64;flt01 float64;str01 string;bln01 bool;
	nbr01 int64;
	flt01 float64;
	str01 string;
	bln01 bool;
	nbr02 int64;
	flt02 float64;
	str02 string;
	bln02 bool;
	nbr03 int64;
	flt03 float64;
	str03 string;
	bln03 bool;
	nbr04 int64;
	flt04 float64;
	str04 string;
	bln04 bool;
	nbr05 int64;
	flt05 float64;
	str05 string;
	bln05 bool;
	nbr06 int64;
	flt06 float64;
	str06 string;
	bln06 bool;
}

func (vo *ValueObjectProf) Set1stInt64(i int64) { vo.nbr01 = i }
func (vo *ValueObjectProf) SetMidInt64(i int64) { vo.nbr04 = i }
func (vo ValueObjectProf) GetMidInt64() int64   { return vo.nbr04 }
func (vo *ValueObjectProf) SetLastBool(b bool)  { vo.bln06 = b /* vo.bln08 = b */ }

type ValueObject1280 struct {
	// nbr01 int64;flt01 float64;str01 string;bln01 bool;
	nbr01 int64;
	flt01 float64;
	str01 string;
	bln01 bool;
	nbr02 int64;
	flt02 float64;
	str02 string;
	bln02 bool;
	nbr03 int64;
	flt03 float64;
	str03 string;
	bln03 bool;
	nbr04 int64;
	flt04 float64;
	str04 string;
	bln04 bool;
	nbr05 int64;
	flt05 float64;
	str05 string;
	bln05 bool;
	nbr06 int64;
	flt06 float64;
	str06 string;
	bln06 bool;
	nbr07 int64;
	flt07 float64;
	str07 string;
	bln07 bool;
	nbr08 int64;
	flt08 float64;
	str08 string;
	bln08 bool;
	nbr09 int64;
	flt09 float64;
	str09 string;
	bln09 bool;
	nbr10 int64;
	flt10 float64;
	str10 string;
	bln10 bool;
	nbr11 int64;
	flt11 float64;
	str11 string;
	bln11 bool;
	nbr12 int64;
	flt12 float64;
	str12 string;
	bln12 bool;
	nbr13 int64;
	flt13 float64;
	str13 string;
	bln13 bool;
	nbr14 int64;
	flt14 float64;
	str14 string;
	bln14 bool;
	nbr15 int64;
	flt15 float64;
	str15 string;
	bln15 bool;
	nbr16 int64;
	flt16 float64;
	str16 string;
	bln16 bool;
	nbr17 int64;
	flt17 float64;
	str17 string;
	bln17 bool;
	nbr18 int64;
	flt18 float64;
	str18 string;
	bln18 bool;
	nbr19 int64;
	flt19 float64;
	str19 string;
	bln19 bool;
	nbr20 int64;
	flt20 float64;
	str20 string;
	bln20 bool;
	nbr21 int64;
	flt21 float64;
	str21 string;
	bln21 bool;
	nbr22 int64;
	flt22 float64;
	str22 string;
	bln22 bool;
	nbr23 int64;
	flt23 float64;
	str23 string;
	bln23 bool;
	nbr24 int64;
	flt24 float64;
	str24 string;
	bln24 bool;
	nbr25 int64;
	flt25 float64;
	str25 string;
	bln25 bool;
	nbr26 int64;
	flt26 float64;
	str26 string;
	bln26 bool;
	nbr27 int64;
	flt27 float64;
	str27 string;
	bln27 bool;
	nbr28 int64;
	flt28 float64;
	str28 string;
	bln28 bool;
	nbr29 int64;
	flt29 float64;
	str29 string;
	bln29 bool;
	nbr30 int64;
	flt30 float64;
	str30 string;
	bln30 bool; // 1280
}

func (vo *ValueObject1280) Set1stInt64(i int64) { vo.nbr01 = i }
func (vo *ValueObject1280) SetMidInt64(i int64) { vo.nbr15 = i }
func (vo ValueObject1280) GetMidInt64() int64   { return vo.nbr15 }
func (vo *ValueObject1280) SetLastBool(b bool)  { vo.bln30 = b }

type VOType = int

const (
	SmallVO VOType = iota
	ProfVO
	LargeVO
)

func NewV(typ VOType, b *testing.B) (vo ValueObject) {
	switch typ {
	case SmallVO:
		vo = new(ValueObject16)
	case ProfVO:
		vo = new(ValueObjectProf)
	case LargeVO:
		vo = new(ValueObject1280)
	default:
		b.Fatalf("Unexcepte VOType:%v!", typ)
	}
	return
}
func NewP(typ VOType, b *testing.B) *ValueObject { // (vop *ValueObject) { // http://blog.csdn.net/qq_26981997/article/details/52608081
	var vo ValueObject = nil // NewV(typ, b)
	switch typ {
	case SmallVO:
		vo = new(ValueObject16) // &ValueObject16{0, 0, false}
	case ProfVO:
		vo = new(ValueObjectProf)
	case LargeVO:
		vo = new(ValueObject1280)
	default:
		b.Fatalf("Unexcepte VOType:%v!", typ)
	}
	return &vo // alloc 16 Byte = {type, value : int64}
}

type VChan16 = chan ValueObject16
type PChan16 = chan *ValueObject16

type VChanProf = chan ValueObjectProf
type PChanProf = chan *ValueObjectProf

type VChan1280 = chan ValueObject1280
type PChan1280 = chan *ValueObject1280

type ValueObject interface {
	Set1stInt64(int64)
	SetMidInt64(int64)
	GetMidInt64() int64
	SetLastBool(bool)
}
type VChan = chan ValueObject
type PChan = chan *ValueObject

func benchmarkVChan(b *testing.B, typ VOType) {
	chan_ := make(VChan, 1024)
	go func() {
		defer close(chan_)
		var vo ValueObject
		for i := 0; i < b.N; i++ {
			// NewV(typ, b) // add 1 time alloc
			vo = NewV(typ, b)
			vo.Set1stInt64(int64(i))
			vo.SetMidInt64(2)
			vo.SetLastBool(i/2 == 0)
			vo.GetMidInt64()
			chan_ <- vo
		}
	}()

	var cnt int64 = 0
	for {
		select {
		case vo, ok := <-chan_:
			if ok {
				cnt = cnt + vo.GetMidInt64()
			} else {
				goto endOfLoop
			}
		}
	}
endOfLoop:
	rst := int64(b.N) * 2
	if cnt != rst {
		b.Errorf("End of benchmarkVChan want cnt[%d]==[%d]", cnt, rst)
	}
}
func benchmarkPChan(b *testing.B, typ VOType) {
	chan_ := make(PChan, 1024)
	go func() {
		defer close(chan_)
		vo := NewP(typ, b) // NewP(typ, b)
		for i := 0; i < b.N; i++ {
			vo = NewP(typ, b)
			(*vo).Set1stInt64(int64(i))
			(*vo).SetMidInt64(2)
			(*vo).SetLastBool(i/2 == 0)
			(*vo).GetMidInt64()
			//vo.Set1stInt64(int64(i))
			//vo.SetMidInt64(2)
			//vo.SetLastBool(i/2 == 0)
			chan_ <- vo
		}
	}()

	var cnt int64 = 0
	for {
		select {
		case vo, ok := <-chan_:
			if ok {
				cnt = cnt + (*vo).GetMidInt64()
			} else {
				goto endOfLoop
			}
		}
	}
endOfLoop:
	rst := int64(b.N) * 2
	if cnt != rst {
		b.Errorf("End of benchmarkPChan want cnt[%d]==[%d]", cnt, rst)
	}
}

// SmallVO ProfVO LargeVO
//benchmarkVChan(b, SmallVO)
//benchmarkVChan(b, ProfVO)
//benchmarkVChan(b, LargeVO)
//benchmarkPChan(b, SmallVO)
//benchmarkPChan(b, ProfVO)
//benchmarkPChan(b, LargeVO)
func BenchmarkChan_VChanSmallUnion(b *testing.B) { benchmarkVChan(b, SmallVO) }
func BenchmarkChan_VChanProfUnion(b *testing.B)  { benchmarkVChan(b, ProfVO) }
func BenchmarkChan_VChanLargeUnion(b *testing.B) { benchmarkVChan(b, LargeVO) }
func BenchmarkChan_PChanSmallUnion(b *testing.B) { benchmarkPChan(b, SmallVO) }
func BenchmarkChan_PChanProfUnion(b *testing.B)  { benchmarkPChan(b, ProfVO) }
func BenchmarkChan_PChanLargeUnion(b *testing.B) { benchmarkPChan(b, LargeVO) }

func BenchmarkChan_VChan(b *testing.B) {
	chan_ := make(VChan1280, 1024)
	go func() {
		defer close(chan_)
		vo := new(ValueObject1280)
		for i := 0; i < b.N; i++ {
			vo = new(ValueObject1280) // vo := new(ValueObject) // no alloc mem
			vo.nbr01 = int64(i)
			vo.nbr15 = 2
			vo.bln30 = i/2 == 0
			chan_ <- *vo
		}
	}()

	var cnt int64 = 0
	for {
		select {
		case vo, ok := <-chan_:
			if ok {
				cnt = cnt + vo.nbr15
			} else {
				goto endOfLoop
			}
		}
	}
endOfLoop:
	rst := int64(b.N) * 2
	if cnt != rst {
		b.Errorf("End of BenchmarkChan_VChan want cnt[%d]==[%d]", cnt, rst)
	}
}

func BenchmarkChan_PChan(b *testing.B) {
	chan_ := make(PChan1280, 1024)
	go func() {
		defer close(chan_)
		vo := new(ValueObject1280)
		for i := 0; i < b.N; i++ {
			vo = new(ValueObject1280)
			vo.nbr01 = int64(i)
			vo.nbr15 = 2
			vo.bln30 = i/2 == 0
			chan_ <- vo
		}
	}()

	var cnt int64 = 0
	for {
		select {
		case vo, ok := <-chan_:
			if ok {
				cnt = cnt + vo.nbr15
			} else {
				goto endOfLoop
			}
		}
	}
endOfLoop:
	rst := int64(b.N) * 2
	if cnt != rst {
		b.Errorf("End of BenchmarkChan_PChan want cnt[%d]==[%d]", cnt, rst)
	}
}
func benchmarkVAlloc(b *testing.B) {
	var vo ValueObject1280
	for i := 0; i < b.N; i++ {
		vo_ := new(ValueObject1280)
		vo = *vo_

		vo.nbr01 = int64(i)
		vo.nbr15 = 2
		vo.bln30 = i/2 == 0
	}
	vo.nbr01 = int64(0)
	vo.nbr15 = 2
	vo.bln30 = true
}
func benchmarkPAlloc(b *testing.B) {
	var vo *ValueObject1280
	for i := 0; i < b.N; i++ {
		// vo := new(ValueObject1280)
		vo = new(ValueObject1280)
		vo.nbr01 = int64(i)
		vo.nbr15 = 2
		vo.bln30 = i/2 == 0
	}
	vo.nbr01 = int64(0)
	vo.nbr15 = 2
	vo.bln30 = true
}

// https://www.cnblogs.com/luckcs/articles/4107647.html
func BenchmarkChan_VAlloc(b *testing.B) { benchmarkVAlloc(b) }
func BenchmarkChan_PAlloc(b *testing.B) { benchmarkPAlloc(b) }

// http://blog.csdn.net/len_yue_mo_fu/article/details/73530494
type Integer int

// func (a Integer) Less(b Integer) bool { return a < b }
func (a Integer) Get() Integer   { return a }
func (a *Integer) Add(b Integer) { *a += b }

//type Lesser interface {
//	Less(b Integer) bool
//}
//type LessAdder interface {
//	Less(b Integer) bool
//	Add(b Integer)
//}
//type Adder interface {
//	Add(b Integer)
//}
type Getter interface {
	Get() Integer
}
type GetAdder interface {
	Get() Integer
	Add(b Integer)
}
type Adder interface {
	Add(b Integer)
}

func TestItf(t *testing.T) {
	var a Integer = 1
	typ := reflect.TypeOf(a)
	t.Logf("var a Integer; a=%v, reflect.TypeOf=[%v][%v][%v]", a, typ, typ.Kind(), reflect.ValueOf(a))

	var b Getter = a
	var b2 Getter = &a
	typ = reflect.TypeOf(b)
	t.Logf("var b Getter = a; b=%v, reflect.TypeOf=[%v][%v][%v]", b, typ, typ.Kind(), reflect.ValueOf(b))
	typ = reflect.TypeOf(b2)
	t.Logf("var b2 Getter = &a; b2=%v, reflect.TypeOf=[%v][%v][%v]:%v", b2, typ, typ.Kind(), reflect.ValueOf(b2), b2.Get())

	// var c GetAdder = a // Cannot use a (type Integer) as type GetAdder in assignment
	var c2 GetAdder = &a
	typ = reflect.TypeOf(c2)
	t.Logf("var c2 GetAdder = &a; c2=%v, reflect.TypeOf=[%v][%v][%v]:%v", c2, typ, typ.Kind(), reflect.ValueOf(c2), c2.Get())

	// var d Adder = a // Cannot use a (type Integer) as type Adder in assignment
	var d2 Adder = &a
	typ = reflect.TypeOf(d2)
	t.Logf("var d2 Adder = &a; d2=%v, reflect.TypeOf=[%v][%v][%v]", d2, typ, typ.Kind(), reflect.ValueOf(d2))
	// 结论：只要接口实现的方法包含[指针实现]，则接口中的存储的类型只能为指针。反过来说，只有在实现方法中不包含[指针实现]，接口才能存储值
	c2.Add(1)
	t.Logf("After add 1 for c2")
	typ = reflect.TypeOf(a)
	t.Logf("var a Integer; a=%v, reflect.TypeOf=[%v][%v][%v]", a, typ, typ.Kind(), reflect.ValueOf(a))
	typ = reflect.TypeOf(b)
	t.Logf("var b Getter = a; b=%v, reflect.TypeOf=[%v][%v][%v]", b, typ, typ.Kind(), reflect.ValueOf(b))
	typ = reflect.TypeOf(b2)
	t.Logf("var b2 Getter = &a; b2=%v, reflect.TypeOf=[%v][%v][%v]:%v", b2, typ, typ.Kind(), reflect.ValueOf(b2), b2.Get())
	typ = reflect.TypeOf(c2)
	t.Logf("var c2 GetAdder = &a; c2=%v, reflect.TypeOf=[%v][%v][%v]:%v", c2, typ, typ.Kind(), reflect.ValueOf(c2), c2.Get())
	typ = reflect.TypeOf(d2)
	t.Logf("var d2 Adder = &a; d2=%v, reflect.TypeOf=[%v][%v][%v]", d2, typ, typ.Kind(), reflect.ValueOf(d2))
	d2.Add(1)
	t.Logf("After add 1 for d2")
	typ = reflect.TypeOf(a)
	t.Logf("var a Integer; a=%v, reflect.TypeOf=[%v][%v][%v]", a, typ, typ.Kind(), reflect.ValueOf(a))
	typ = reflect.TypeOf(b)
	t.Logf("var b Getter = a; b=%v, reflect.TypeOf=[%v][%v][%v]", b, typ, typ.Kind(), reflect.ValueOf(b))
	typ = reflect.TypeOf(b2)
	t.Logf("var b2 Getter = &a; b2=%v, reflect.TypeOf=[%v][%v][%v]:%v", b2, typ, typ.Kind(), reflect.ValueOf(b2), b2.Get())
	typ = reflect.TypeOf(c2)
	t.Logf("var c2 GetAdder = &a; c2=%v, reflect.TypeOf=[%v][%v][%v]:%v", c2, typ, typ.Kind(), reflect.ValueOf(c2), c2.Get())
	typ = reflect.TypeOf(d2)
	t.Logf("var d2 Adder = &a; d2=%v, reflect.TypeOf=[%v][%v][%v]", d2, typ, typ.Kind(), reflect.ValueOf(d2))
}
