package cdr

import (
	"time"
	"fmt"
	"strings"
)

/*
create table txe_inputkpi_record (
   circle_id            varchar(20)          null,
   file_type            varchar(4)           null,
   record_type          varchar(4)           null,
   filename             varchar(41)          null, -- 没有路径的文件名称
   OriginatingCircle    varchar(6)           null, -- 同 circle_id 表示漫游：只有 MSC 与 ROAM 有不同。实际上MSC会取字段14
   Subscription_Type    varchar(3)           null, -- 字段14，TAPIN 不填，MSC为字段15
   records              numeric(10)          null, -- count(1)
   oper_date            timestamp            null,
   service_time         timestamp            null,
   constraint PK_TXE_INPUTKPI_RECORD primary key ()
);
comment on table txe_inputkpi_record is
'利用文件直接统计入库的信息表，按照业务类型区分';
comment on column txe_inputkpi_record.circle_id is
'取文件名称的最后一共段如：HP
改为：
MSC:字段1
ROAM,INMA:字段13
TAPIN:字段3的前5位';
comment on column txe_inputkpi_record.file_type is
'不同的目录存放不同的cdr文件类型，当前支持四种以及编码为：
MSC:1,ROAM:2,TAPIN:3,INMA:4';
comment on column txe_inputkpi_record.record_type is
'文件第一个字段
MSC为2字段';
comment on column txe_inputkpi_record.oper_date is
'系统时间';
comment on column txe_inputkpi_record.service_time is
'暂取系统时间';

*/
type txe_inputkpi_record struct {
	circle_id, file_type, record_type, filename, originatingCircle, subscription_Type string
	records                                                                           int64
	oper_date, service_time                                                           time.Time
	group                                                                             map[string]*Event
}

func (evt txe_inputkpi_record) String() string {
	return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%d,%s,%s",
		evt.circle_id, evt.file_type, evt.record_type, evt.filename, evt.originatingCircle, evt.subscription_Type,
		evt.records, evt.oper_date.Format("2006-01-02 15:04:05.000"),
		evt.service_time.Format("2006-01-02 15:04:05.000"))
}
func (evt txe_inputkpi_record) Key() string {
	return fmt.Sprintf("%s,%s,%s,%s,%s",
		evt.circle_id, evt.file_type, evt.record_type, /*evt.filename,*/ evt.originatingCircle, evt.subscription_Type)
}

type CdrType string

const (
	CdrType4MSC   CdrType = "1" // iota
	CdrType4ROAM          = "2"
	CdrType4TAPIN         = "3"
	CdrType4INMA          = "4"
)

func parse_txe_inputkpi_record(fileType CdrType, line []byte, pathFile string, linCnt int) (
	evt *txe_inputkpi_record, row4err string) {

	cols := strings.Split(string(line), ",")
	size := len(cols)
	switch(fileType) {
	case CdrType4MSC:
		if size < 15 {
			return nil, fmt.Sprintf("Too little[%d<15] for parse to %v", size, fileType)
		}
	case CdrType4ROAM:
		if size < 14 {
			return nil, fmt.Sprintf("Too little[%d<14] for parse to %v", size, fileType)
		}
	case CdrType4TAPIN:
		if size < 3 {
			return nil, fmt.Sprintf("Too little[%d<3] for parse to %v", size, fileType)
		}
		if len(cols[2]) < 5 {
			return nil, fmt.Sprintf("Too short col[2:circle_id][%s] length<5 for parse to %v",
				cols[2], fileType)
		}
	case CdrType4INMA:
		if size < 14 {
			return nil, fmt.Sprintf("Too little[%d<14] for parse to %v", size, fileType)
		}
	default:
		return nil, fmt.Sprintf("Unexcept file_type:%v!", fileType)
	}

	var circle_id, file_type, record_type, filename, originatingCircle, subscription_Type string
	var records int64
	var oper_date, service_time time.Time

	switch(fileType) {
	case CdrType4MSC:
		circle_id = cols[0]
		record_type = cols[1]
		originatingCircle = cols[13]
		subscription_Type = cols[14]
	case CdrType4ROAM:
		circle_id = cols[12]
		record_type = cols[0]
		originatingCircle = circle_id
		subscription_Type = cols[13]
	case CdrType4TAPIN:
		circle_id = cols[2][0:5]
		record_type = cols[0]
		originatingCircle = circle_id
	case CdrType4INMA:
		circle_id = cols[12]
		record_type = cols[0]
		originatingCircle = circle_id
		subscription_Type = cols[13]
	}
	file_type = string(fileType)
	// filename = filepath.Base(pathFile)

	t := txe_inputkpi_record{
		strings.Trim(circle_id, " "), file_type,
		strings.Trim(record_type, " "), filename,
		strings.Trim(originatingCircle, " "),
		strings.Trim(subscription_Type, " "),
		records, oper_date, service_time, nil}
	return &t, ""
}

type GroupMSC struct{ *txe_inputkpi_record }
type GroupROAM struct{ *txe_inputkpi_record }
type GroupINMA struct{ *txe_inputkpi_record }
type GroupTAPIN struct{ *txe_inputkpi_record }

func (evt GroupMSC) ToDsv() string   { return evt.String() }
func (evt GroupROAM) ToDsv() string  { return evt.String() }
func (evt GroupINMA) ToDsv() string  { return evt.String() }
func (evt GroupTAPIN) ToDsv() string { return evt.String() }

var ddl_ string = `CREATE TABLE IF NOT EXISTS %s (
   circle_id           varchar(20) DEFAULT null,
   file_type           varchar(4)  DEFAULT null,
   record_type         varchar(4)  DEFAULT null,
   filename            varchar(41) DEFAULT null,
   OriginatingCircle   varchar(6)  DEFAULT null,
   Subscription_Type   varchar(3)  DEFAULT null,
   records             numeric(10) DEFAULT null,
   oper_date           timestamp   null DEFAULT null,
   service_time        timestamp   null DEFAULT null
 )` // ENGINE=InnoDB DEFAULT CHARSET=utf8`
var load_ string = `load data local infile '%s' into table %s
fields terminated by ',' enclosed by '\'' lines terminated by '\n'`

func (_ GroupMSC) Sql() (ddl, load string)   { return ddl_, load_ }
func (_ GroupROAM) Sql() (ddl, load string)  { return ddl_, load_ }
func (_ GroupINMA) Sql() (ddl, load string)  { return ddl_, load_ }
func (_ GroupTAPIN) Sql() (ddl, load string) { return ddl_, load_ }

func (_ GroupMSC) NewEvent(line []byte, pathFile string, linCnt int) (evt *Event, row4err string) {
	t, row4err := parse_txe_inputkpi_record(CdrType4MSC, line, pathFile, linCnt)
	var e Event = GroupMSC{t}
	evt = &e
	return
}
func (_ GroupROAM) NewEvent(line []byte, pathFile string, linCnt int) (evt *Event, row4err string) {
	t, row4err := parse_txe_inputkpi_record(CdrType4ROAM, line, pathFile, linCnt)
	var e Event = GroupROAM{t}
	evt = &e
	return
}
func (_ GroupINMA) NewEvent(line []byte, pathFile string, linCnt int) (evt *Event, row4err string) {
	t, row4err := parse_txe_inputkpi_record(CdrType4INMA, line, pathFile, linCnt)
	var e Event = GroupINMA{t}
	evt = &e
	return
}
func (_ GroupTAPIN) NewEvent(line []byte, pathFile string, linCnt int) (evt *Event, row4err string) {
	t, row4err := parse_txe_inputkpi_record(CdrType4TAPIN, line, pathFile, linCnt)
	var e Event = GroupTAPIN{t}
	evt = &e
	return
}

// func (t GroupMSC) Reduce(evt *Event) (evts map[string][]*Event) {
func (t GroupMSC) Reduce(evt *Event) (sel *Event, grp map[string]*Event) {
	if evt == nil { // evt == nil then evts.Reduce output all cache
		return nil, t.group
	}
	if t.group == nil {
		t.group = make(map[string]*Event) // make(map[string][]*Event)
	}
	t2 := (*evt).(GroupMSC)
	e, ok := t.group[t2.Key()]
	if ok {
		tmp := (*e).(GroupMSC)
		if t2.records == 0 {
			tmp.records++
		} else {
			tmp.records = tmp.records + t2.records
		}
	} else {
		if t2.records == 0 {
			t2.records = 1
		}
		t2.oper_date = time.Now().Local()
		var e Event = t2
		t.group[t2.Key()] = &e // []*Event{&e}
	}
	return
}
func (t GroupROAM) Reduce(evt *Event) (sel *Event, grp map[string]*Event) {
	if evt == nil { // evt == nil then evts.Reduce output all cache
		return nil, t.group
	}
	if t.group == nil {
		t.group = make(map[string]*Event)
	}
	t2 := (*evt).(GroupROAM)
	e, ok := t.group[t2.Key()]
	if ok {
		tmp := (*e).(GroupROAM)
		if t2.records == 0 {
			tmp.records++
		} else {
			tmp.records = tmp.records + t2.records
		}
	} else {
		if t2.records == 0 {
			t2.records = 1
		}
		t2.oper_date = time.Now().Local()
		var e Event = t2
		t.group[t2.Key()] = &e
	}
	return
}
func (t GroupINMA) Reduce(evt *Event) (sel *Event, grp map[string]*Event) {
	if evt == nil { // evt == nil then evts.Reduce output all cache
		return nil, t.group
	}
	if t.group == nil {
		t.group = make(map[string]*Event)
	}
	t2 := (*evt).(GroupINMA)
	e, ok := t.group[t2.Key()]
	if ok {
		tmp := (*e).(GroupINMA)
		if t2.records == 0 {
			tmp.records++
		} else {
			tmp.records = tmp.records + t2.records
		}
	} else {
		if t2.records == 0 {
			t2.records = 1
		}
		t2.oper_date = time.Now().Local()
		var e Event = t2
		t.group[t2.Key()] = &e
	}
	return
}
func (t GroupTAPIN) Reduce(evt *Event) (sel *Event, grp map[string]*Event) {
	if evt == nil { // evt == nil then evts.Reduce output all cache
		return nil, t.group
	}
	if t.group == nil {
		t.group = make(map[string]*Event)
	}
	t2 := (*evt).(GroupTAPIN)
	e, ok := t.group[t2.Key()]
	if ok {
		tmp := (*e).(GroupTAPIN)
		if t2.records == 0 {
			tmp.records++
		} else {
			tmp.records = tmp.records + t2.records
		}
	} else {
		if t2.records == 0 {
			t2.records = 1
		}
		t2.oper_date = time.Now().Local()
		var e Event = t2
		t.group[t2.Key()] = &e
	}
	return
}
