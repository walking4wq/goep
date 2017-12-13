package cdr

import (
	"fmt"
	"reflect"
	"strings"
)

/*
echo "  APPEND INTO TABLE CDR_TAPIN WHEN (1:3) = 'MT3'"
echo "  TRAILING NULLCOLS"
echo "  ("
echo "        record_type POSITION(1:3),"
echo "        CDR_FILE CONSTANT \"${FILE}\","
echo "        imsi POSITION(26:40),"
echo "        imei POSITION(288:303),"
echo "        point_origin POSITION(377:397),"
echo "        point_target POSITION(199:211),"
echo "        trans_dt  POSITION(104:117) DATE \"YYYYMMDDhh24:mi:ss\","
echo "        primary_units POSITION(149:153),"
echo "        searchno POSITION(199:211),"
echo "        originating_circle POSITION(41:45)"
echo ")"
echo "INTO TABLE CDR_TAPIN WHEN (1:3) = 'MO3'"
echo "TRAILING NULLCOLS"
echo "  ("
echo "        record_type POSITION(1:3),"
echo "        CDR_FILE CONSTANT \"${FILE}\" ,"
echo "        imsi POSITION(26:40),"
echo "        imei POSITION(301:316),"
echo "        point_origin POSITION(212:224),"
echo "        point_target POSITION(41:60),"
echo "        trans_dt  POSITION(117:130) DATE \"YYYYMMDDHH24:MI:SS\","
echo "        primary_units POSITION(162:166),"
echo "        searchno POSITION(212:224),"
echo "        originating_circle POSITION(62:66)"
echo ") "
echo "INTO TABLE CDR_TAPIN WHEN (1:3) = 'MOC'"
echo "TRAILING NULLCOLS"
echo "  ("
echo "        record_type POSITION(1:3),"
echo "        CDR_FILE CONSTANT \"${FILE}\" ,"
echo "        imsi POSITION(71:86),"
echo "        imei POSITION(281:296),"
echo "        point_origin POSITION(29:49),"
echo "        point_target POSITION(51:70),"
echo "        trans_dt  POSITION(127:140) DATE \"YYYYMMDDHH24MISS\","
echo "        primary_units POSITION(141:146),"
echo "        searchno POSITION(29:49),"
echo "        originating_circle CONSTANT \"INDDL\""
echo ") "
 */
type TAPIN struct {
	record_type, cdr_file, imsi, imei, point_origin, point_target,
	trans_dt, primary_units, searchno, originating_circle, first_cell_id string // []byte
}

func (_ TAPIN) NewEvent(line []byte, pathFile string, linCnt int) (evt *Event, row4err string) {
	size := len(line)
	if size < 3 {
		return nil, fmt.Sprintf("Too short[%d] for parse to event", size)
	}
	var record_type, cdr_file, imsi, imei, point_origin, point_target,
	trans_dt, primary_units, searchno, originating_circle, first_cell_id string

	record_type = string(line[0:3])
	meta := map[string]int{"MT3": 397, "MO3": 224, "MOC": 296}
	maxSize, ok := meta[record_type]
	if !ok {
		// return nil, fmt.Sprintf("Parse[%d] record_type[%s] not found in:%v", linCnt, record_type, meta)
		return nil, "" // ignore this error
	}
	if size < maxSize {
		return nil, fmt.Sprintf("Parse[%d] record_type[%s] size[%d] to short, want to %d",
			linCnt, record_type, size, maxSize)
	}
	if record_type == "MT3" {
		imsi = string(line[26-1:40])
		imei = string(line[288-1:303])
		point_origin = string(line[377-1:397])
		point_target = string(line[199-1:211])
		trans_dt = string(line[104-1:117])
		primary_units = string(line[149-1:153])
		searchno = string(line[199-1:211])
		originating_circle = string(line[41-1:45])
		first_cell_id = string(line[74-1:83])
	} else if record_type == "MO3" {
		imsi = string(line[26-1:40])
		imei = string(line[301-1:316])
		point_origin = string(line[212-1:224])
		point_target = string(line[41-1:60])
		trans_dt = string(line[117-1:130])
		primary_units = string(line[162-1:166])
		searchno = string(line[212-1:224])
		originating_circle = string(line[62-1:66])
		first_cell_id = string(line[87-1:96])
	} else if record_type == "MOC" {
		imsi = string(line[71-1:86])
		imei = string(line[281-1:296])
		point_origin = string(line[29-1:49])
		point_target = string(line[51-1:70])
		trans_dt = string(line[127-1:140])
		primary_units = string(line[141-1:146])
		searchno = string(line[29-1:49])
		originating_circle = "INDDL"
	}
	cdr_file = pathFile
	t := &TAPIN{record_type, cdr_file, imsi, imei, point_origin, point_target,
		trans_dt, primary_units, searchno, originating_circle, first_cell_id}
	// evt = t
	var e Event = t
	return &e, ""
}
func (t TAPIN) Reduce(evt *Event) (evts map[string][]*Event) { // reflect.Value.Type().String() // reflect.ValueOf(evt)
	if evt == nil { // evt == nil then evts.Reduce output all cache
		return
	}
	evts = make(map[string][]*Event, 1)
	// fmt.Println(reflect.ValueOf(*evt).Type().String())
	// evts[reflect.ValueOf(*evt).Type().String()] = []*Event{
	// evts["TAPIN"] = []*Event{
	// tapin := (*evt).(TAPIN) // panic: interface conversion: cdr.Event is *cdr.TAPIN, not cdr.TAPIN
	tapin := (*evt).(*TAPIN)
	tapin.first_cell_id = strings.Replace(tapin.first_cell_id, " ", "X", -1)
	evts[reflect.ValueOf(t).Type().String()] = []*Event{
		evt,
	}
	return
}
func (evt TAPIN) ToDsv() string {
	return evt.record_type + "," + evt.cdr_file + "," + evt.imsi + "," + evt.imei + "," + evt.point_origin + "," +
		evt.point_target + "," + evt.trans_dt + "," + evt.primary_units + "," + evt.searchno + "," +
		evt.originating_circle + "," + evt.first_cell_id
}
