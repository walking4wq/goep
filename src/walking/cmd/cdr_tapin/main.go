package main

import (
	"flag"
	"fmt"
	"strings"
	"os"
	"io/ioutil"
	"log"
	"bufio"
	"io"
)

// var port = flag.Uint("port", 9527, "network port, default is 9527")
var path = flag.String("path", ".", "cdr_tapin filepath, default is [.]")
var maxErrLine = flag.Int("rj", 3, "File reject limit, default is 3")
// var v = flag.Bool(metrics.MetricsEnabledFlag, false, "export metrics to http://localhost:port+1/debug/metrics")

func main() {
	fmt.Println("args:" + strings.Join(os.Args[1:], ","))
	flag.Parse()
	fmt.Println("args parsed:" + strings.Join(flag.Args(), ","))
	i := 0
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("%d>%v\n", i, f)
		i++
	})
	fileInfo, err := ioutil.ReadDir(*path)
	if err != nil {
		log.Fatalf("ioutil.ReadDir(%s) failure:%v\n", *path, err)
	} else {
		log.Printf("ioutil.ReadDir(%s) fetch fileInfo:%d\n", *path, len(fileInfo))
	}
	// os.Chdir(*path)
	for _, fi := range fileInfo {
		if fi.IsDir() {
			log.Printf("Unsupport to process child dir:%s\n", fi.Name())
			continue
		}
		read(*path, fi, *maxErrLine)
	}
}

// https://www.douban.com/note/637051973/
func read(path string, fi os.FileInfo, maxErrLine int) {
	log.Printf("Start process file:%v\n", fi)
	name := fmt.Sprintf("%s%c%s", path, os.PathSeparator, fi.Name())
	file, err := os.Open(name)
	if err != nil {
		log.Printf("Open file[%s] failure:%v\n", name, err)
		return
	}
	defer file.Close()

	donePath := fmt.Sprintf("%s%cdone", path, os.PathSeparator)
	err = os.MkdirAll(donePath, 0666)
	if err != nil {
		log.Printf("Open write path[%s] failure:%v\n", donePath, err)
		return
	}
	done := fmt.Sprintf("%s%c%s", donePath, os.PathSeparator, fi.Name())
	wf, err := os.Create(done)
	if err != nil {
		log.Printf("Open write file[%s] failure:%v\n", done, err)
		return
	}
	defer wf.Close()
	errFile := fmt.Sprintf("%s%c%s.err", donePath, os.PathSeparator, fi.Name())
	ef, err := os.Create(errFile)
	if err != nil {
		log.Printf("Open write err file[%s] failure:%v\n", errFile, err)
		return
	}
	defer ef.Close()

	r := bufio.NewReader(file)
	errCnt := 0
	linCnt := 0
	w := bufio.NewWriter(wf)
	e := bufio.NewWriter(ef)
	defer w.Flush()
	defer e.Flush()

	for ; true; linCnt++ {
		line, err := readLine(r)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Process file[%s]:%d failure:%v\n", name, linCnt, err)
			errCnt++
			if errCnt > maxErrLine {
				break
			} else {
				continue
			}
		}
		// log.Printf("Start process file[%s]:%d[%s]\n", name, linCnt, string(line))
		err = parse(fi.Name(), linCnt, line, w)
		if err != nil {
			// log.Printf("Process file[%s]:%d[%s] failure:%v\n", name, linCnt, string(line), err)
			if _, err = e.Write(append(line, '\n')); err != nil {
				log.Printf("Process file[%s]:%d[%s] failure and write error file:%v\n",
					name, linCnt, string(line), err)
			}
			errCnt++
			if errCnt > maxErrLine {
				break
			} else {
				continue
			}
		}
		if linCnt%10000 == 0 {
			if err = w.Flush(); err != nil {
				log.Printf("Process file[%s]:%d flush to[%s] failure:%v\n", name, linCnt, done, err)
			}
		}
	}
	log.Printf("End process file[%d-%d]:%v\n", linCnt, errCnt, fi)
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, isprefix, err := r.ReadLine() // bufio.ScanLines
	for isprefix && err == nil {
		var bs []byte
		bs, isprefix, err = r.ReadLine()
		line = append(line, bs...)
	}
	return line, err
}

func parse(name string, linCnt int, line []byte, w *bufio.Writer) error {
	// log.Printf("Start process file[%s]:%d[%d/%d][%s]\n", name, linCnt, *errCnt, maxErrLine, string(line))
	evt, err := parseEvent(line)
	if err != nil {
		return err
	}
	evt.cdr_file = name

	_, err = w.WriteString(evt.String() + "\n")

	return err
}

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
type Event struct {
	record_type, cdr_file, imsi, imei, point_origin, point_target,
	trans_dt, primary_units, searchno, originating_circle string // []byte
}

func (evt Event) String() string {
	return evt.record_type + "," + evt.cdr_file + "," + evt.imsi + "," + evt.imei + "," + evt.point_origin + "," +
		evt.point_target + "," + evt.trans_dt + "," + evt.primary_units + "," + evt.searchno + "," + evt.originating_circle
}

func parseEvent(data []byte) (evt *Event, err error) {
	size := len(data)
	if size < 3 {
		return nil, fmt.Errorf("Too short[%d] for parse to event", size)
	}
	var record_type, cdr_file, imsi, imei, point_origin, point_target,
	trans_dt, primary_units, searchno, originating_circle string

	record_type = string(data[0:3])
	meta := map[string]int{"MT3": 397, "MO3": 224, "MOC": 296}
	maxSize, ok := meta[record_type]
	if !ok {
		return nil, fmt.Errorf("Parse record_type[%s] not found in:%v", record_type, meta)
	}
	if size < maxSize {
		return nil, fmt.Errorf("Parse record_type[%s] size[%d] to short, want to %d",
			record_type, size, maxSize)
	}
	if record_type == "MT3" {
		imsi = string(data[26-1:40])
		imei = string(data[288-1:303])
		point_origin = string(data[377-1:397])
		point_target = string(data[199-1:211])
		trans_dt = string(data[104-1:117])
		primary_units = string(data[149-1:153])
		searchno = string(data[199-1:211])
		originating_circle = string(data[41-1:45])
	} else if record_type == "MO3" {
		imsi = string(data[26-1:40])
		imei = string(data[301-1:316])
		point_origin = string(data[212-1:224])
		point_target = string(data[41-1:60])
		trans_dt = string(data[117-1:130])
		primary_units = string(data[162-1:166])
		searchno = string(data[212-1:224])
		originating_circle = string(data[62-1:66])
	} else if record_type == "MOC" {
		imsi = string(data[71-1:86])
		imei = string(data[281-1:296])
		point_origin = string(data[29-1:49])
		point_target = string(data[51-1:70])
		trans_dt = string(data[127-1:140])
		primary_units = string(data[141-1:146])
		searchno = string(data[29-1:49])
		originating_circle = "INDDL"
	}
	evt = &Event{record_type, cdr_file, imsi, imei, point_origin, point_target,
		trans_dt, primary_units, searchno, originating_circle}
	return evt, nil
}
