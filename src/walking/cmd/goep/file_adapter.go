package main

import (
	"fmt"
	"time"
	"net/http"
	"strconv"
	"sync"
	gometrics "github.com/rcrowley/go-metrics"
	"github.com/ethereum/go-ethereum/log"
	logext "github.com/inconshreveable/log15/ext"
	"walking/file"
	"os"
	"errors"
	"strings"
	"path/filepath"
	"walking/cdr"
)

var mu4fa sync.Mutex

func FileAdapterFn(w http.ResponseWriter, r *http.Request, svr *Server) {
	rp := r.URL.Query().Get("root_path")
	op := r.URL.Query().Get("output_path")
	rl_ := r.URL.Query().Get("reject_limit")
	of_ := r.URL.Query().Get("open_files")
	wc_ := r.URL.Query().Get("worker_count")

	if rp == "" {
		rp = "."
	}
	if op == "" {
		op = fmt.Sprintf("%s%c..%coutput", rp, os.PathSeparator, os.PathSeparator)
	}

	rl, err := strconv.Atoi(rl_)
	if err != nil {
		w.WriteHeader(http.StatusNotFound) //	404
		fmt.Fprintf(w, "strconv.Atoi(%s) failure:%v", rl_, err)
		return
	}
	of, err := strconv.Atoi(of_)
	if err != nil {
		of = 4
	}
	wc, err := strconv.Atoi(wc_)
	if err != nil {
		wc = 4
	}

	mc_ := r.URL.Query().Get("max_count_per_file")
	fc_ := r.URL.Query().Get("flush_cnt_per_file")
	md_ := r.URL.Query().Get("max_delay_per_file")
	mc, err := strconv.Atoi(mc_)
	if err != nil {
		mc = 1000000 // 0.5GB = 0.5K * 1024 * 1024
	}
	fc, err := strconv.Atoi(fc_)
	if err != nil {
		fc = 100000 // 50MB = 0.5K * 1024 * 100
	}
	md, err := strconv.Atoi(md_)
	if err != nil {
		md = 60 // sec
	}

	currentTime := time.Now().Local()
	//format Time, string type
	sysdate := currentTime.Format("20060102150405") // ("2006-01-02 15:04:05.000")
	// jobId := fmt.Sprintf("%s-%s-%s", r.URL.Path, rp, sysdate)
	jobId := fmt.Sprintf("%s-%s", r.URL.Path, sysdate)
	mu4fa.Lock() // writers lock
	defer mu4fa.Unlock()
	t := NewFileAdapter(svr, jobId, rp, fmt.Sprintf("%s%c%s%c", op, os.PathSeparator,
		strings.Replace(strings.Replace(jobId, "\\", "_", -1), "/", "_", -1), os.PathSeparator),
		rl, of, wc, mc, fc, md)
	if !t.IsRunning() { // maybe submit multi-times for jobId's sysdate
		t.loadCnt, t.doneCnt = MeterRegister(t.jobId)

		t.log.Info("go routine run", "r.URL.Path", r.URL.Path, "root_path", rp, "reject_limit", rl,
			"open_files", of, rp, "worker_count", wc)
		// fmt.Fprintf(w, "jobId=%s, r.URL.Path=%v, root_path=%s, reject_limit=%d", jobId, r.URL.Path, pf, rl)

		go t.Run()
	}
	fmt.Fprintf(w, Meter(jobId))
}

type EventChannel chan *cdr.Event // interface{}
// type ErrorChannel chan string
type OutputEventChannel chan map[string][]*cdr.Event

const (
	Cap4Channel = 256 // byte
)

type FileAdapter struct {
	svr                               *Server
	jobId, rootPath, outputPath       string
	rejectLimit, openFiles, workerCnt int
	loadCnt, doneCnt                  gometrics.Counter

	evts    EventChannel
	outEvts OutputEventChannel
	// errs, outErrs ErrorChannel

	maxCountPerFile, flushCntPerFile int
	maxDelayPerFile                  time.Duration

	log log.Logger // Contextual logger tracking the database path
}

func NewFileAdapter(svr *Server, jobId, rootPath, outputPath string, rejectLimit, openFiles, workerCnt int,
	maxCountPerFile, flushCntPerFile, maxDelayPerFile int) (t *FileAdapter) {
	t = &FileAdapter{
		svr:        svr,
		jobId:      jobId,
		rootPath:   rootPath,
		outputPath: outputPath,

		rejectLimit: rejectLimit,
		openFiles:   openFiles,
		workerCnt:   workerCnt,

		loadCnt: nil, // metrics.NewCounter(MeterName4Load(jobId)),
		doneCnt: nil, // metrics.NewCounter(MeterName4Done(jobId)),

		evts:    make(EventChannel, Cap4Channel),
		outEvts: make(OutputEventChannel, Cap4Channel),
		//errs:    make(ErrorChannel, Cap4Channel),
		//outErrs: make(ErrorChannel, Cap4Channel),
		maxCountPerFile: maxCountPerFile,
		flushCntPerFile: flushCntPerFile,
		maxDelayPerFile: time.Duration(maxDelayPerFile) * time.Second,

		log: log.New("rid", logext.RandId(8), "jobId", jobId, "rootPath", rootPath,
			"rejectLimit", rejectLimit, "openFiles", openFiles, "workerCnt", workerCnt,
			"loadCnt", log.Lazy{func() int64 {
				if t.loadCnt == nil {
					return -1
				} else {
					return t.loadCnt.Count()
				}
			}},
			"doneCnt", log.Lazy{func() int64 {
				if t.doneCnt == nil {
					return -1
				} else {
					return t.doneCnt.Count()
				}
			}},
			"len4evts", log.Lazy{func() int {
				return len(t.evts)
			}},
			"len4outEvts", log.Lazy{func() int {
				return len(t.outEvts)
			}},
		),
	}
	return
}

func (t *FileAdapter) IsRunning() bool {
	return MeterIsRegister(t.jobId)
}

//type NewEvent func(line []byte, pathFile string, linCnt int) (evt *cdr.Event, row4err string)
//func NewString(line []byte, pathFile string, linCnt int) (evt *cdr.Event, row4err string) {
//	return nil, ""
//}

//func (t *FileAdapter) Run() {
//	file.Walk(t.rootPath, "*", func(pathFile string) {
//		suffix := file.Suffix(pathFile, '.')
//		if evtFn, ok := fileSuffixAdapter[suffix]; ok {
//		}
//	})
//}

func (t *FileAdapter) Run() {
	// go语言圣经#321 #327 #331 // 控制打开文件数
	//	sema is a counting semaphore for limiting concurrency in file.WalkFn.
	var sema = make(chan struct{}, t.openFiles)
	var wg4file sync.WaitGroup // go语言圣经#316
	sizes := make(chan int64)
	file.Walk(t.rootPath, "*", func(pathFile string) {
		evt := t.Adapte(pathFile)
		if evt != nil {
			sema <- struct{}{}        //	acquire	token
			defer func() { <-sema }() //	release	token

			wg4file.Add(1)
			t.loadCnt.Inc(1)

			go func(pathFile string, argEvt *cdr.Event) { // map
				defer wg4file.Done()

				errPathFile := fmt.Sprintf("%s%s.err", t.outputPath, filepath.Base(pathFile))
				w := file.NewWriter(errPathFile, t.maxCountPerFile, t.flushCntPerFile, t.maxDelayPerFile)
				t.log.Info("new error file writer", "errWriter", *w)
				go w.Run()
				defer w.Close()

				t.log.Info("begin process", "pathFile", pathFile)
				file.ReadLine(pathFile, t.rejectLimit, func(line []byte, pathFile string, linCnt, errCnt int) (err error) {
					evt, row4err := (*argEvt).NewEvent(line, pathFile, linCnt)

					if row4err != "" {
						//t.errs <- row4err
						// err = fmt.Errorf(row4err)
						w.Write(row4err)
						err = errors.New(row4err)
						return
					}
					t.evts <- evt
					return
				})
				t.log.Info("end process", "pathFile", pathFile)

				info, _ := os.Stat(pathFile) //	OK to ignore error
				sizes <- info.Size()
				t.doneCnt.Inc(1)
			}(pathFile, evt)
		} else {
			t.log.Info("unsupported file", "pathFile", pathFile)
		}
	})
	//	closer
	go func() {
		wg4file.Wait()
		close(sizes)
		close(t.evts)
		//close(t.errs)
		// close(t.outEvts)
		//close(t.outErrs)
	}()

	var wg4worker sync.WaitGroup // go语言圣经#316
	for i := 0; i < t.workerCnt; i++ {
		go func() { // reduce
			wg4worker.Add(1)
			var evts cdr.Event
			for {
				select {
				case evt, ok := <-t.evts: // ok is true, evt == nil then evts.Reduce output all cache
					// fmt.Printf("%v, %v := <-t.evts:", evt, ok)
					if evts != nil {
						if oe := evts.Reduce(evt); oe != nil || len(oe) > 0 {
							t.outEvts <- oe
						}
					} else {
						if evt != nil {
							evts = *evt
							if oe := evts.Reduce(evt); oe != nil {
								t.outEvts <- oe
							}
						} else {
							t.log.Warn("evts channel received nil and handler is nil")
						}
					}
					if !ok { // job is over!
						wg4worker.Done()
						return
					}
					//case err, ok := <-t.errs:
					//	if !ok {
					//		return
					//	}
				}
			}
		}()
	}
	//	closer
	go func() {
		wg4worker.Wait()
		close(t.outEvts)
	}()
	go func() { // output
		wm := make(map[string]*file.Writer)
		defer func() {
			for _, w := range wm {
				w.Close()
			}
		}()

		for {
			select {
			case oe, ok := <-t.outEvts:
				if !ok {
					return
				}
				for k, v := range oe {
					if len(v) < 1 {
						continue
					}
					var w *file.Writer
					if w_, ok := wm[k]; !ok {
						// pathFile = pathFile - t.rootPath + t.outputPath + reflect.ValueOf(argEvt).Type().String()
						// errPathFile := strings.Replace(pathFile, t.rootPath, t.outputPath, 1) + reflect.ValueOf(*argEvt).Type().String()
						// outputPathFile := fmt.Sprintf("%s%s.dsv", t.outputPath, reflect.ValueOf(v[0]).Type().String())
						outputPathFile := fmt.Sprintf("%s%s.dsv", t.outputPath, k)
						w = file.NewWriter(outputPathFile, t.maxCountPerFile, t.flushCntPerFile, t.maxDelayPerFile)
						t.log.Info("new output file writer", "writer", *w)
						go w.Run()
						wm[k] = w
					} else {
						w = w_
					}
					for _, r := range v {
						w.Write((*r).(cdr.Event).ToDsv())
					}
				}
				//case err, ok := <-t.outErrs:
				//	if !ok {
				//		return
				//	}
			}
		}

	}()

	var total int64
	for size := range sizes {
		total += size
	}
	t.log.Info("process finish", "total.MB", float64(total)/1024/1024)
	// time.Sleep(time.Second * 30)
	MeterUnregister(t.jobId) // /debug/metrics /debug/vars can visible for expvar!
}

//var fileSuffixAdapter = map[string]NewEvent{
//	"HP":   NewString,
//	"HR":   NewString,
//	"JK":   NewString,
//	"PB":   NewString,
//	"RJ":   NewString,
//	"ROAM": NewString,
//	"UPE":  NewString,
//	"UPW":  NewString,
//	"00":   NewString,
//}
var pathFileAdapter = map[string]*cdr.Event{
}

func (t *FileAdapter) Adapte(pathFile string) (evt *cdr.Event) {
	suffix := file.Suffix(pathFile, '.')
	if suffix == "00" {
		t := &cdr.TAPIN{}
		var e cdr.Event = t
		return &e
	}
	return nil
}

/*
create table txe_inputkpi_record (
   circle_id            varchar(20)          null,
   file_type            varchar(4)           null,
   record_type          varchar(4)           null,
   filename             varchar(41)          null,
   OriginatingCircle    varchar(6)           null,
   Subscription_Type    varchar(3)           null,
   records              numeric(10)          null,
   oper_date            timestamp            null,
   service_time         timestamp            null,
   constraint PK_TXE_INPUTKPI_RECORD primary key ()
);
当前文件存放目录为：
├─DWH shared from ZTE mediation	文件均已[ZTE__ShimlaMSS1_MSZSHM10117092912055374.dat__DWH.]开头，后缀分别有：HP,HR,JK,PB,RJ,ROAM,UPE,UPW
└─ZTE_Tracia from BSNL			无文件
	├─Circle CDR					文件存有[ZTE__KanpurMSS_MSZKNP10117110803190806.dat.00.00.00.HP]
	├─INMA_CDR					文件存有[FTP_MeerutMSS2_FT772783.171113_0821.00.00.IM]
	├─Roam_CDR					文件存有[FTP_VaransMSS5_FF133820.171113_0801.00.00.00.ROAM]
	└─TAPIN_CDR					文件存有[CDINDWBINDUE18343.00]

以上文件均单行长度为297字符

circle_id	文件后缀名，当前支持：[HP(Circle CDR),HR,JK,PB,RJ,ROAM(Roam_CDR),UPE,UPW],IM(INMA_CDR),00(TAPIN_CDR)
file_type	为数据文件存放目录编码信息
 */
type Txe_inputkpi_record struct {
	Circle_id, File_type, Record_type, Filename, OriginatingCircle, Subscription_Type string
	Records                                                                           int64
	Oper_date, Service_time                                                           time.Time
}

func newTxe_inputkpi_record(pathFile string, linCnt int, line []byte) (evt interface{}, row4err string) {
	ln := len(line)
	row := string(line)
	cols := strings.Split(row, ",")
	cnt := len(cols)

	if ln != 296 || cnt != 20 {
		row4err = fmt.Sprintf("%s->%d,%d", row, ln, cnt)
	}
	t := &Txe_inputkpi_record{}
	return t, row4err
}
