package main

import (
	"fmt"
	"time"
	"net/http"
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
	"hash/crc32"
	"github.com/go-sql-driver/mysql"
	"database/sql"
	"bytes"
	"strconv"
)

var mu4fa sync.Mutex

func FileAdapterFn(w http.ResponseWriter, r *http.Request, svr *Server) {
	params := r.URL.Query()

	log.Info("FileAdapterFn call", "r.URL.Query()", log.Lazy{func() string {
		var buffer bytes.Buffer // http://blog.csdn.net/u012210379/article/details/45110705
		for k, v := range params {
			buffer.WriteString(fmt.Sprintf("%s=[", k))
			for i, s := range v {
				if i != 0 {
					buffer.WriteByte(',')
				}
				buffer.WriteString(s)
			}
			buffer.WriteString("]:")
		}
		return buffer.String()
	}})
	rp := params.Get("root_path")
	op_ := params.Get("output_path")
	et := params.Get("event_type")
	rl_ := params.Get("reject_limit")
	of_ := params.Get("open_files")
	mt_ := params.Get("map_tasks")
	rt_ := params.Get("reduce_tasks")
	ot_ := params.Get("output_tasks")

	hp := params.Get("his_sub_path")
	wp := params.Get("warn_sub_path")
	dns4mysql := params.Get("mysql_dns")
	tab4mysql := params.Get("mysql_tab")
	lt4mysql_ := params.Get("mysql_load_tasks")

	tab4mysql = strings.Trim(tab4mysql, " ")

	currAbsPath, err := file.GetCurrentAbsPath()
	if err != nil {
		w.WriteHeader(200) // http.StatusInternalServerError 500
		fmt.Fprintf(w, "GetCurrentAbsPath[%s] err:%v", os.Args[0], err)
		return
	}
	contains, err := file.Contains(rp, currAbsPath, true)
	if err != nil {
		w.WriteHeader(200) // http.StatusInternalServerError 500
		fmt.Fprintf(w, "root_path[%s] invalid:%v", rp, err)
		return
	}
	if contains {
		w.WriteHeader(200) // http.StatusInternalServerError 500
		fmt.Fprintf(w, "root_path[%s] contains program execution directory[%s]", rp, currAbsPath)
		return
	}

	op := op_
	if op == "" {
		op = fmt.Sprintf("%s%c..%coutput", rp, os.PathSeparator, os.PathSeparator)
	}

	rl, err := strconv.Atoi(rl_)
	if err != nil {
		w.WriteHeader(200) // http.StatusNotFound 404
		fmt.Fprintf(w, "strconv.Atoi(%s) failure:%v", rl_, err)
		return
	}
	of, err := strconv.Atoi(of_)
	if err != nil {
		of = 6
	}
	mt, err := strconv.Atoi(mt_)
	if err != nil {
		mt = 4
	}
	rt, err := strconv.Atoi(rt_)
	if err != nil {
		rt = 3
	}
	ot, err := strconv.Atoi(ot_)
	if err != nil {
		ot = 2
	}
	lt4mysql, err := strconv.Atoi(lt4mysql_)
	if err != nil {
		lt4mysql = 2
	}

	mc_ := params.Get("max_count_per_file")
	fc_ := params.Get("flush_cnt_per_file")
	md_ := params.Get("max_delay_per_file")
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
	outputPath := fmt.Sprintf("%s%c%s%c", op, os.PathSeparator,
		strings.Replace(strings.Replace(jobId, "\\", "_", -1), "/", "_", -1), os.PathSeparator)
	outputPath, err = filepath.Abs(outputPath)
	if err != nil {
		w.WriteHeader(200) // http.StatusInternalServerError 500
		fmt.Fprintf(w, "output_path[%s]->[%s] filepath.Abs[%s] err:%v", op_, op, outputPath, err)
		return
	}
	outputPath = fmt.Sprintf("%s%c", outputPath, os.PathSeparator)
	err = os.MkdirAll(outputPath, 0777) // 0666)
	if err != nil {
		w.WriteHeader(200) // http.StatusInternalServerError 500
		fmt.Fprintf(w, "output_path[%s]->[%s] mkdir[%s] err:%v", op_, op, outputPath, err)
		return
	}
	t := NewFileAdapter(svr, jobId, rp, outputPath, et, rl, of, mt, rt, ot, mc, fc, md, hp, wp, dns4mysql, tab4mysql, lt4mysql)
	if !t.IsRunning() { // maybe submit multi-times for jobId's sysdate
		t.loadCnt, t.doneCnt = MeterRegister(t.jobId)

		t.log.Info("go routine run", "r.URL.Path", r.URL.Path, "root_path", rp, "output_path", op,
			"event_type", et, "reject_limit", rl, "open_files", of, "map_tasks", mt, "reduce_tasks", rt,
			"output_tasks", ot, "mysql_dns", dns4mysql, "mysql_tab", tab4mysql, "mysql_load_tasks", lt4mysql)
		// fmt.Fprintf(w, "jobId=%s, r.URL.Path=%v, root_path=%s, reject_limit=%d", jobId, r.URL.Path, pf, rl)

		go t.Run()
	}
	fmt.Fprintf(w, Meter(jobId))
}

type EventChannel chan *cdr.Event // interface{}
// type ErrorChannel chan string
// type GroupEventChannel chan map[string]*cdr.Event // chan map[string][]*cdr.Event

const (
	Cap4Channel = 256 // byte
)

type FileAdapter struct {
	svr                                                        *Server
	jobId, rootPath, outputPath, eventType                     string
	rejectLimit, openFiles, mapTasks, reduceTasks, outputTasks int
	hisSubPath, warnSubPath, dns4mysql, tab4mysql              string
	lt4mysql                                                   int

	loadCnt, doneCnt gometrics.Counter

	fileIn, selOut, grpOut EventChannel
	// grpOut         GroupEventChannel
	// errs, outErrs ErrorChannel

	maxCountPerFile, flushCntPerFile int
	maxDelayPerFile                  time.Duration

	log log.Logger // Contextual logger tracking the database path
}

func NewFileAdapter(svr *Server, jobId, rootPath, outputPath, eventType string, rejectLimit, openFiles int,
	mapTasks, reduceTasks, outputTasks, maxCountPerFile, flushCntPerFile, maxDelayPerFile int,
	hisSubPath, warnSubPath, dns4mysql, tab4mysql string, lt4mysql int) (t *FileAdapter) {
	t = &FileAdapter{
		svr:        svr,
		jobId:      jobId,
		rootPath:   rootPath,
		outputPath: outputPath,
		eventType:  eventType,

		rejectLimit: rejectLimit,
		openFiles:   openFiles,
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		outputTasks: outputTasks,

		hisSubPath:  hisSubPath,
		warnSubPath: warnSubPath,
		dns4mysql:   dns4mysql,
		tab4mysql:   tab4mysql,
		lt4mysql:    lt4mysql,

		loadCnt: nil, // metrics.NewCounter(MeterName4Load(jobId)),
		doneCnt: nil, // metrics.NewCounter(MeterName4Done(jobId)),

		fileIn: make(EventChannel, Cap4Channel),
		selOut: make(EventChannel, Cap4Channel),
		// grpOut: make(GroupEventChannel, Cap4Channel),
		grpOut: make(EventChannel, Cap4Channel),

		//errs:    make(ErrorChannel, Cap4Channel),
		//outErrs: make(ErrorChannel, Cap4Channel),
		maxCountPerFile: maxCountPerFile,
		flushCntPerFile: flushCntPerFile,
		maxDelayPerFile: time.Duration(maxDelayPerFile) * time.Second,

		log: log.New("rid", logext.RandId(8), "jobId", jobId,
			"rootPath", rootPath, "outputPath", outputPath, "eventType", eventType,
			"rejectLimit", rejectLimit, "openFiles", openFiles,
			"mapTasks", mapTasks, "reduceTasks", reduceTasks, "outputTasks", outputTasks,
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
			"len.fileIn", log.Lazy{func() int {
				return len(t.fileIn)
			}},
			"len.selOut", log.Lazy{func() int {
				return len(t.selOut)
			}},
			"len.grpOut", log.Lazy{func() int {
				return len(t.grpOut)
			}}, "hisSubPath", hisSubPath, "warnSubPath", warnSubPath,
			"dns4mysql", dns4mysql, "tab4mysql", tab4mysql, "lt4mysql", lt4mysql,
			"maxCountPerFile", maxCountPerFile, "flushCntPerFile", flushCntPerFile, "maxDelayPerFile", maxDelayPerFile,
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

// func hash4crc32(key string) uint32 {
func hash(key string) int {
	// http://blog.csdn.net/xcl168/article/details/43898807
	return int(crc32.ChecksumIEEE([]byte(key)))
}
func (t *FileAdapter) load2mysql() bool {
	return t.dns4mysql != "" && t.tab4mysql != "" && t.lt4mysql > 0
}
func (t *FileAdapter) Run() {
	errSubDir := "err"
	dataSubDir := "data"

	hisSubDir := t.hisSubPath   // "his"
	warnSubDir := t.warnSubPath // "warn"

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

				errPathFile := fmt.Sprintf("%s%s%c%s.%s", t.outputPath, errSubDir, os.PathSeparator, filepath.Base(pathFile), errSubDir)
				w := file.NewWriter(errPathFile, t.maxCountPerFile, t.flushCntPerFile, t.maxDelayPerFile, nil)
				t.log.Info("new error file writer", "errWriter", *w)
				go w.Run()
				defer w.Close()

				var w4w *file.Writer
				if warnSubDir != "" {
					warnPathFile := fmt.Sprintf("%s%s%c%s.%s", t.outputPath, warnSubDir, os.PathSeparator, filepath.Base(pathFile), warnSubDir)
					w4w = file.NewWriter(warnPathFile, t.maxCountPerFile, t.flushCntPerFile, t.maxDelayPerFile, nil)
					t.log.Info("new warn file writer", "warnWriter", *w4w)
					go w4w.Run()
					defer w4w.Close()
				}

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
					if evt == nil {
						//t.log.Trace("ignore line", "line", string(line), "pathFile", pathFile, "linCnt",
						//	linCnt, "errCnt", errCnt)
						if w4w != nil {
							w4w.Write(fmt.Sprintf("%08d>%s", linCnt, string(line)))
						}
						return
					}
					t.fileIn <- evt
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
		close(t.fileIn)

		if hisSubDir != "" {
			// hisPath := fmt.Sprintf("%s%s%c%s", t.outputPath, hisSubDir, os.PathSeparator, filepath.Base(t.rootPath))
			hisPath := fmt.Sprintf("%s%s", t.outputPath, hisSubDir)
			t.log.Info("try to Rename rootPath to hisPath ...", "rootPath", t.rootPath, "hisPath", hisPath)
			//err := os.MkdirAll(hisPath, 0666)
			//if err != nil {
			//	goto rm
			//}
			// err := os.Rename(t.rootPath, hisPath)
			err := file.BackupDir(t.rootPath, hisPath)
			if err != nil {
				goto rm
			}
			goto outterIfHisSubDir
		rm:
			t.log.Warn("try to Rename rootPath to hisPath failure",
				"rootPath", t.rootPath, "hisPath", hisPath, "err", err)
			//err = os.RemoveAll(t.rootPath) // too dangerous!
			//if err != nil {
			//	t.log.Error("RemoveAll rootPath to hisPath failure", "rootPath", t.rootPath, "err", err)
			//}
		}
	outterIfHisSubDir:

		//close(t.errs)
		// close(t.outEvts)
		//close(t.outErrs)
	}()

	reduceChannel := make([]EventChannel, t.reduceTasks)
	for i := 0; i < t.reduceTasks; i++ {
		reduceChannel[i] = make(EventChannel, Cap4Channel)
	}
	var wg4map sync.WaitGroup // go语言圣经#316
	for i := 0; i < t.mapTasks; i++ { // 多线程Reduce导致出现多个同纬度的结果
		wg4map.Add(1)
		go func(mapTaskIdx int) { // map
			defer wg4map.Done()
			var cache cdr.Event
			//debugHash := make(map[string]int)
			//defer func() {
			//	t.log.Debug("debugHash", "len", len(debugHash), "mapTaskIdx", mapTaskIdx)
			//	for k, v := range debugHash {
			//		t.log.Debug("debugHash", "key", k, "value", v, "mapTaskIdx", mapTaskIdx)
			//	}
			//}()
			for {
				select {
				case evt, ok := <-t.fileIn: // ok is true, evt == nil then evts.Reduce output all cache
					// fmt.Printf("%v, %v := <-t.evts:", evt, ok)
					if ok {
						if cache != nil {
							sel, grp := cache.Reduce(evt)
							if sel != nil {
								t.selOut <- sel
							}
							for k, v := range grp {
								h := hash(k)
								idx := h % t.reduceTasks

								reduceChannel[idx] <- v

								// https://www.jianshu.com/p/9637c18d5f01
								//dbgKey := fmt.Sprintf("[%s]->%d%%%d=%d", k, h, t.reduceTasks, idx)
								//cnt, ok := debugHash[dbgKey]
								//if !ok {
								//	t.log.Debug("find new hash", "key", dbgKey, "mapTaskIdx", mapTaskIdx)
								//	debugHash[dbgKey] = 1
								//} else {
								//	debugHash[dbgKey] = cnt + 1
								//}
							}
							//if grp != nil && len(grp) > 0 {
							//	 t.grpOut <- grp
							//}
						} else { // the first loop
							if evt != nil {
								cache = *evt
								cache.Reduce(nil) // clear cache
								sel, grp := cache.Reduce(evt)
								if sel != nil {
									t.selOut <- sel
								}
								for k, v := range grp {
									h := hash(k)
									idx := h % t.reduceTasks

									reduceChannel[idx] <- v

									// https://www.jianshu.com/p/9637c18d5f01
									//dbgKey := fmt.Sprintf("[%s]->%d%%%d=%d", k, h, t.reduceTasks, idx)
									//cnt, ok := debugHash[dbgKey]
									//if !ok {
									//	t.log.Debug("find new hash", "key", dbgKey, "mapTaskIdx", mapTaskIdx)
									//	debugHash[dbgKey] = 1
									//} else {
									//	debugHash[dbgKey] = cnt + 1
									//}
								}
							} else {
								t.log.Warn("fileIn channel received nil and evt as a handler is nil")
							}
						}
					} else { // job is over!
						if cache != nil {
							sel, grp := cache.Reduce(nil)
							if sel != nil {
								t.selOut <- sel
							}
							for k, v := range grp {
								h := hash(k)
								idx := h % t.reduceTasks

								reduceChannel[idx] <- v

								// https://www.jianshu.com/p/9637c18d5f01
								//dbgKey := fmt.Sprintf("[%s]->%d%%%d=%d", k, h, t.reduceTasks, idx)
								//cnt, ok := debugHash[dbgKey]
								//if !ok {
								//	t.log.Debug("find new hash", "key", dbgKey, "mapTaskIdx", mapTaskIdx)
								//	debugHash[dbgKey] = 1
								//} else {
								//	debugHash[dbgKey] = cnt + 1
								//}
							}
						}
						// wg4map.Done()
						return
					}
					//case err, ok := <-t.errs:
					//	if !ok {
					//		return
					//	}
				}
			}
		}(i)
	}
	//	closer
	go func() {
		wg4map.Wait()
		close(t.selOut)
		// close(t.grpOut)
		for _, rc := range reduceChannel {
			close(rc)
		}
	}()
	var wg4red sync.WaitGroup
	for i := 0; i < t.reduceTasks; i++ {
		wg4red.Add(1)
		go func(reduceTaskIdx int) { // reduce
			defer wg4red.Done()
			var cache cdr.Event
			for {
				select {
				case evt, ok := <-reduceChannel[reduceTaskIdx]:
					// fmt.Printf("%v, %v := <-t.evts:", evt, ok)
					if ok {
						// t.log.Debug("reduceChannel received", "evt", (*evt).ToDsv(), "reduceTaskIdx", reduceTaskIdx)
						if cache != nil {
							sel, grp := cache.Reduce(evt)
							if sel != nil {
								t.selOut <- sel
							}
							for _, v := range grp {
								t.grpOut <- v
							}
						} else { // the first loop
							if evt != nil {
								cache = *evt
								cache.Reduce(nil) // clear cache
								sel, grp := cache.Reduce(evt)
								if sel != nil {
									t.selOut <- sel
								}
								for _, v := range grp {
									t.grpOut <- v
								}
							} else {
								t.log.Warn("reduce channel received nil and evt as a handler is nil", "i", i)
							}
						}
					} else { // job is over!
						if cache != nil {
							sel, grp := cache.Reduce(nil)
							if sel != nil {
								t.selOut <- sel
							}
							for _, v := range grp {
								t.grpOut <- v
								// t.log.Debug("reduce output", "v", (*v).ToDsv(), "reduceTaskIdx", reduceTaskIdx)
							}
						}
						// wg4red.Done()
						return
					}
				}
			}
		}(i)
	}
	//	closer
	go func() {
		wg4red.Wait()
		close(t.grpOut)
	}()
	//go func() { // output
	//	wm := make(map[string]*file.Writer)
	//	defer func() {
	//		for _, w := range wm {
	//			w.Close()
	//		}
	//	}()
	//
	//	for {
	//		select {
	//		case oe, ok := <-t.outEvts:
	//			if !ok {
	//				return
	//			}
	//			for k, v := range oe {
	//				if len(v) < 1 {
	//					continue
	//				}
	//				var w *file.Writer
	//				if w_, ok := wm[k]; !ok { // 使用key(类型)作为文件名称不能与group需求兼容
	//					// pathFile = pathFile - t.rootPath + t.outputPath + reflect.ValueOf(argEvt).Type().String()
	//					// errPathFile := strings.Replace(pathFile, t.rootPath, t.outputPath, 1) + reflect.ValueOf(*argEvt).Type().String()
	//					// outputPathFile := fmt.Sprintf("%s%s.dsv", t.outputPath, reflect.ValueOf(v[0]).Type().String())
	//					outputPathFile := fmt.Sprintf("%s%s%c%s.dsv", t.outputPath, dataSubDir, os.PathSeparator, k)
	//					w = file.NewWriter(outputPathFile, t.maxCountPerFile, t.flushCntPerFile, t.maxDelayPerFile)
	//					t.log.Info("new output file writer", "writer", *w)
	//					go w.Run()
	//					wm[k] = w
	//				} else {
	//					w = w_
	//				}
	//				for _, r := range v {
	//					w.Write((*r).(cdr.Event).ToDsv())
	//				}
	//			}
	//			//case err, ok := <-t.outErrs:
	//			//	if !ok {
	//			//		return
	//			//	}
	//		}
	//	}
	//}()
	wm := make([]file.Writer, t.outputTasks)
	var writeDone chan string
	if t.load2mysql() {
		writeDone = make(chan string, t.outputTasks*t.lt4mysql)
	}
	var wg4done sync.WaitGroup
	for i := 0; i < t.outputTasks; i++ {
		if writeDone != nil {
			wg4done.Add(1)
		}
		// pathFile = pathFile - t.rootPath + t.outputPath + reflect.ValueOf(argEvt).Type().String()
		// errPathFile := strings.Replace(pathFile, t.rootPath, t.outputPath, 1) + reflect.ValueOf(*argEvt).Type().String()
		// outputPathFile := fmt.Sprintf("%s%s.dsv", t.outputPath, reflect.ValueOf(v[0]).Type().String())
		outputPathFile := fmt.Sprintf("%s%s%c%s_%d.dsv", t.outputPath, dataSubDir, os.PathSeparator, t.eventType, i)
		w := file.NewWriter(outputPathFile, t.maxCountPerFile, t.flushCntPerFile, t.maxDelayPerFile, func(pathFile string) {
			t.log.Info("pathFile writer to local file system is done", "pathFile", pathFile)
			if writeDone != nil {
				if pathFile != "" {
					writeDone <- pathFile
				} else {
					wg4done.Done()
				}
			}
		})
		t.log.Info("new output file writer", "writer", *w)
		go w.Run()
		wm[i] = *w
	}
	var wg4out sync.WaitGroup
	for i := 0; i < t.outputTasks; i++ {
		wg4out.Add(1)
		go func() { // output
			defer wg4out.Done()
			idx := 1
			for {
				select {
				case evt, ok := <-t.selOut:
					if !ok {
						return
					}
					wm[idx%t.outputTasks].Write((*evt).(cdr.Event).ToDsv())
					// wm[idx].Write(evt.ToDsv())
					idx++
				}
			}
		}()
	}
	for i := 0; i < t.outputTasks; i++ {
		wg4out.Add(1)
		go func() { // output
			defer wg4out.Done()
			idx := 1
			for {
				select {
				case evt, ok := <-t.grpOut:
					if !ok {
						return
					}
					wm[idx%t.outputTasks].Write((*evt).(cdr.Event).ToDsv())
					idx++
				}
			}
		}()
	}
	//	closer
	go func() {
		wg4out.Wait()
		for _, w := range wm {
			w.Close()
		}
		wg4done.Wait()
		if writeDone != nil { // panic: close of nil channel
			close(writeDone)
		}
	}()
	if writeDone != nil {
		//go func() {
		//	db, err := sql.Open("mysql", t.dns4mysql)
		//	if err != nil {
		//		t.log.Error(`sql.Open("mysql", dns) failure`, "dns", t.dns4mysql, "err", err)
		//		return
		//	}
		//	defer db.Close()
		//	t.log.Info(`sql.Open("mysql", dns) success`, "dns", t.dns4mysql, "db", db)
		for i := 0; i < t.lt4mysql; i++ {
			//wg4out.Add(1)
			go func() { // mysql data load
				//defer wg4out.Done()
				db, err := sql.Open("mysql", t.dns4mysql)
				if err != nil {
					t.log.Error(`sql.Open("mysql", dns) failure`, "dns", t.dns4mysql, "err", err)
					return
				}
				defer db.Close()
				t.log.Info(`sql.Open("mysql", dns) success`, "dns", t.dns4mysql, "db", db)

				var ddl string
				for {
					select {
					case pathFile, ok := <-writeDone:
						if !ok {
							return
						}
						// path, err := filepath.Abs(pathFile)
						absPathFile, err := filepath.Abs(pathFile)
						if err != nil {
							t.log.Error("filepath.Abs(pathFile) failure", "pathFile", pathFile, "err", err)
							continue
						}
						absPathFile = strings.Replace(absPathFile, "\\", "/", -1)
						// absPathFile := filepath.Join(path, filepath.Base(pathFile))
						t.log.Debug("convert the file path to an absolute path", "pathFile", pathFile,
							"absPathFile", absPathFile)
						adapte := t.Adapte(absPathFile) // attention to pathFile is output
						if adapte != nil {
							ddl_, lad_ := (*adapte).(cdr.Event).Sql()
							if ddl_ != "" && ddl != ddl_ {
								ddl = ddl_
								sql := fmt.Sprintf(ddl, t.tab4mysql)
								rst, err := db.Exec(sql)
								if err != nil {
									t.log.Error("db.Exec(ddl) failure", "ddl", sql, "err", err)
									continue
								}
								t.log.Info("db.Exec(ddl) success", "ddl", sql, "rst", rst)
							}
							sql := fmt.Sprintf(lad_, absPathFile, t.tab4mysql)
							mysql.RegisterLocalFile(absPathFile)
							rst, err := db.Exec(sql)
							if err != nil {
								t.log.Error("db.Exec(sql) failure", "sql", sql, "err", err)
								continue
							}
							t.log.Info("db.Exec(sql) success", "sql", sql, "rst", rst)
						} else {
							t.log.Warn("Adapte(absPathFile) is nil", "absPathFile", absPathFile)
						}
					}
				}
			}()
		}
		//}()
	}

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
	"TAPIN": func() (*cdr.Event) {
		t := &cdr.TAPIN{}
		var e cdr.Event = t
		return &e
	}(),
	"GroupMSC": func() (*cdr.Event) {
		t := &cdr.GroupMSC{}
		var e cdr.Event = t
		return &e
	}(),
	"GroupROAM": func() (*cdr.Event) {
		t := &cdr.GroupROAM{}
		var e cdr.Event = t
		return &e
	}(),
	"GroupINMA": func() (*cdr.Event) {
		t := &cdr.GroupINMA{}
		var e cdr.Event = t
		return &e
	}(),
	"GroupTAPIN": func() (*cdr.Event) {
		t := &cdr.GroupTAPIN{}
		var e cdr.Event = t
		return &e
	}(),
}

func (t *FileAdapter) Adapte(pathFile string) (evt *cdr.Event) {
	evt, ok := pathFileAdapter[t.eventType]
	if !ok {
		//	suffix := file.Suffix(pathFile, '.')
		//	if suffix == "00" || true {
		//		t := &cdr.TAPIN{}
		//		var e cdr.Event = t
		//		return &e
		//	}
		return nil
	}
	return
}
