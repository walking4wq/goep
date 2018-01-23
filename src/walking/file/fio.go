package file

import (
	"os"
	"bufio"
	"io"
	"time"
	"github.com/ethereum/go-ethereum/log"
	logext "github.com/inconshreveable/log15/ext"
	"strings"
	"fmt"
	"path/filepath"
	"sync/atomic"
)

//var fioLog_ log.Logger
//
//func init() {
//	fioLog_ = log.New("fio.rid", logext.RandId(8))
//	if log_ == nil { log.New("fio.rid", logext.RandId(8)) }
//}

type LineFn func(line []byte, pathFile string, linCnt, errCnt int) error

func LinePrint(line []byte, pathFile string, linCnt, errCnt int) error {
	log_.Info("call LinePrint", "line", string(line), "pathFile", pathFile,
		"linCnt", linCnt, "errCnt", errCnt)
	return nil
}

// http://blog.csdn.net/learning_oracle_lh/article/details/50484802
func ReadLine(pathFile string, rejectLimit int, lf LineFn) (linCnt, errCnt int) {
	log_.Info("Start process file", "pathFile", pathFile, "rejectLimit", rejectLimit)
	file, err := os.Open(pathFile)
	if err != nil {
		log_.Error("ReadLine open file failure", "pathFile", pathFile, "err", err)
		return
	}
	defer file.Close()
	//log_.Info("os.Open", "pathFile", pathFile, "file.Name", file.Name(),
	//	"filepath.Dir", filepath.Dir(file.Name()), "filepath.Base", filepath.Base(file.Name()), "file", *file)
	r := bufio.NewReader(file)
	// linCnt, errCnt := 0, 0
	linCnt, errCnt = 1, 0
	reject := false
	rl := rejectLimit
	if rejectLimit < 0 { // <0 will break, >=0 will not continue print log
		rl = -rejectLimit
		reject = true
	}
	for ; true; linCnt++ {
		line, isPrefix, err := r.ReadLine() // bufio.ScanLines
		for isPrefix && err == nil {
			var bs []byte
			bs, isPrefix, err = r.ReadLine()
			line = append(line, bs...)
		}
		// line, err := readLine(r)
		if err == io.EOF {
			break
		} else if err != nil {
			errCnt++
			if rl != 0 && errCnt > rl {
				if reject {
					break
				}
			} else {
				log_.Error("ReadLine read failure", "pathFile", pathFile, "rejectLimit", rejectLimit,
					"linCnt", linCnt, "errCnt", errCnt, "err", err)
			}
			continue
		}
		// log.Printf("Start process file[%s]:%d[%s]\n", name, linCnt, string(line))
		if lf == nil {
			continue
		}
		err = lf(line, pathFile, linCnt, errCnt)
		if err != nil {
			errCnt++
			if rl != 0 && errCnt > rl {
				if reject {
					break
				}
			} else {
				log_.Error("ReadLine line failure", "pathFile", pathFile, "rejectLimit", rejectLimit,
					"linCnt", linCnt, "errCnt", errCnt, "err", err)
			}
			// continue
		}
	}
	linCnt--
	log_.Info("End process file", "pathFile", pathFile, "rejectLimit", rejectLimit,
		"linCnt", linCnt, "errCnt", errCnt)
	return
}

type WriteDone func(string)
type Writer struct {
	maxCount, flushCnt int
	currLine           uint64
	maxDelay           time.Duration
	pathFile           string
	outputLine         chan string
	writeDone          WriteDone
	log                log.Logger
}

const (
	Cap4Channel = 1024 * 1024 // byte
)

func NewWriter(pathFile string, maxCount, flushCnt int, maxDelay time.Duration, writeDone WriteDone) (w *Writer) {
	w = &Writer{
		maxCount:   maxCount,
		flushCnt:   flushCnt,
		maxDelay:   maxDelay,
		pathFile:   pathFile,
		outputLine: make(chan (string), Cap4Channel),
		writeDone:  writeDone,

		log: log.New("rid", logext.RandId(8), "maxCount", maxCount, "flushCnt", flushCnt,
			"maxDelay", maxDelay,
			"pathFile", log.Lazy{func() string {
				return w.pathFile
			}},
			"currLine", log.Lazy{func() uint64 {
				return atomic.LoadUint64(&w.currLine)
			}},
			"len4outputLine", log.Lazy{func() int {
				return len(w.outputLine)
			}}, "writeDone", writeDone,
		),
	}
	return
}
func (fw *Writer) Run() {
	defer func() {
		if fw.writeDone != nil {
			fw.writeDone("")
		}
	}()
	c := time.Tick(fw.maxDelay)
	var pathFile string
	var wf *os.File
	var wfb *bufio.Writer
	fileSeq, lineCnt := 0, 0
	for {
		select {
		case line, ok := <-fw.outputLine:
			if !ok {
				if wf != nil {
					if err := wfb.Flush(); err != nil {
						fw.log.Error("ending flush file failure", "err", err)
					}
					if err := wf.Close(); err != nil {
						fw.log.Error("ending close file failure", "err", err)
					}
					// wf = nil
					if fw.writeDone != nil {
						fw.writeDone(pathFile)
					}
				}
				goto endOfLoop // http://www.runoob.com/go/go-goto-statement.html
			}
			atomic.AddUint64(&fw.currLine, 1)
			if wf == nil {
				pf, newWf, err := newFile(fw.pathFile, fileSeq)
				if err != nil {
					fw.log.Error("new file failure", "err", err)
					wf = nil
					break
				}
				wfb = bufio.NewWriter(newWf)
				fileSeq++
				lineCnt = 0

				pathFile = pf
				wf = newWf
				fw.log.Info("new file for output", "pathFile", wf.Name())
			}
			lineCnt++
			if lineCnt != 1 {
				wfb.WriteByte('\n')
			}
			wfb.WriteString(line)
			if lineCnt >= fw.maxCount {
				fw.log.Info("too many lines close file for output", "pathFile", wf.Name(), "lineCnt", lineCnt)
				if err := wfb.Flush(); err != nil {
					// wf.Name()
					fw.log.Warn("too many lines flush file failure",
						"lineCnt", lineCnt, "err", err)
					break
				}
				if err := wf.Close(); err != nil {
					fw.log.Warn("too many lines close file failure",
						"lineCnt", lineCnt, "err", err)
					break
				}
				wf = nil
				if fw.writeDone != nil {
					fw.writeDone(pathFile)
				}
			} else if lineCnt%fw.flushCnt == 0 {
				if err := wfb.Flush(); err != nil {
					fw.log.Warn("flush file failure", "lineCnt", lineCnt, "err", err)
					break
				}
			}
		case _ = <-c:
			if wf != nil {
				fw.log.Info("tick timeout close file for output", "pathFile", wf.Name(), "lineCnt", lineCnt)
				if err := wfb.Flush(); err != nil {
					fw.log.Warn("so long delay flush file failure", "err", err)
					break
				}
				if err := wf.Close(); err != nil {
					fw.log.Warn("so long delay close file failure", "err", err)
					break
				}
				wf = nil
				if fw.writeDone != nil {
					fw.writeDone(pathFile)
				}
			} else {
				fw.log.Trace("tick timeout and current output file is nil")
			}
		}
	}
endOfLoop:
}
func (fw *Writer) Write(line string) {
	fw.outputLine <- line
}
func (fw *Writer) Close() {
	close(fw.outputLine)
}

func newFile(path string, fileSeq int) (pathFile string, f *os.File, err error) {
	suffix := strings.LastIndexByte(path, '.')
	if suffix != -1 {
		pathFile = fmt.Sprintf("%s.%d%s", path[0:suffix], fileSeq, path[suffix:])
	}
	err = os.MkdirAll(filepath.Dir(pathFile), 0777) // 0666)
	if err != nil {
		return
	}
	f, err = os.Create(pathFile)
	return
}
