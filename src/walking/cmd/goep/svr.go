package main

import (
	"net/http"
	logext "github.com/inconshreveable/log15/ext"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	gometrics "github.com/rcrowley/go-metrics"
	"fmt"
)

//type JobLog map[string]string

type Server struct {
	db *ethdb.Database
	//jobs JobLog
	// mu sync.RWMutex

	// channel

	log log.Logger // Contextual logger tracking the database path
}

func NewServer(db ethdb.Database) (svr *Server) {
	svr = &Server{
		db: &db,
		//jobs: make(JobLog),

		log: log.New("svr.rid", logext.RandId(8)),
	}
	return
}

func (s *Server) RegisterHandleFunc() {
	http.HandleFunc("/joblog", s.fn4joblog)

	http.HandleFunc("/file_adapter", func(w http.ResponseWriter, r *http.Request) {
		FileAdapterFn(w, r, s)
	})

}

func MeterName(jobId, kpiName string) string {
	return fmt.Sprintf("goep_%s/%s", jobId, kpiName)
}
func MeterName4Load(jobId string) string {
	return MeterName(jobId, "load")
}
func MeterName4Done(jobId string) string {
	return MeterName(jobId, "done")
}
func MeterIsRegister(jobId string) bool {
	return gometrics.Get(MeterName4Load(jobId)) != nil
}
func Meter(jobId string) string {
	if !MeterIsRegister(jobId) {
		return fmt.Sprintf(`{"job_id":"%s","msg":"Not found job"}`, jobId)
	} else {
		loadCnt, doneCnt := MeterRegister(jobId)
		return fmt.Sprintf(`{"job_id":"%s","load":%d,"done":%d}`, jobId, loadCnt.Count(), doneCnt.Count())
	}
}
func MeterRegister(jobId string) (loadCnt, doneCnt gometrics.Counter) {
	return metrics.NewCounter(MeterName4Load(jobId)), metrics.NewCounter(MeterName4Done(jobId))
}
func MeterUnregister(jobId string) {
	gometrics.Unregister(MeterName4Load(jobId))
	gometrics.Unregister(MeterName4Done(jobId))
}
func (s *Server) fn4joblog(w http.ResponseWriter, r *http.Request) {
	ji := r.URL.Query().Get("job_id")
	fmt.Fprintf(w, Meter(ji))
}

/*
func (s *Server) fn4joblog(w http.ResponseWriter, r *http.Request) {
	ji := r.URL.Query().Get("job_id")

	s.mu.RLock() // readers lock
	defer s.mu.RUnlock()

	if ji == "" || ji == "*" {
		fmt.Fprintf(w, "%v", s.jobs)
	} else if log, ok := s.jobs[ji]; ok {
		fmt.Fprintf(w, "[%s]=[\n%s\n]", ji, log)
	} else {
		fmt.Fprintf(w, "Not found job[%s] log", ji)
	}
}

func (s *Server) jobLog(jobId, jobLog string) {
	s.mu.Lock() // writers lock
	defer s.mu.Unlock()

	if log, ok := s.jobs[jobId]; ok {
		var buf bytes.Buffer // C:/Go/src/net/url/url.go#719 func (u *URL) String() string {
		buf.WriteString(log)
		buf.WriteByte('\n')
		currentTime := time.Now().Local() // 20060102150405") // ("2006-01-02 15:04:05.000")
		buf.WriteString(fmt.Sprintf("%s-", currentTime.Format("2006-01-02_15:04:05.000")))
		buf.WriteString(jobLog)
		s.jobs[jobId] = buf.String()
	} else {
		//s.log.Warn("Not found jobIdl, Server.jobLog failure", "jobId", jobId, "jobLog", jobLog, "Server.jobs", s.jobs)
		s.jobs[jobId] = jobLog
	}
}
*/
