package main

import (
	"flag"
	"fmt"
	"strings"
	"os"
	"time"
	"io/ioutil"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"net/http"
	"walking/file"
)

var port = flag.Uint("port", 8080, "network port, default is 8080")
//var path = flag.String("path", ".", "input filepath, default is [.]")
//var rj = flag.Int("rj", 3, "File reject limit, default is 3")
var v = flag.Bool(metrics.MetricsEnabledFlag, false, "export metrics to http://localhost:port+1/debug/metrics")

func main() {
	fmt.Println("args:" + strings.Join(os.Args[1:], ","))
	flag.Parse()
	fmt.Println("args parsed:" + strings.Join(flag.Args(), ","))
	i := 0
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("%d>%v\n", i, f)
		i++
	})

	// get current timestamp
	currentTime := time.Now().Local()
	//format Time, string type
	newFormat := currentTime.Format("20060102") // 20060102150405") // ("2006-01-02 15:04:05.000")
	dirName := "./log/"
	if _, err := ioutil.ReadDir(dirName); err != nil {
		/*
+-----+---+--------------------------+
| rwx | 7 | Read, write and execute  |
| rw- | 6 | Read, write              |
| r-x | 5 | Read, and execute        |
| r-- | 4 | Read,                    |
| -wx | 3 | Write and execute        |
| -w- | 2 | Write                    |
| --x | 1 | Execute                  |
| --- | 0 | no permissions           |
+------------------------------------+

+------------+------+-------+
| Permission | Octal| Field |
+------------+------+-------+
| rwx------  | 0700 | User  |
| ---rwx---  | 0070 | Group |
| ------rwx  | 0007 | Other |
+------------+------+-------+
		*/
		os.Mkdir(dirName, 0666)
	}
	path := fmt.Sprintf("%smain_%s.log", dirName, newFormat)
	// os.dir
	h := log.CallerStackHandler("%+v", log.FailoverHandler(
		// D:/coding/ztesoft/blockchain/ethereum/geth/go_get_github/src/github.com/ethereum/go-ethereum/log/handler.go:222
		// log.Must.NetHandler("tcp", ":9090", log.JsonFormat()),
		log.Must.FileHandler(path, log.LogfmtFormat()), // LogfmtFormat
		log.StdoutHandler)) // format ref from:https://github.com/go-stack/stack/blob/master/stack.go
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, h))

	addr := fmt.Sprintf(":%d", *port)
	if metrics.Enabled {
		addr := fmt.Sprintf(":%d", *port+1)
		log.Info("Start goroutine for metrics http server:http://localhost${addr}/debug/metrics ...",
			"addr", addr)
		go func() {
			log.Crit("http.ListenAndServe(${addr}, nil) failure",
				"return", http.ListenAndServe(addr, nil), "addr", addr)
		}()
	} else {
		metrics.Enabled = true
	}
	dbPath := "./ldb/" // TODO
	cache := 256
	handles := 0
	db, err := ethdb.NewLDBDatabase(dbPath, cache, handles)
	if err != nil {
		log.Crit("failed to create or open database", "err", err)
	} else {
		log.Info("Started database...", "db", db)
	}
	// defer func() { os.RemoveAll(dirname) }()
	defer db.Close()
	db.Meter("goep_")

	http.HandleFunc("/", serveHome)
	// go pgd.run()
	//http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
	//	wsHandler(pgd, w, r)
	//})
	addTestHandleFunc()

	svr := NewServer(db)
	svr.RegisterHandleFunc()

	log.Info(fmt.Sprintf("Start http server:http://localhost%s/ ...", addr))
	err = http.ListenAndServe(addr, nil)
	if err != nil {
		// log.Fatal("ListenAndServe: ", err)
		log.Crit("ListenAndServe: ", "err", err, "addr", addr)
	}
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	log.Info("call serveHome", "url", r.URL)
	if r.URL.Path != "/" {
		http.Error(w, `{"return_code":404,"user_message":"Not found"}`, 404)
		return
	}
	if r.Method != "GET" {
		http.Error(w, `{"return_code":405,"user_message":"Method not allowed"}`, 405)
		return
	}
	// search index.html
	indexHtml := "index.html"
	indexHtmls := []string{indexHtml, fmt.Sprintf("web/%s", indexHtml),
		fmt.Sprintf("../web/%s", indexHtml)}
	for i, p := range indexHtmls {
		if file.IsExist(p) {
			http.ServeFile(w, r, p)
			log.Info("Search index html success", "index.html", p)
			return
		} else {
			log.Info("Search index html not found", "try", i, "index.html", p)
		}
	}
	http.Error(w, fmt.Sprintf(`{"return_code":404,"user_message":"Not found index.html:%v"}`, indexHtmls), 404)
	log.Error("Search index html failure", "indexHtmls", indexHtmls)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type dollars float32

func (d dollars) String() string { return fmt.Sprintf("$%.2f", d) }

type database map[string]dollars

func (db database) list(w http.ResponseWriter, req *http.Request) {
	for item, price := range db {
		fmt.Fprintf(w, "{%s:%s}\n", item, price)
	}
}

func (db database) price(w http.ResponseWriter, req *http.Request) {
	item := req.URL.Query().Get("item")
	price, ok := db[item]
	if !ok {
		w.WriteHeader(http.StatusNotFound) //	404
		fmt.Fprintf(w, "no such item:%q\n", item)
		return
	}
	fmt.Fprintf(w, "%s\n", price)
}
func addTestHandleFunc() {
	db := database{"shoes": 50, "socks": 5}
	http.HandleFunc("/list", db.list)
	http.HandleFunc("/price", db.price)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
