package file

import (
	"testing"
	"github.com/ethereum/go-ethereum/log"
	"os"
)

func init() {
	//	// get current timestamp
	//	currentTime := time.Now().Local()
	//	//format Time, string type
	//	newFormat := currentTime.Format("20060102150405") // ("2006-01-02 15:04:05.000")
	//	dirname := "./log/"
	//	if _, err := ioutil.ReadDir(dirname); err != nil {
	//		/*
	//+-----+---+--------------------------+
	//| rwx | 7 | Read, write and execute  |
	//| rw- | 6 | Read, write              |
	//| r-x | 5 | Read, and execute        |
	//| r-- | 4 | Read,                    |
	//| -wx | 3 | Write and execute        |
	//| -w- | 2 | Write                    |
	//| --x | 1 | Execute                  |
	//| --- | 0 | no permissions           |
	//+------------------------------------+
	//
	//+------------+------+-------+
	//| Permission | Octal| Field |
	//+------------+------+-------+
	//| rwx------  | 0700 | User  |
	//| ---rwx---  | 0070 | Group |
	//| ------rwx  | 0007 | Other |
	//+------------+------+-------+
	//		*/
	//		os.Mkdir(dirname, 0666)
	//	}
	//	path := fmt.Sprintf("%smain_%s.log", dirname, newFormat)
	path := "../../../log/file_test.log"
	// os.dir
	h := log.CallerStackHandler("%+v", log.FailoverHandler(
		// D:/coding/ztesoft/blockchain/ethereum/geth/go_get_github/src/github.com/ethereum/go-ethereum/log/handler.go:222
		// log.Must.NetHandler("tcp", ":9090", log.JsonFormat()),
		log.Must.FileHandler(path, log.LogfmtFormat()), // LogfmtFormat
		log.StdoutHandler)) // format ref from:https://github.com/go-stack/stack/blob/master/stack.go
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, h))
}

func FileInfoPrint(t *testing.T, pathFile string) {
	f, e := os.Stat(pathFile)
	if e != nil {
		log.Info("FileInfoPrint err", "pathFile", pathFile, "err", e)
	} else {
		log.Info("FileInfoPrint ok", "pathFile", pathFile, "f", f)
	}
	file, err := os.Open(pathFile)
	if err != nil {
		t.Error("FileInfoPrint Open file failure", "pathFile", pathFile, "err", err)
		return
	}
	defer file.Close()
}

func TestDir_ReadDir(t *testing.T) {
	ReadDir("../../../", "*", func(pathFile string) {
		FileInfoPrint(t, pathFile)
	}) // cdr_tapin/
	// t.Errorf("after unmarshal content unchange\n")
}

func TestDir_Walk(t *testing.T) {
	Walk("../", "*", func(pathFile string) {
		FileInfoPrint(t, pathFile)
	}) // cdr_tapin/src/walking/
	// t.Errorf("after unmarshal content unchange\n")
}
