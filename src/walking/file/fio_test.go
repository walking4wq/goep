package file

import (
	"testing"
	"time"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"os"
	"github.com/jlaffaye/ftp"
	"net/textproto"
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

func TestFio_ReadLine(t *testing.T) {
	pathFile := "../../../clean.bat"
	linCnt, errCnt := ReadLine(pathFile, 0, nil)
	// fmt.Printf("ReadLine(%s, 0, nil) get %d/%d\n", pathFile, linCnt, errCnt)
	if linCnt != 2 || errCnt != 0 {
		t.Errorf("ReadLine(%s, 0, nil) get %d/%d", pathFile, linCnt, errCnt)
	}
	linCnt, errCnt = ReadLine(pathFile, 0, LinePrint)
	if linCnt != 2 || errCnt != 0 {
		t.Errorf("ReadLine(%s, 0, LinePrint) get %d/%d", pathFile, linCnt, errCnt)
	}
}

func TestFio_Writer(t *testing.T) {
	pathFile := "../../../test_data/fio_test/TestFio_Writer.txt"
	maxCount := 10
	flushCnt := 4
	maxDelay := time.Second * 10

	fw := NewWriter(pathFile, maxCount, flushCnt, maxDelay, nil)
	go fw.Run()
	for i := 0; i < 15; i++ {
		fw.Write(fmt.Sprintf("%d", i))
	}
	time.Sleep(maxDelay * 2)
	fw.Close()

	pathFile0 := "../../../test_data/fio_test/TestFio_Writer.0.txt"
	pathFile1 := "../../../test_data/fio_test/TestFio_Writer.1.txt"
	defer func() {
		os.Remove(pathFile0)
		os.Remove(pathFile1)
	}()

	if !IsExist(pathFile0) || !IsExist(pathFile1) {
		t.Errorf("Not found file:%s,%s!", pathFile0, pathFile1)
	}
}
func TestFio_Ftp(t *testing.T) {
	// 10.45.51.101 root/root
	addr := "10.45.51.101:22" // "localhost:21"
	c, err := ftp.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// err = c.Login("anonymous", "anonymous")
	err = c.Login("root", "root")
	if err != nil {
		t.Fatal(err)
	}

	dir, err := c.CurrentDir()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("[%v].CurrentDir:%s!", c, dir)

	//err = c.ChangeDir("mysqlimport")
	//if err != nil {
	//	t.Error(err)
	//}
	//err = c.MakeDir(testDir)
	//if err != nil {
	//	t.Error(err)
	//}
	//data := bytes.NewBufferString(testData)
	//err = c.Stor("test", data)
	//if err != nil {
	//	t.Error(err)
	//}
	//err = c.Rename("test", "tset")
	//if err != nil {
	//	t.Error(err)
	//}

	err = c.Logout()
	if err != nil {
		if protoErr := err.(*textproto.Error); protoErr != nil {
			if protoErr.Code != ftp.StatusNotImplemented {
				t.Error(err)
			}
		} else {
			t.Error(err)
		}
	}

	c.Quit()

	err = c.NoOp()
	if err == nil {
		t.Error("Expected error")
	}
}
