package file

import (
	"testing"
	"github.com/ethereum/go-ethereum/log"
	"os"
	"fmt"
	"path/filepath"
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

func TestDir_Util(t *testing.T) {
	fmt.Printf("os.Args[0]:%s\n[%s]\n[%s]\n", os.Args[0], filepath.Base("."), filepath.Base(".."))

	path, err := GetCurrentAbsPath()
	abs, err2 := filepath.Abs(path)
	fmt.Printf("GetCurrentPath is [%s]:%v-[%s]:%v\n", path, err, abs, err2)

	fi, err := os.Stat(`D:\coding\ztesoft\golang\goep\src\walking\file\`)
	fi2, err2 := os.Stat(".")
	flag := os.SameFile(fi, fi2)
	fmt.Printf("fi:%v-%v\nfi2:%v-%v\nos.SameFile return:%t\n", fi, err, fi2, err2, flag)
	if !flag {
		t.Error("os.SameFile return false", "fi", fi, "fi2", fi2)
	}
	abs, err = filepath.Abs(".")
	fmt.Printf("filepath.Abs(.):%s,%v\n", abs, err)
	abs, err = filepath.Abs("..")
	fmt.Printf("filepath.Abs(..):%s,%v\n", abs, err)
	type DirTst struct {
		rootDir, subDir      string
		check, err, contains bool
	}
	dirs := []DirTst{
		DirTst{`D:\coding\ztesoft\golang\goep\src\walking`, ".", false, false, true},
		DirTst{`D:\coding\`, ".", false, false, true},
		DirTst{`c:/`, ".", false, false, false},
		DirTst{`D:\coding\ztesoft\golang\goep\src\walking`, `D:\coding/ztesoft//golang`, false, false, false},
		DirTst{`e://`, `D:\coding/ztesoft//golang`, false, false, false},
		DirTst{`e://///`, `e:\/coding/ztesoft//golang`, false, false, true},
		DirTst{`e:///coding\ztesoft\golang/`, `e:\coding/ztesoft//golang`, false, false, true},
		DirTst{`e:///`, `e:\coding/ztesoft//golang`, true, true, false},
	}
	for idx, dir := range dirs {
		flag, err = Contains(dir.rootDir, dir.subDir, dir.check)
		fmt.Printf("%d>[%s]contains[%s]return=%t:%v\n", idx, dir.rootDir, dir.subDir, flag, err)
		if dir.err && err == nil {
			t.Error("Contains(rootDir, subDir) return OK but want err",
				"idx", idx, "rootDir", dir.rootDir, "subDir", dir.subDir, "err", dir.err, "flag", flag)
			continue
		}
		if err != nil {
			if !dir.err {
				t.Error("Contains(rootDir, subDir) return err", "idx", idx, "rootDir", dir.rootDir,
					"subDir", dir.subDir, "flag", flag, "err", err)
			}
			continue
		}
		if flag != dir.contains {
			t.Error("Contains(rootDir, subDir) return flag but want contains",
				"idx", idx, "rootDir", dir.rootDir, "subDir", dir.subDir, "flag", flag, "contains", dir.contains)
		}
	}
}
