// ref by http://www.jb51.net/article/58137.htm
package file

import (
	"strings"
	"path/filepath"
	"os"

	"github.com/ethereum/go-ethereum/log"
	logext "github.com/inconshreveable/log15/ext"
	"io/ioutil"
	"os/exec"
	"fmt"
)

var log_ log.Logger

func init() {
	log_ = log.New("file.rid", logext.RandId(8))
}

// type DirFn func(filename string, fi os.FileInfo)
type DirFn func(pathFile string)

// func DirPrint(filename string, fi os.FileInfo) {
//log_.Info("call DirPrint", "filename", filename, "fi", fi)
func DirPrint(pathFile string) {
	log_.Info("call DirPrint", "pathFile", pathFile)
}

func ReadDir(dirName, suffix string, df DirFn) {
	dir, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Error("ioutil.ReadDir error", "dirName", dirName, "suffix", suffix, "err", err)
		return
	}
	ps := string(os.PathSeparator)
	suffix = strings.ToUpper(suffix)
	for _, fi := range dir {
		if fi.IsDir() {
			log_.Trace("ioutil.ReadDir skip dirName", "dirName", dirName, "suffix", suffix, "fi", fi)
			continue
		}
		if suffix == "*" || strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) {
			log_.Trace("ioutil.ReadDir get file", "dirName", dirName, "suffix", suffix, "fi", fi)
			if df != nil {
				// df(fi.Name(), fi)
				df(dirName + ps + fi.Name()) // http://sugarmanman.blog.163.com/blog/static/81079080201372962431611/
			}
		} else {
			log_.Trace("ioutil.ReadDir skip file", "dirName", dirName, "suffix", suffix, "fi", fi)
		}
	}
}

func Walk(root, suffix string, df DirFn) {
	suffix = strings.ToUpper(suffix)
	err := filepath.Walk(root, func(pathFile string, fi os.FileInfo, err error) error {
		if err != nil {
			log.Error("filepath.Walk error", "root", root, "suffix", suffix, "pathFile", pathFile, "fi", fi, "err", err)
			return nil
		}
		if fi.IsDir() {
			log_.Trace("filepath.Walk get root", "root", root, "suffix", suffix, "pathFile", pathFile, "fi", fi)
			return nil
		}
		if suffix == "*" || strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) {
			log_.Trace("filepath.Walk get file", "root", root, "suffix", suffix, "pathFile", pathFile, "fi", fi)
			if df != nil {
				// df(filename, fi)
				df(pathFile)
			}
		} else {
			log_.Trace("filepath.Walk skip file", "root", root, "suffix", suffix, "pathFile", pathFile, "fi", fi)
		}
		return nil
	})
	if err != nil {
		log.Error("filepath.Walk error", "root", root, "suffix", suffix, "err", err)
	}
}

// http://sugarmanman.blog.163.com/blog/static/81079080201372962431611/
func IsExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func Suffix(pathFile string, suffixSeparator byte) (suffix string) {
	if i := strings.LastIndexByte(pathFile, suffixSeparator); i != -1 {
		suffix = strings.ToUpper(pathFile[i+1:])
	} else {
		suffix = strings.ToUpper(pathFile)
	}
	return
}

func GetCurrentAbsPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	// # ./filepath
	// exec.LookPath(./filepath):./filepath,<nil>
	// filepath.Abs(./filepath):/root/wq/filepath,<nil>
	// filepath.Dir(./filepath):.
	// # /root/wq/filepath
	// exec.LookPath(/root/wq/filepath):/root/wq/filepath,<nil>
	// filepath.Abs(/root/wq/filepath):/root/wq/filepath,<nil>
	// filepath.Dir(/root/wq/filepath):/root/wq
	// # ../wq/filepath
	// exec.LookPath(../wq/filepath):../wq/filepath,<nil>
	// filepath.Abs(../wq/filepath):/root/wq/filepath,<nil>
	// filepath.Dir(../wq/filepath):../wq
	// return fmt.Sprintf("%s%c", filepath.Dir(file), os.PathSeparator), nil
	abs, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%c", filepath.Dir(abs), os.PathSeparator), nil

	//path, err := filepath.Abs(file)
	//if err != nil {
	//	return ".", err
	//}
	//return path, nil
	//i := strings.LastIndex(path, "/")
	//if i < 0 {
	//	i = strings.LastIndex(path, "\\")
	//}
	//if i < 0 {
	//	return ".", errors.New(`error: Can't find "/" or "\".`)
	//}
	//return string(path[0: i+1]), nil
}
func Contains(rootDir, subDir string, check bool) (contains bool, err error) {
	rootDirAbs, err := filepath.Abs(rootDir)
	if err != nil {
		// log_.Trace("Contains Abs(rootDir) error", "rootDir", rootDir, "rootDirAbs", rootDirAbs, "err", err)
		// fmt.Printf("Contains(rootDir[%s],subDir[%s]) Abs(rootDir) error:%v\n", rootDir, subDir, err)
		return false, err
	}
	if check {
		if _, err = os.Stat(rootDirAbs); err != nil {
			return false, err
		}
	}
	// _, err = os.Stat(rootDir) // check the dir valid
	subDirAbs, err := filepath.Abs(subDir)
	if err != nil {
		// log_.Trace("Contains Abs(subDir) error", "subDir", subDir, "subDirAbs", subDirAbs, "err", err)
		// fmt.Printf("Contains(rootDir[%s],subDir[%s]) Abs(subDir) error:%v\n", rootDir, subDir, err)
		return false, err
	}
	if check {
		if _, err = os.Stat(subDirAbs); err != nil {
			return false, err
		}
	}
	//subDirAbsLen := len(subDirAbs)
	//rootDirAbsLen := len(rootDirAbs)
	//contains = subDirAbsLen > rootDirAbsLen && subDirAbs[0:rootDirAbsLen] == rootDirAbs
	contains = strings.HasPrefix(subDirAbs, rootDirAbs)
	//fmt.Printf("Contains(rootDir[%s]->[%s],subDir[%s]->[%s]) return %t\n",
	//	rootDir, rootDirAbs, subDir, subDirAbs, contains)
	log_.Trace("Contains executed", "rootDir", rootDir, "rootDirAbs", rootDirAbs, "subDir",
		subDir, "subDirAbs", subDirAbs, "contains", contains)
	return
	// return fmt.Errorf("rootDir[%s] not contains subDir[%s]", rootDir, subDir)

	//rfi, err := os.Stat(rootDir)
	//if err != nil {
	//	return err
	//}
	//if !rfi.IsDir() {
	//	return fmt.Errorf("rootDir[%s] is not dir, cant contains subDir[%s]", rootDir, subDir)
	//}
	//sfi, err := os.Stat(subDir)
	//if err != nil {
	//	return err
	//}
	//// contains := false
	//return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
	//	fmt.Printf("WalkFn(%s,%v,%v)\n", path, info, err)
	//	if err != nil {
	//		return err
	//	}
	//	if os.SameFile(sfi, info) {
	//		// contains = true
	//		// goto rtn
	//		return filepath.SkipDir
	//	}
	//	return nil
	//})
	//// rtn:
	//// return contains
}
