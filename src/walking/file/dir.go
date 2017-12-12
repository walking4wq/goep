// ref by http://www.jb51.net/article/58137.htm
package file

import (
	"strings"
	"path/filepath"
	"os"

	"github.com/ethereum/go-ethereum/log"
	logext "github.com/inconshreveable/log15/ext"
	"io/ioutil"
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
