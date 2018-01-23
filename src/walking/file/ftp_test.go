package file

import (
	"testing"
	"net"
	"os"
	"golang.org/x/crypto/ssh/agent"
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"log"
	"time"
	"io"
)

// package vendor/golang.org/x/crypto/ssh:
// directory "D:\\coding\\ztesoft\\golang\\goep\\src\\vendor\\golang.org\\x\\crypto\\ssh"
// is not using a known version control system
func TestPkg_Sftp(t *testing.T) { // http://www.01happy.com/golang-transfer-remote-file/
	USER := "root"         // flag.String("user", os.Getenv("USER"), "ssh username")
	HOST := "10.45.51.101" // flag.String("host", "localhost", "ssh server hostname")
	PORT := 22             // flag.Int("port", 22, "ssh server port")
	PASS := "root"         // flag.String("pass", os.Getenv("SOCKSIE_SSH_PASSWORD"), "ssh password")
	SIZE := 1 << 15        // flag.Int("s", 1<<15, "set max packet size")

	var auths []ssh.AuthMethod
	if aconn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(aconn).Signers))
	}
	//if *PASS != "" {
	//	auths = append(auths, ssh.Password(*PASS))
	//}
	if PASS != "" {
		auths = append(auths, ssh.Password(PASS))
	}
	config := ssh.ClientConfig{
		// User:            *USER,
		User:            USER,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	// addr := fmt.Sprintf("%s:%d", *HOST, *PORT)
	addr := fmt.Sprintf("%s:%d", HOST, PORT)
	conn, err := ssh.Dial("tcp", addr, &config)
	if err != nil {
		log.Fatalf("unable to connect to [%s]: %v", addr, err)
	}
	defer conn.Close()

	// c, err := sftp.NewClient(conn, sftp.MaxPacket(*SIZE))
	c, err := sftp.NewClient(conn, sftp.MaxPacket(SIZE))
	if err != nil {
		log.Fatalf("unable to start sftp subsytem: %v", err)
	}
	defer c.Close()

	pwd, err := c.Getwd()
	if err != nil {
		t.Fatalf("Getwd(%v):%v", c, err)
	}
	fmt.Printf("c[%v].Getwd:%s!", c, pwd)

	//wkr := c.Walk(pwd)
	//i := 1
	//for wkr.Step() {
	//	fmt.Printf("%s-%d>%s\n", pwd, i, wkr.Path())
	//	i++
	//}
	fis, err := c.ReadDir(pwd)
	if err != nil {
		t.Fatalf("ReadDir(%s):%v", pwd, err)
	}
	for i, fi := range fis {
		fmt.Printf("%d>%v\n", i, fi)
	}

	// show variables like '%secure%' // secure_file_priv=/var/lib/mysql-files/
	rootPath := "mysqlimport/" // "/root/mysqlimport/"
	subPath := "test/mysql/"   // file does not exist
	subPath = "test/"
	currentTime := time.Now().Local()
	sysdate := currentTime.Format("20060102150405") // ("2006-01-02 15:04:05.000")

	tmpPath := fmt.Sprintf("%s%s", rootPath, subPath)
	// hisPath := fmt.Sprintf("%s%c%s", tmpPath, os.PathSeparator, sysdate)
	hisPath := fmt.Sprintf("%s%s", rootPath, sysdate)

	err = c.Mkdir(tmpPath)
	if err != nil {
		t.Fatalf("Mkdir(%s):%v", tmpPath, err)
	}
	fmt.Printf("Mkdir(%s) success\n", tmpPath)

	fileName := "ftp_test.go"
	updataPathFile := fmt.Sprintf("%s%s%s", rootPath, subPath, fileName)
	// path.Join("")
	w, err := c.Create(updataPathFile) // c.OpenFile(updataPathFile, syscall.O_WRONLY)
	if err != nil {
		log.Fatalf("c.Create(%s):%v", updataPathFile, err)
	}
	defer w.Close()
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal("os.Open(%s):%v", fileName, err)
	}
	defer f.Close()

	//const size int64 = 1e9
	//log.Printf("writing %v bytes", size)
	buf := make([]byte, 1024*1024*64)
	t1 := time.Now()
	//n, err := io.Copy(w, io.LimitReader(f, size))
	n, err := io.CopyBuffer(w, f, buf)
	if err != nil {
		log.Fatalf("io.CopyBuffer(dst:%v,src:%v,buf:%v):%v", w, f, buf, err)
	}
	//if n != size {
	//	log.Fatalf("copy: expected %v bytes, got %d", size, n)
	//}
	log.Printf("wrote %v bytes in %s", n, time.Since(t1))
	//for {
	//	n, _ := f.Read(buf)
	//	if n == 0 {
	//		break
	//	}
	//	w.Write(buf)
	//}

	defer func() {
		fmt.Printf("defer [%s]->[%s]\n", tmpPath, hisPath)
		//return
		err = c.Rename(tmpPath, hisPath)
		if err != nil {
			t.Fatalf("Rename(old:%s,new:%s):%v", tmpPath, hisPath, err)
		}
		fmt.Printf("Rename(old:%s,new:%s) success\n", tmpPath, hisPath)

		//err = c.Remove(hisPath + "/*") // file does not exist
		//if err != nil {
		//	t.Fatalf("Remove(%s/*):%v", hisPath, err)
		//}
		//fmt.Printf("Remove(%s/*) success\n", hisPath)
		//err = c.RemoveDirectory(hisPath)
		//if err != nil {
		//	t.Fatalf("RemoveDirectory(%s):%v", hisPath, err)
		//}
		//fmt.Printf("RemoveDirectory(%s) success\n", hisPath)
	}()
}
