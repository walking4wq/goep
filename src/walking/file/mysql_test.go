package file

import (
	"testing"
	// _ "github.com/go-sql-driver/mysql" // https://github.com/go-sql-driver/mysql#load-data-local-infile-support
	"github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
	"log"
	"time"
	"walking/cdr"
	"path/filepath"
	"strings"
)

func mysqlOpen(t *testing.T) (db *sql.DB) {
	host := "10.45.51.101"
	port := 3306
	dbname := "xeexplore"
	user := "xeexplore"
	pass := "xeexplore"
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?allowAllFiles=true", user, pass, host, port, dbname)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf(`sql.Open("mysql",%s) failure:%v`, dsn, err)
	}
	log.Printf(`sql.Open("mysql",%s) success:%v`, dsn, db)
	return
}

func TestSql_Mysql(t *testing.T) {
	db := mysqlOpen(t)
	defer db.Close()

	//CREATE TABLE IF NOT EXISTS `goep_tst_tab` (
	//	`size` int(11) DEFAULT NULL,
	//	`name` varchar(255) DEFAULT NULL
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8
	tblName := "goep_tst_tab_" + time.Now().Local().Format("20060102") // ("2006-01-02 15:04:05.000")
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	size int(11) DEFAULT NULL,
	name varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8`, tblName)
	rst, err := db.Exec(ddl) // , tblName)
	if err != nil {
		t.Fatalf(`db.Exec(%s) failure:%v`, ddl, err)
	}
	log.Printf("db.Exec(%s) success:%v", ddl, rst)

	sql := fmt.Sprintf("select * from %s limit 6", tblName)
	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf(`db.Query(%s) failure:%v`, sql, err)
	}
	i := 1
	for rows.Next() { // http://blog.csdn.net/u013421629/article/details/72722632
		var size int
		var name string

		err = rows.Scan(&size, &name)
		if err != nil {
			t.Fatalf(`rows[%d->%v].Scan failure:%v`, i, rows, err)
		}
		// user_id := gojson.Json(in_param).Get("user_id").Tostring()
		log.Printf("%05d>%d,%s\n", i, size, name)
		i++
	}
	//desc goep_tst_tab;
	//show create table goep_tst_tab
	//show variables like '%secure%'
	//show variables like '%infile%' -- no effective
	//load data infile "/root/mysqlimport/goep_tst_tab.csv" into table goep_tst_tab
	//load data infile "/var/lib/mysql-files/goep/goep_tst_tab.csv" into table goep_tst_tab fields terminated by ',' enclosed by '\'' lines terminated by '\n'

	// Error 1290: The MySQL server is running with the --secure-file-priv option so it cannot execute this statement
	pathFile := "/root/mysqlimport/goep_tst_tab.csv"
	// Error 13: Can't get stat of '/var/lib/mysql-files/goep/*' (Errcode: 2 - No such file or directory)
	pathFile = "/var/lib/mysql-files/goep/*"
	pathFile = "/var/lib/mysql-files/goep/goep_tst_tab.csv" // is ok
	// Error 1265: Data truncated for column 'size' at row 1
	sql = fmt.Sprintf("load data infile '%s' into table %s", pathFile, tblName)
	sql = fmt.Sprintf(`load data infile '%s' into table %s
fields terminated by ',' enclosed by '\'' lines terminated by '\n'`, pathFile, tblName) // is ok

	pathFile = `D:\\coding\\ztesoft\\golang\\goep\\doc\\tmp\\goep_tst_tab.csv`
	mysql.RegisterLocalFile(pathFile)
	sql = fmt.Sprintf(`load data local infile '%s' into table %s
fields terminated by ',' enclosed by '\'' lines terminated by '\n'`, pathFile, tblName)
	rst, err = db.Exec(sql)
	if err != nil {
		t.Fatalf(`db.Exec(%s,%s) failure:%v`, sql, tblName, err)
	}
	log.Printf("db.Exec(%s,%s) success:%v", sql, tblName, rst)
}

func TestSql_LoadMysql(t *testing.T) {
	db := mysqlOpen(t)
	defer db.Close()

	tab4mysql := "txe_inputkpi_record"
	var inma cdr.GroupINMA
	ddl, lad := inma.Sql()
	sql := fmt.Sprintf(ddl, tab4mysql)
	rst, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("db.Exec(%s) failure:%v", sql, err)
	}
	log.Printf("db.Exec(%s) success:%v", sql, rst)

	pathFile := "../../../doc/tmp/ds/output/_file_adapter-20180116201311/data/GroupINMA_0.0.dsv"
	absPathFile, err := filepath.Abs(pathFile)
	if err != nil {
		t.Fatalf("filepath.Abs(%s) failure:%v", pathFile, err)
	}
	absPathFile = strings.Replace(absPathFile, "\\", "/", -1)
	// absPathFile := filepath.Join(path, filepath.Base(pathFile))
	log.Printf("convert the file path[%s] to an absolute path:%s!", pathFile, absPathFile)
	sql = fmt.Sprintf(lad, absPathFile, tab4mysql)
	mysql.RegisterLocalFile(absPathFile)
	rst, err = db.Exec(sql)
	if err != nil {
		t.Fatalf("db.Exec(%s) failure:%v", sql, err)
	}
	log.Printf("db.Exec(%s) success:%v", sql, rst)
}
