package gotest

import (
	"testing"
	"encoding/json"
	"log"
)

type Dao struct {
	Id   int
	Name string
}

func (d Dao) Unmarshal(data []byte) (err error) {
	err = json.Unmarshal(data, &d)
	log.Printf("after unmarshal in object inner:%v\n", d)
	return
}

func TestPtr_json(t *testing.T) {
	id := 123
	name := "abc"
	d := Dao{id, name}
	data := []byte(`{"Id":321,"Name":"cba"}`)
	if err := d.Unmarshal(data); err != nil {
		log.Printf("Dao Unmarshal data error:%s\n", err)
	}
	log.Printf("after unmarshal:%v\n", d)
	if d.Id != id && d.Name != name {
		t.Errorf("after unmarshal content unchange\n")
	}
}

func localSlice() []byte {
	str := "1234567890"
	a := []byte(str)
	b := a[0:3]
	return b
}
func TestPtr_slice(t *testing.T) {
	str := "abcdefgh"
	a := []byte(str)
	b := a[0:3]
	b[1] = 'x'
	log.Printf("str:%s,a:%s,b:%s!", str, string(a), string(b))
	if string(a) == str {
		t.Errorf("slice not modified\n")
	}

	b2 := localSlice()
	log.Printf("b2:%s!", string(b2))
	if "123" != string(b2) {
		t.Errorf("local slice modified\n")
	}
}
