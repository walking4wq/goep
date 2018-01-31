package gotest

import (
	"testing"
	"fmt"
	"strconv"
	"hash/crc32"
	"crypto/sha1"

	logext "github.com/inconshreveable/log15/ext"
)

func hash4crc32(key string) uint32 {
	// http://blog.csdn.net/xcl168/article/details/43898807
	return crc32.ChecksumIEEE([]byte(key))
}
func genValue(bs []byte) uint32 {
	if len(bs) < 4 {
		return 0
	}
	v := (uint32(bs[3]) << 24) | (uint32(bs[2]) << 16) | (uint32(bs[1]) << 8) | (uint32(bs[0]))
	return v
}

func hash4sha1(key string) uint32 {
	// https://github.com/g4zhuj/hashring/blob/master/hashring.go
	hash := sha1.New()
	hash.Write([]byte(key))
	hashBytes := hash.Sum(nil)
	return genValue(hashBytes[6:10])
}

type HashCounter map[uint32]int

func PrintHashCounter(m HashCounter) {
	i := 0
	for k, v := range m {
		fmt.Printf("%05d>%v=%v\n", i, k, v)
		i++
	}
}
func TestHash_alg(tst *testing.T) {
	size := 1000

	info := "dafsupoiureqmewqn8731	nfna;fhdpy"
	k4crc32 := hash4crc32(info)
	k4sha1 := hash4sha1(info)
	for i := 0; i < size; i++ {
		k := hash4crc32(info)
		if k4crc32 != k {
			tst.Errorf("%05d>hash4crc32=%v but want %v", i, k, k4crc32)
		}
		k = hash4sha1(info)
		if k4sha1 != k {
			tst.Errorf("%05d>hash4crc32=%v but want %v", i, k, k4sha1)
		}
	}

	var node uint32 = 4
	m4crc32 := make(HashCounter, node)
	m4sha1 := make(HashCounter, node)

	fmt.Println("=== test seq ===")
	for i := 0; i < size; i++ {
		k := strconv.Itoa(i) // fmt.Sprintf("%d", i)
		// fmt.Printf("%05d>crc32=%d,sha1=%d\n", i, hash4crc32(k), hash4sha1(k))
		key := hash4crc32(k) % node
		if _, ok := m4crc32[key]; ok {
			m4crc32[key]++
		} else {
			m4crc32[key] = 1
		}
		key = hash4sha1(k) % node
		if _, ok := m4sha1[key]; ok {
			m4sha1[key]++
		} else {
			m4sha1[key] = 1
		}
	}
	PrintHashCounter(m4crc32)
	fmt.Println("--- crc32 end and sha1 begin ---")
	PrintHashCounter(m4sha1)

	fmt.Println("=== test rand ===")
	for i := 0; i < size; i++ {
		k := logext.RandId(8)
		key := hash4crc32(k) % node
		if _, ok := m4crc32[key]; ok {
			m4crc32[key]++
		} else {
			m4crc32[key] = 1
		}
		key = hash4sha1(k) % node
		if _, ok := m4sha1[key]; ok {
			m4sha1[key]++
		} else {
			m4sha1[key] = 1
		}
	}
	PrintHashCounter(m4crc32)
	fmt.Println("--- crc32 end and sha1 begin ---")
	PrintHashCounter(m4sha1)
}
func hash(key string) int {
	// http://blog.csdn.net/xcl168/article/details/43898807
	return int(crc32.ChecksumIEEE([]byte(key)))
}
func TestHash_Determinism(tst *testing.T) {
	key := "40455,3,MT3,,40455,"
	rst := hash(key)
	times := 100000
	for i := 0; i < times; i++ {
		rst2 := hash(key)
		if rst != rst2 {
			tst.Fatal("%08d>hash(%s)=%v but want %v", i, key, rst2, rst)
		}
	}
}
