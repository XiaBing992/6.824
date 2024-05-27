/*
 * @Author: WXB 1763567512@qq.com
 * @Date: 2024-04-25 16:58:55
 * @LastEditors: WXB 1763567512@qq.com
 * @LastEditTime: 2024-05-18 20:55:54
 * @FilePath: /6.824/labgob/labgob.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package labgob

import (
	"encoding/gob"
	"io"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"
	"fmt"
)


var mu sync.Mutex
var errorCount int
var checked map[reflect.Type] bool

type LabEncoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *LabEncoder {
	enc := &LabEncoder{}
	enc.gob = gob.NewEncoder(w)
	return enc
}

func (enc *LabEncoder) Encode(e interface{}) error {
	checkValue(e)
	return enc.gob.Encode(e)
}

func (enc *LabEncoder) EncodeValue(value reflect.Value) error {
	checkValue(value.Interface())
	return enc.gob.EncodeValue(value)
}

type LabDecoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *LabDecoder {
	dec := &LabDecoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}

func (dec *LabDecoder) Decode(e interface{}) error {
	checkValue(e)
	checkDefault(e)
	return dec.gob.Decode(e)
}

func Register(value interface{}) {
	checkValue(value)
	gob.Register(value) // 对interface类型进行注册
}

func RegisterName(name string, value interface{}) {
	checkValue(value)
	gob.RegisterName(name, value)
}

func checkValue(value interface{}) {
	checkType(reflect.TypeOf(value))
}

func checkType(t reflect.Type) {
	k := t.Kind()

	mu.Lock()

	if checked == nil {
		checked = map[reflect.Type]bool{}
	}

	if checked[t] {
		mu.Unlock()
		return
	}
	checked[t] = true
	mu.Unlock()

	switch k {
	case reflect.Struct:
		for i:=0; i < t.NumField(); i++ {
			f := t.Field(i)
			rune, _ := utf8.DecodeLastRuneInString(f.Name)
			if unicode.IsUpper(rune) == false {
				fmt.Printf("labgob error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n",
					f.Name, t.Name())
				mu.Lock()
				errorCount += 1
				mu.Unlock()
			}
			checkType(f.Type)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return
	}
}

func checkDefault(value interface{}) {
	if value == nil {
		return
	}
	checkDefault1(reflect.ValueOf(value), 1, "")
}

func checkDefault1(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}

	t := value.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			vv := value.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefault1(vv, depth+1, name1)
		}
		return
	case reflect.Ptr:
		if value.IsNil() {
			return
		}
		checkDefault1(value.Elem(), depth+1, name)
		return
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.String:
		if reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) == false {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name()
				}
				// this warning typically arises if code re-uses the same RPC reply
				// variable for multiple RPC calls, or if code restores persisted
				// state into variable that already have non-default values.
				fmt.Printf("labgob warning: Decoding into a non-default variable/field %v may not work\n",
					what)
			}
			errorCount += 1
			mu.Unlock()
		}
		return
	}
}