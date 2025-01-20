package rredis

import (
	"fmt"
	red "github.com/redis/go-redis/v9"
	"reflect"
	"strconv"
	"strings"
)

const addrSep = ","

func splitClusterAddr(addr string) []string {
	addrs := strings.Split(addr, addrSep)
	unique := make(map[string]struct{})
	for _, each := range addrs {
		unique[strings.TrimSpace(each)] = struct{}{}
	}

	addrs = addrs[:0]
	for k := range unique {
		addrs = append(addrs, k)
	}

	return addrs
}

func toStrings(vals []interface{}) []string {
	ret := make([]string, len(vals))

	for i, val := range vals {
		if val == nil {
			ret[i] = ""
			continue
		}

		switch val := val.(type) {
		case string:
			ret[i] = val
		default:
			ret[i] = Repr(val)
		}
	}

	return ret
}

// Repr returns the string representation of v.
func Repr(v interface{}) string {
	if v == nil {
		return ""
	}

	// if func (v *Type) String() string, we can't use Elem()
	switch vt := v.(type) {
	case fmt.Stringer:
		return vt.String()
	}

	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}

	return reprOfValue(val)
}

func reprOfValue(val reflect.Value) string {
	switch vt := val.Interface().(type) {
	case bool:
		return strconv.FormatBool(vt)
	case error:
		return vt.Error()
	case float32:
		return strconv.FormatFloat(float64(vt), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(vt, 'f', -1, 64)
	case fmt.Stringer:
		return vt.String()
	case int:
		return strconv.Itoa(vt)
	case int8:
		return strconv.Itoa(int(vt))
	case int16:
		return strconv.Itoa(int(vt))
	case int32:
		return strconv.Itoa(int(vt))
	case int64:
		return strconv.FormatInt(vt, 10)
	case string:
		return vt
	case uint:
		return strconv.FormatUint(uint64(vt), 10)
	case uint8:
		return strconv.FormatUint(uint64(vt), 10)
	case uint16:
		return strconv.FormatUint(uint64(vt), 10)
	case uint32:
		return strconv.FormatUint(uint64(vt), 10)
	case uint64:
		return strconv.FormatUint(vt, 10)
	case []byte:
		return string(vt)
	default:
		return fmt.Sprint(val.Interface())
	}
}

func toPairs(vals []red.Z) []Pair {
	pairs := make([]Pair, len(vals))
	for i, val := range vals {
		switch member := val.Member.(type) {
		case string:
			pairs[i] = Pair{
				Member: member,
				Score:  int64(val.Score),
			}
		default:
			pairs[i] = Pair{
				Member: Repr(val.Member),
				Score:  int64(val.Score),
			}
		}
	}
	return pairs
}

func toFloatPairs(vals []red.Z) []FloatPair {
	pairs := make([]FloatPair, len(vals))

	for i, val := range vals {
		switch member := val.Member.(type) {
		case string:
			pairs[i] = FloatPair{
				Member: member,
				Score:  val.Score,
			}
		default:
			pairs[i] = FloatPair{
				Member: Repr(val.Member),
				Score:  val.Score,
			}
		}
	}

	return pairs
}
