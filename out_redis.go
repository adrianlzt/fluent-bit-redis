package main

import (
	"C"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ugorji/go/codec"
)

var client *redis.Client

//export FLBPluginRegister
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginRegister(ctx unsafe.Pointer) int {
	// roll call for the specifics of the plugin
	return output.FLBPluginRegister(ctx, "redis", "Redis")
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	redisServer := os.Getenv("REDIS_SERVER")
	if redisServer == "" {
		redisServer = "localhost:6379"
	}

	var err error
	client, err = redis.Dial("tcp", redisServer)
	if err != nil {
		fmt.Printf("Error connecting to redis server: %v\n", err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var h codec.Handle = new(codec.MsgpackHandle)
	var b []byte
	var m interface{}
	var err error

	if client == nil {
		fmt.Printf("redis client is nil. Trying to reconnect\n")
		reconnect := FLBPluginInit(nil)
		if reconnect == output.FLB_ERROR {
			return output.FLB_RETRY
		}
		fmt.Printf("Reconnected to redis\n")
	}

	b = C.GoBytes(data, length)
	dec := codec.NewDecoderBytes(b, h)

	// Iterate the original MessagePack array
	count = 0
	for {
		// Decode the entry
		err = dec.Decode(&m)
		if err != nil {
			break
		}

		// Get a slice and their two entries: timestamp and map
		slice := reflect.ValueOf(m)

		// The first entry is a struct with the date in it
		val := reflect.ValueOf(slice.Index(0).Interface())

		if val.Kind() != reflect.Struct {
			fmt.Printf("%v type can't have attributes inspected\n", val.Kind())
		}

		// Data containes 8 bytes, 4 first bytes are unix epoc, last ones nanoseconds
		// https://github.com/fluent/fluent-bit/blob/669dc377d5b87b482f84897506231bc0de5ee76c/src/flb_time.c#L167
		timestampData := val.FieldByName("Data")
		timestamp := binary.BigEndian.Uint32(timestampData.Bytes()[0:4])

		// Second entry is a map with the data
		data := slice.Index(1)

		// Convert slice data to a real map and flatten to insert into redis as HASH
		map_data := data.Interface().(map[interface{}]interface{})
		flat := []string{}
		for key, value := range map_data {
			flat = append(flat, key.(string))
			flat = append(flat, string(value.([]byte)))
		}

		key := fmt.Sprintf("%v.%v.%v", C.GoString(tag), timestamp, count)
		resp := client.Cmd("HSET", key, flat)

		// If cannot write to redis because connection problem, retry
		// If fails to write because an application problem, discard
		if resp.IsType(redis.IOErr) {
			fmt.Printf("Connection problem with redis: %v\n", resp.Err)
			client = nil
			return output.FLB_RETRY
		} else if resp.IsType(redis.AppErr) {
			fmt.Printf("Error writing to redis: %v\n", resp.Err)
			return output.FLB_ERROR
		}

		// fmt.Printf("HSET %v %v\n\n", key, flat)

		// To mofidy key if we have several metrics in the same second
		count++
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return 0
}

func main() {
}
