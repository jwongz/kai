// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
    "context"
    "flag"
    "fmt"
    "math"
    "os"
    "sync"
    "time"
    "io/ioutil"

    "github.com/tikv/client-go/config"
    "github.com/tikv/client-go/rawkv"
)

var (
    client *rawkv.Client
    pdAddr    = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
    filePath  = flag.String("filePath", "", "test file")
    valueSize = flag.Int("V", 4194304, "value size in byte, default:4MB")
)

type KV struct {
    K, V []byte
}

// Init initializes information.
func initStore() {
    var err error
    client, err = rawkv.NewClient(context.TODO(), []string{*pdAddr}, config.Default())
    if err != nil {
        panic(err)
    }
}

// batchRW makes sure conflict free.
func batchRW(value []byte, size int) {
    wg := sync.WaitGroup{}
    base := int(math.Ceil(float64(size) / float64(*valueSize)))
    wg.Add(base)
    for i := 0; i < base; i++ {
        go func(value []byte, i int) {
            defer wg.Done()

            start := time.Now()
            key := fmt.Sprintf("file_key_%d", i)
            var err error

            if i == (base - 1) {
                err = client.Put(context.TODO(), []byte(key), value[*valueSize * i :])
            } else {
                err = client.Put(context.TODO(), []byte(key), value[*valueSize * i : *valueSize * (i + 1)])
            }
            
            if err != nil {
                panic(err)
            }

            fmt.Printf("\nelapse:%v, key: %s\n", time.Since(start), key)
        }(value, i)
    }
    wg.Wait()
}

func get(k []byte) (KV, error) {
    v, err := client.Get(context.TODO(), k)
    if err != nil {
        return KV{}, err
    }
    return KV{K: k, V: v}, nil
}

func main() {
    flag.Parse()
    initStore()

    f, err := os.OpenFile(*filePath, os.O_RDWR|os.O_CREATE, 0755)
    defer f.Close()
    if err != nil {
        panic(err)
    }

    value, err := ioutil.ReadAll(f)
    if err != nil {
        panic(err)
    }

    t := time.Now()
    size := len(value)
    batchRW(value, size)
    fmt.Printf("\nelapse:%v, total size:%v\n", time.Since(t), size)

    v, err := get([]byte("file_key_0"))
    if err != nil {
        panic(err)
    }

    fmt.Println(string(v.K), len(v.V))
}
