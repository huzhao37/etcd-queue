/**
 * @Author: hiram
 * @Date: 2021/3/18 13:40
 */
package etcdq

import (
"log"
"os"
)
var (
	Debug *log.Logger
	Info  *log.Logger
	Error *log.Logger
)
func init() {
	Debug = log.New(os.Stdout, "[DEBUG]", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	Info = log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	Error = log.New(os.Stderr, "[ERROR]", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
}
