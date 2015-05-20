package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/template"

	"github.com/jackc/pgx"
)

const Version = "0.0.1"

var options struct {
	parallel int
	count    int
	version  bool
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options] FILE\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.IntVar(&options.parallel, "parallel", 2, "number of parallel connections")
	flag.IntVar(&options.count, "count", 1, "number of times to run FILE")
	flag.BoolVar(&options.version, "version", false, "print version and exit")
	flag.Parse()

	if options.version {
		fmt.Printf("ppg v%v\n", Version)
		os.Exit(0)
	}

	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(1)
	}

	sqlTmpl, err := template.ParseFiles(flag.Args()[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var connPoolConfig pgx.ConnPoolConfig
	connPoolConfig.Host = os.Getenv("PGHOST")
	if os.Getenv("PGPORT") != "" {
		n64, err := strconv.ParseUint(os.Getenv("PGPORT"), 10, 16)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Invalid port:", err)
			os.Exit(1)
		}

		connPoolConfig.Port = uint16(n64)
	}
	connPoolConfig.Database = os.Getenv("PGDATABASE")
	connPoolConfig.User = os.Getenv("PGUSER")
	connPoolConfig.Password = os.Getenv("PGPASSWORD")

	connPoolConfig.MaxConnections = options.parallel

	connPool, err := pgx.NewConnPool(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Connection failed:", err)
		os.Exit(1)
	}

	errChan := make(chan error)
	resultChan := make(chan bool)

	for i := 0; i < options.count; i++ {
		go func(i int) {
			var data struct {
				Parallel  int
				Count     int
				JobNumber int
			}
			data.Parallel = options.parallel
			data.Count = options.count
			data.JobNumber = i

			var buf bytes.Buffer
			err := sqlTmpl.Execute(&buf, data)
			if err != nil {
				errChan <- err
				return
			}

			_, err = connPool.Exec(buf.String())
			if err != nil {
				errChan <- err
				return
			}

			resultChan <- true
		}(i)
	}

	for i := 0; i < options.count; i++ {
		select {
		case err := <-errChan:
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		case <-resultChan:
		}
	}

	connPool.Close()
}
