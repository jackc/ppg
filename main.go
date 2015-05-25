package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"text/template"

	"github.com/jackc/pgx"
)

const Version = "0.1.0"

var options struct {
	parallel int
	repeat   int
	echo     bool
	pretend  bool
	version  bool
}

var connPool *pgx.ConnPool
var sqlTmpl *template.Template
var errChan = make(chan error)
var resultChan = make(chan bool)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options] FILE\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.IntVar(&options.parallel, "parallel", 1, "number of parallel connections")
	flag.IntVar(&options.repeat, "repeat", 1, "number of times to run FILE")
	flag.BoolVar(&options.echo, "echo", false, "echo processed SQL")
	flag.BoolVar(&options.pretend, "pretend", false, "do not actually execute SQL")
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

	var err error
	sqlTmpl, err = template.ParseFiles(flag.Args()[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var connPoolConfig pgx.ConnPoolConfig
	connPoolConfig, err = extractConnPoolConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	connPool, err = pgx.NewConnPool(connPoolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Connection failed:", err)
		os.Exit(1)
	}

	for i := 0; i < options.repeat; i++ {
		go doJob(i)
	}

	for i := 0; i < options.repeat; i++ {
		select {
		case err := <-errChan:
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		case <-resultChan:
		}
	}

	connPool.Close()
}

func extractConnPoolConfig() (pgx.ConnPoolConfig, error) {
	var connPoolConfig pgx.ConnPoolConfig
	var err error

	connPoolConfig.ConnConfig, err = pgx.ParseEnvLibpq()
	connPoolConfig.MaxConnections = options.parallel

	return connPoolConfig, err
}

func doJob(jobNumber int) {
	var data struct {
		Parallel  int
		Repeat    int
		JobNumber int
	}
	data.Parallel = options.parallel
	data.Repeat = options.repeat
	data.JobNumber = jobNumber

	var buf bytes.Buffer
	err := sqlTmpl.Execute(&buf, data)
	if err != nil {
		errChan <- err
		return
	}

	if options.echo {
		fmt.Println(buf.String())
	}

	if !options.pretend {
		_, err = connPool.Exec(buf.String())
		if err != nil {
			errChan <- err
			return
		}
	}

	resultChan <- true
}
