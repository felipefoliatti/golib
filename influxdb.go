package golib

import (
	"fmt"
	"net"
	"time"

	"github.com/felipefoliatti/backoff"
	"github.com/felipefoliatti/errors"
	_ "github.com/influxdata/influxdb1-client"
	v2 "github.com/influxdata/influxdb1-client/v2"
)

type Influx struct {
	url      string
	database string
	client   v2.Client
}

func (i *Influx) Write(measurement string, fields map[string]interface{}, tags map[string]string, time time.Time) *errors.Error {

	var e error
	var err *errors.Error

	// Create a new point batch
	bp, e := v2.NewBatchPoints(v2.BatchPointsConfig{
		Database:  i.database,
		Precision: "ns",
		// RetentionPolicy: "720d",
	})

	err = errors.WrapInner("error creating a batch point", e, 0)
	if err != nil {
		return err
	}

	var pt *v2.Point
	pt, e = v2.NewPoint(measurement, tags, fields, time)

	err = errors.WrapInner("error creating a new point", e, 0)
	if err != nil {
		return err
	}

	bp.AddPoint(pt)

	e = backoff.Retry(func() error {

		var e error
		var err *errors.Error

		if i.client == nil {
			var c v2.Client
			c, e = v2.NewHTTPClient(v2.HTTPConfig{
				Addr:     i.url,
				Username: "root",
				Password: "root",
			})

			err = errors.WrapInner("error creating the http client", e, 0)
			if err == nil {
				i.client = c
			}
		}

		//if no error connecting
		if err == nil {

			// Write the batch
			e = i.client.Write(bp)
			err = errors.WrapInner("error writing the points to influx", e, 0)
			//fmt.Println("ESCRITO! : -  " + golib.TryError(e))

			// If any connection error
			if err != nil {
				var ok bool
				if _, ok = err.Root().(net.Error); ok {
					i.client = nil
				}
			}
		}

		return err
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
	err = e.(*errors.Error)

	return err
}

// queryDB convenience function to query the database
func (i *Influx) Query(cmd string) ([]v2.Result, *errors.Error) {

	var err *errors.Error
	var res []v2.Result

	q := v2.Query{
		Command:  cmd,
		Database: i.database,
	}
	if response, e := i.client.Query(q); e == nil {

		if response.Error() != nil {
			err = errors.WrapInner("error querying the influx database", response.Error(), 0)
			return res, err
		}
		res = response.Results

	} else {
		return res, errors.WrapInner("error querying the influx database", e, 0)
	}
	return res, nil
}

func NewInflux(database string, host string, port string) (*Influx, *errors.Error) {

	var err *errors.Error
	var e error

	obj := &Influx{}
	obj.url = host + ":" + port
	obj.database = database

	//try to connect 3 times
	e = backoff.Retry(func() error {
		c, e := v2.NewHTTPClient(v2.HTTPConfig{
			Addr:     obj.url,
			Username: "root",
			Password: "root",
		})
		obj.client = c

		return errors.WrapInner("error connecting to influx database", e, 0)
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
	err = e.(*errors.Error)

	if err == nil {
		_, err = obj.Query(fmt.Sprintf("CREATE DATABASE %s", obj.database))
	}

	if err == nil {
		_, err = obj.Query(fmt.Sprintf("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION 1 DEFAULT", "112w", obj.database, "112w"))

	}

	return obj, err
}
