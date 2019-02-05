package golib

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/felipefoliatti/backoff"
	"github.com/felipefoliatti/errors"
	"github.com/felipefoliatti/golib"
)

func Post(logger golib.Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("POST", logger, url, obj, target, headers)
}
func Put(logger golib.Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("PUT", logger, url, obj, target, headers)
}
func Patch(logger golib.Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("PATCH", logger, url, obj, target, headers)
}
func Head(logger golib.Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("HEAD", logger, url, obj, target, headers)
}

func request(method string, logger golib.Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	err := backoff.Retry(func() error {

		var e error
		var err *errors.Error

		var resp *http.Response
		var req *http.Request

		b := &bytes.Buffer{}

		if obj != nil {
			enc := json.NewEncoder(b)
			enc.SetEscapeHTML(false)

			e = enc.Encode(obj)
			err = errors.WrapPrefix(e, "error marshalling", 0)
		}

		if err != nil {
			return err
		}

		j := []byte(string(b.Bytes()))
		req, e = http.NewRequest(method, url, bytes.NewBuffer(j))
		err = errors.WrapPrefix(e, "error creating the request", 0)

		if headers != nil {
			for key, value := range headers {
				req.Header.Add(key, value)
			}
		}

		if err == nil {
			req.Header.Set("Content-Type", "application/json")

			client := http.Client{Timeout: time.Duration(10 * time.Second)}
			resp, e = client.Do(req)
			err = errors.WrapPrefix(e, "error requesting", 0)

			if err == nil {

				defer resp.Body.Close()

				if resp.StatusCode == 200 || resp.StatusCode == 202 {

					e = nil
					if target != nil {
						//var response Response
						e = json.NewDecoder(resp.Body).Decode(&target)
						err = errors.WrapPrefix(e, "error decoding", 0)
					}
					return e

				} else {
					//body, err := ioutil.ReadAll(resp.Body)
					scanner := bufio.NewScanner(resp.Body)
					scanner.Split(bufio.ScanRunes)
					var buf bytes.Buffer
					for scanner.Scan() {
						buf.WriteString(scanner.Text())
					}

					body := strings.Replace(buf.String(), "\"", "'", -1)
					e = fmt.Errorf("error in service - %s - code %d and body %s", url, resp.StatusCode, body)
					err = errors.WrapPrefix(e, "error marshalling", 0)

					return err
				}
			}
		}

		return err

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	if err == nil {
		return nil
	}
	return err.(*errors.Error)
}

func Get(logger golib.Logger, url string, target interface{}) *errors.Error {
	err := backoff.Retry(func() error {

		var e error
		var err *errors.Error

		var resp *http.Response
		var req *http.Request

		req, e = http.NewRequest("GET", url, nil)
		err = errors.WrapPrefix(e, "error creating the request", 0)

		if err == nil {
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: time.Second * 10, Transport: &http.Transport{Dial: (&net.Dialer{Timeout: 5 * time.Second}).Dial, TLSHandshakeTimeout: 5 * time.Second}}
			resp, e = client.Do(req)
			err = errors.WrapPrefix(e, "error requesting url", 0)

			if err == nil {

				defer resp.Body.Close()

				if resp.StatusCode == 200 || resp.StatusCode == 202 {

					//var response Response
					e = json.NewDecoder(resp.Body).Decode(&target)
					err = errors.WrapPrefix(e, "error decoding json", 0)

					//logger.LogAIf2(e != nil, golib.WARN, golib.Data{Message: "error parsing json from request ( " + url + "): " + golib.TryError(e)})

					return err

				} else {

					body, e := ioutil.ReadAll(resp.Body)

					if e == nil {
						err = errors.Errorf("error requesting url - body:" + string(body))
					} else {
						err = errors.WrapPrefix(e, "error requesting url - not possible to parse the body", 0)
					}

					return err

				}
			}
		}

		return err

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	if err == nil {
		return nil
	}
	return err.(*errors.Error)
}
