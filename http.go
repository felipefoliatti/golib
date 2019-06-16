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
)

func Post(logger Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("POST", logger, url, obj, target, headers)
}
func Put(logger Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("PUT", logger, url, obj, target, headers)
}
func Patch(logger Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("PATCH", logger, url, obj, target, headers)
}
func Head(logger Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("HEAD", logger, url, obj, target, headers)
}

func request(method string, logger Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
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
			err = errors.WrapInner("error marshalling", e, 0)
		}

		if err != nil {
			return err
		}

		j := []byte(string(b.Bytes()))
		req, e = http.NewRequest(method, url, bytes.NewBuffer(j))
		err = errors.WrapInner("error creating the request", e, 0)

		if headers != nil {
			for key, value := range headers {
				req.Header.Add(key, value)
			}
		}

		if err == nil {
			req.Header.Set("Content-Type", "application/json")

			client := http.Client{Timeout: time.Duration(10 * time.Second)}
			resp, e = client.Do(req)
			err = errors.WrapInner("error requesting", e, 0)

			if err == nil {

				defer resp.Body.Close()

				if resp.StatusCode == 200 || resp.StatusCode == 202 {

					e = nil
					if target != nil {
						//var response Response
						e = json.NewDecoder(resp.Body).Decode(&target)
						err = errors.WrapInner("error decoding", e, 0)
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
					err = errors.WrapInner("error marshalling", e, 0)

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

func Get(logger Logger, url string, target interface{}, headers map[string]string) *errors.Error {
	err := backoff.Retry(func() error {

		var e error
		var err *errors.Error

		var resp *http.Response
		var req *http.Request

		req, e = http.NewRequest("GET", url, nil)
		err = errors.WrapInner("error creating the request", e, 0)

		if err == nil {
			req.Header.Set("Content-Type", "application/json")

			if headers != nil {
				for key, value := range headers {
					req.Header.Add(key, value)
				}
			}

			client := &http.Client{Timeout: time.Second * 10, Transport: &http.Transport{Dial: (&net.Dialer{Timeout: 5 * time.Second}).Dial, TLSHandshakeTimeout: 5 * time.Second}}
			resp, e = client.Do(req)
			err = errors.WrapInner("error requesting url", e, 0)

			if err == nil {

				defer resp.Body.Close()

				if resp.StatusCode == 200 || resp.StatusCode == 202 {

					//var response Response
					e = json.NewDecoder(resp.Body).Decode(&target)
					err = errors.WrapInner("error decoding json", e, 0)

					//logger.LogAIf2(e != nil, WARN, Data{Message: "error parsing json from request ( " + url + "): " + TryError(e)})

					return err

				} else {

					body, e := ioutil.ReadAll(resp.Body)

					if e == nil {
						err = errors.Errorf("error requesting url - body:" + string(body))
					} else {
						err = errors.WrapInner("error requesting url - not possible to parse the body", e, 0)
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
