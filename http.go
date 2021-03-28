package golib

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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
func Delete(logger Logger, url string, obj interface{}, target interface{}, headers map[string]string) *errors.Error {
	return request("DELETE", logger, url, obj, target, headers)
}
func Get(logger Logger, url string, target interface{}, headers map[string]string) *errors.Error {
	return request("GET", logger, url, nil, target, headers)
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

					err = nil
					if target != nil {
						//var response Response
						e = json.NewDecoder(resp.Body).Decode(&target)
						err = errors.WrapInner("error decoding", e, 0)
					}
					return err

				} else {

					scanner := bufio.NewScanner(resp.Body)
					scanner.Split(bufio.ScanRunes)
					var buf bytes.Buffer

					for scanner.Scan() {
						buf.WriteString(scanner.Text())
					}

					body := buf.String()

					//try to decode if there is a target (suppress any error)
					if target != nil {
						b := strings.Replace(body, "'", "\"", -1)
						_ = json.Unmarshal([]byte(b), &target)
					} 

					//cleans up the string to print
					pbody := strings.Replace(body, "\"", "'", -1)
					var baseErr error

					//create a base error (or a caused by)
					if obj == nil {
						baseErr = fmt.Errorf("error in service - %s %s -> code %d and body %s", method, url, resp.StatusCode, pbody)
					} else {
						baseErr = fmt.Errorf("error in service - %s %s - body: %v -> code %d and body %s", method, url, obj, resp.StatusCode, pbody)
					}

					//try to convert to error
					type Response struct {
						Success bool `json:"success"`
						Code    *int `json:"code"`
						Detail  struct {
							Message *string `json:"message"`
						} `json:"detail"`
					}
					response := Response{}
					b := strings.Replace(body, "'", "\"", -1)
					e = json.Unmarshal([]byte(body), &response)
				
					//check if the error was parsed
					if e == nil && response.Code != nil && response.Detail.Message != nil{
						//if possible to decode the error
						err = errors.WrapInnerWithCode(*response.Detail.Message, *response.Code, baseErr, 0)
					} else {
						//in case of not being able to decode the error
						err = errors.WrapInner("error marshalling", baseErr, 0)
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
