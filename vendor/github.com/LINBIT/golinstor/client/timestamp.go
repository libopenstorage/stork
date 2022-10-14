package client

import (
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

type TimeStampMs struct {
	time.Time
}

func (t *TimeStampMs) UnmarshalJSON(s []byte) (err error) {
	r := string(s)
	q, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		return err
	}
	t.Time = time.Unix(q/1000, (q%1000)*1_000_000)
	return nil
}

func (t TimeStampMs) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Unix() * 1000)
}

func (t TimeStampMs) EncodeValues(key string, v *url.Values) error {
	v.Add(key, t.Format("20060102_150405"))
	return nil
}
