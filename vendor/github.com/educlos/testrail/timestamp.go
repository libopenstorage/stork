package testrail

import (
	"encoding/json"
	"time"
)

// UNIX timestamp
type timestamp struct {
	time.Time
}

func (ts *timestamp) UnmarshalJSON(data []byte) error {
	var seconds int64
	if err := json.Unmarshal(data, &seconds); err != nil {
		return err
	}
	ts.Time = time.Unix(seconds, 0)
	return nil
}
