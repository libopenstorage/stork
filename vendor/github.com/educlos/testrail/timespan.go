package testrail

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Timespan field type.
// e.g. "1m" or "2m 30s" -- similar to time.Duration
//
// For a description, see:
// http://docs.gurock.com/testrail-api2/reference-results

type timespan struct {
	time.Duration
}

// TimespanFromDuration converts a standard Go time duration into a testrail compatible timespan.
func TimespanFromDuration(duration time.Duration) *timespan {
	if duration == 0 {
		return nil
	}

	return &timespan{duration}
}

// Unmarshal TestRail timespan into a time.Duration.
// Transform TestRail-specific formats into something time.ParseDuration understands:
//   "4h 5m 6s" => "4h5m6s"
//   "1d" => "8h"
//   "1w" => "40h"
//   "1d 2h" => "8h2h"
//   "1w 2d 3h" => "40h16h3h"
func (tsp *timespan) UnmarshalJSON(data []byte) error {
	const (
		// These are hardcoded in TestRail.
		// https://discuss.gurock.com/t/estimate-fields/900
		daysPerWeek = 5
		hoursPerDay = 8
	)
	var (
		err  error
		span string
	)

	if err = json.Unmarshal(data, &span); err != nil {
		return err
	}

	if span == "" {
		return nil
	}

	var parts []string
	for _, p := range strings.Fields(span) {
		if len(p) < 2 {
			return fmt.Errorf("%q: sequence is too short", p)
		}
		amount, err := strconv.Atoi(p[:len(p)-1])
		if err != nil {
			return fmt.Errorf("%q: cannot convert to int: %v", amount, err)
		}
		unit := p[len(p)-1]
		switch unit {
		case 'd':
			unit = 'h'
			amount *= hoursPerDay
		case 'w':
			unit = 'h'
			amount *= daysPerWeek * hoursPerDay
		}
		parts = append(parts, fmt.Sprintf("%v%c", amount, unit))
	}

	tsp.Duration, err = time.ParseDuration(strings.Join(parts, ""))
	if err != nil {
		return fmt.Errorf("%q: %v", span, err)
	}
	return nil
}

func (tsp timespan) MarshalJSON() ([]byte, error) {
	d := tsp.Duration

	if d == 0 {
		return []byte(`null`), nil
	}

	h, d := d/time.Hour, d%time.Hour
	m, d := d/time.Minute, d%time.Minute
	s := d / time.Second

	return []byte(fmt.Sprintf(`"%dh %dm %ds"`, h, m, s)), nil
}
