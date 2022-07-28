package yag

import "strings"

type variable struct {
	flag *wrapper

	name    string
	envName string
	help    string

	required  bool
	parseFlag bool
	parseEnv  bool
}

func (v *variable) usage() string {
	u := make([]string, 1, 10)
	u[0] = "\t"
	if v.parseFlag {
		u = append(u, "-", v.name)
	}
	if v.parseEnv && v.parseFlag {
		u = append(u, " ($", v.envName, ")")
	}
	if v.parseEnv && !v.parseFlag {
		u = append(u, "$", v.envName)
	}
	if v.required {
		u = append(u, " [required]")
	}
	u = append(u, "\n\t\t", v.help)

	return strings.Join(u, "")
}
