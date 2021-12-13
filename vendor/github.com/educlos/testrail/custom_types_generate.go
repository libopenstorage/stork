package testrail

import (
	"html/template"
	"os"
	"strings"
)

type resultField struct {
	ResultField
}

// Determine the Go type that matches the TestRail type
func (f *resultField) GoType() string {
	switch f.TypeID {
	case 1: // String
		return "string"
	case 2: // Integer
		return "int"
	case 3: // Text
		return "string"
	case 4: // URL
		return "string"
	case 5: // Checkbox
		return "bool"
	case 6: // Dropdown
		return "int"
	case 7: // User
		return "int"
	case 8: // Date
		return "timestamp"
	case 9: // Milestone
		return "int" //"milestone"
	case 11: // StepResults
		return "[]CustomStepResult"
	case 12: // MultiSelect
		return "int"
	default:
		return "interface{}"
	}
}

// Make a valid Go name from a TestRail field name
func (f *resultField) GoSystemName() string {
	camelCase := func(s string) string {
		return strings.Replace(strings.Title(strings.Replace(s, "_", " ", -1)), " ", "", -1)
	}
	return camelCase(f.SystemName)
}

type status struct {
	Status
}

// Make a valid Go name from a TestRail status name
func (s *status) GoName() string {
	return strings.Replace(s.Label, " ", "", -1)
}

func (c *Client) GenerateCustom() error {
	custom := struct {
		Statuses     []status
		ResultFields []resultField
	}{}

	statuses, err := c.GetStatuses()
	if err != nil {
		return err
	}
	for _, s := range statuses {
		custom.Statuses = append(custom.Statuses, status{s})
	}

	fields, err := c.GetResultFields()
	if err != nil {
		return err
	}
	for _, f := range fields {
		if !f.IsActive {
			continue
		}
		custom.ResultFields = append(custom.ResultFields, resultField{f})
	}

	/////////////////////////////////////

	const templateName = "custom_types.tmpl"
	tmpl, err := template.New(templateName).ParseFiles(templateName)
	if err != nil {
		return err
	}

	f, err := os.Create("custom_types.go")
	if err != nil {
		return err
	}
	defer f.Close()

	err = tmpl.Execute(f, custom)
	if err != nil {
		return err
	}

	return nil
}
