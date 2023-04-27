package testrail

// ResultField represents a ResultField
type ResultField struct {
	Configs      []ResultFieldConfig `json:"configs"`
	Description  string              `json:"description"`
	DisplayOrder int                 `json:"display_order"`
	ID           int                 `json:"ID"`
	Label        string              `json:"label"`
	Name         string              `json:"name"`
	SystemName   string              `json:"system_name"`
	TypeID       int                 `json:"type_id"`
	IsActive     bool                `json:"is_active"`
}

// ResultFieldConfig represents a config
// a ResultField can have
type ResultFieldConfig struct {
	Context Context           `json:"context"`
	ID      string            `json:"id"`
	Options ResultFieldOption `json:"options"`
}

// ResultFieldOption represents an option
// a ResultField can have
type ResultFieldOption struct {
	Format      string `json:"format"`
	HasActual   bool   `json:"has_actual"`
	HasExpected bool   `json:"has_expected"`
	IsRequired  bool   `json:"is_required"`
}

// GetResultFields returns a list of available test result custom fields
func (c *Client) GetResultFields() ([]ResultField, error) {
	caseFields := []ResultField{}
	err := c.sendRequest("GET", "get_result_fields", nil, &caseFields)
	return caseFields, err
}
