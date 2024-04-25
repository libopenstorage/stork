package flashblade

type BladeItem struct {
	RawCapacity int64       `json:"raw_capacity"`
	Details     interface{} `json:"details"`
	Name        interface{} `json:"name"`
	ID          interface{} `json:"id"`
	Progress    interface{} `json:"progress"`
	Status      interface{} `json:"status"`
	Target      interface{} `json:"target"`
}

// Response represents the entire JSON response.
type Blades struct {
	Total struct {
		*BladeItem
	} `json:"total"`
	ContinuationToken interface{} `json:"continuation_token"`
	TotalItemCount    int         `json:"total_item_count"`
	Items             []BladeItem `json:"items"`
}
