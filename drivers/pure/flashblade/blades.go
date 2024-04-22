package flashblade

type BladesService struct {
	client *Client
}

// Get points to GetArray for compatibility
func (b *BladesService) Get(data interface{}) ([]Blades, error) {
	return b.GetBlades(nil, data)
}

func (b *BladesService) GetBlades(params map[string]string, data interface{}) ([]Blades, error) {
	req, _ := b.client.NewRequest("GET", "blades", params, data)
	m := []Blades{}
	_, err := b.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// GetBladeTotal Get List of all Blades
func (b *BladesService) GetBladeTotal() ([]Blades, error) {
	params := make(map[string]string)
	params["total_only"] = "true"
	req, _ := b.client.NewRequest("GET", "blades", params, nil)
	m := []Blades{}
	_, err := b.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}
