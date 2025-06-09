package llm

// Model represents the metadata for a single AI model that our platform supports.
type Model struct {
	Model       string // The actual ID used in API calls, e.g., "qwen-plus". This is the unique identifier.
	Type        string // ModelTypeChat or ModelTypeEmbedding
	Provider    string // The provider type, e.g., ProviderTongyi
	Description string // A brief description for the UI (Optional, can be removed if not needed)
}

// modelCatalog is our central, in-code registry of all supported models.
var modelCatalog = make(map[string][]*Model)

// RegisterModels allows different provider files to register their models.
func RegisterModels(provider string, models []*Model) {
	if _, exists := modelCatalog[provider]; exists {
		// In a real-world scenario, you might want to handle this more gracefully.
		panic("provider " + provider + " is already registered")
	}
	modelCatalog[provider] = models
}

// GetModelCatalog returns a copy of the entire model catalog.
func GetModelCatalog() map[string][]*Model {
	// Return a copy to prevent external modification.
	catCopy := make(map[string][]*Model)
	for k, v := range modelCatalog {
		catCopy[k] = v
	}
	return catCopy
}
