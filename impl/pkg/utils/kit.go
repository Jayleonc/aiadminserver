package utils

func StringMapToAnyMap(src map[string]string) map[string]any {
	res := make(map[string]any, len(src))
	for k, v := range src {
		res[k] = v
	}
	return res
}
