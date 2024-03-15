package asynq

type Metadata map[string]string

func (a Metadata) Get(key string) string {
	v, ok := a[key]
	if !ok {
		return ""
	}
	return v
}

func (a Metadata) Set(key string, value string) {
	a[key] = value
}

func (a Metadata) Keys() []string {
	i := 0
	r := make([]string, len(a))

	for k := range a {
		r[i] = k
		i++
	}

	return r
}
