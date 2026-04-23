package utils

func IsInt(f float64) bool {
	return f == float64(int64(f))
}
