package alg

import "golang.org/x/exp/constraints"

func MaxElemIfIdx[T any](arr []T, less func(lhs, rhs int) bool) int {
	result := -1
	if len(arr) <= 0 {
		return result
	}
	result = 0
	for i := 1; i < len(arr); i++ {
		if less(result, i) {
			result = i
		}
	}
	return result
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
