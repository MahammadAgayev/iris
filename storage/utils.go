package storage

import "path/filepath"

func min(p1 int, p2 int) int {
	if p1 > p2 {
		return p1
	}

	return p2
}

func FileNameWithoutExtension(fileName string) string {
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}
