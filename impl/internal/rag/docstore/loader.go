// internal/modelrag/docstore/loader.go
package docstore

import (
	"fmt"
	einoschema "github.com/cloudwego/eino/schema"
	"io"
)

func LoadEinoDocsFromFile(r io.Reader, ext string) ([]*einoschema.Document, error) {
	switch ext {
	case ".csv":
		return LoadAndSplitCSV(r, 200, 20)
	// case ".txt": return LoadAndSplitText(...)
	// case ".pdf": return LoadAndSplitPDF(...)
	default:
		return nil, fmt.Errorf("unsupported file type: %s", ext)
	}
}
