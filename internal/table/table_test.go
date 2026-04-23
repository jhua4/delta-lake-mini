package table_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jhua4/delta-lake-mini/internal/schema"
	"github.com/jhua4/delta-lake-mini/internal/table"
)

// testSchema returns a simple schema for use in tests.
func testSchema() *schema.Schema {
	return &schema.Schema{
		Fields: []schema.Field{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "score", Type: "float64", Nullable: true},
		},
	}
}

func TestCreateAndWrite(t *testing.T) {
	dir := t.TempDir()

	tbl, err := table.Create(dir, testSchema())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	req := table.WriteRequest{
		Schema: testSchema(),
		Rows: []table.Row{
			{"id": 1, "name": "alice", "score": 9.5},
			{"id": 2, "name": "bob", "score": 8.1},
		},
	}
	if err := tbl.Write(req); err != nil {
		t.Fatalf("Write: %v", err)
	}

	v, err := tbl.Version()
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}
}

func TestTimeTravel(t *testing.T) {
	dir := t.TempDir()

	tbl, err := table.Create(dir, testSchema())
	if err != nil {
		t.Fatal(err)
	}

	// Write v1: 2 rows
	_ = tbl.Write(table.WriteRequest{
		Schema: testSchema(),
		Rows:   []table.Row{{"id": 1, "name": "alice", "score": 9.5}},
	})
	// Write v2: 1 more file
	_ = tbl.Write(table.WriteRequest{
		Schema: testSchema(),
		Rows:   []table.Row{{"id": 2, "name": "bob", "score": 8.1}},
	})

	// v1 snapshot should have 1 file; v2 should have 2 files.
	r1, err := tbl.ReadVersion(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Files) != 1 {
		t.Errorf("v1: expected 1 file, got %d", len(r1.Files))
	}

	r2, err := tbl.ReadVersion(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Files) != 2 {
		t.Errorf("v2: expected 2 files, got %d", len(r2.Files))
	}
}

func TestSchemaEnforcement(t *testing.T) {
	dir := t.TempDir()

	tbl, err := table.Create(dir, testSchema())
	if err != nil {
		t.Fatal(err)
	}

	// Write with a wrong column type for "id" (string instead of int64).
	badSchema := &schema.Schema{
		Fields: []schema.Field{
			{Name: "id", Type: "string"}, // wrong type
			{Name: "name", Type: "string", Nullable: true},
		},
	}
	err = tbl.Write(table.WriteRequest{
		Schema: badSchema,
		Rows:   []table.Row{{"id": "oops", "name": "alice"}},
	})
	if err == nil {
		t.Error("expected schema validation error, got nil")
	}
}

func TestOptimize(t *testing.T) {
	dir := t.TempDir()

	tbl, err := table.Create(dir, testSchema())
	if err != nil {
		t.Fatal(err)
	}

	// Write 3 separate files.
	for i := 0; i < 3; i++ {
		_ = tbl.Write(table.WriteRequest{
			Schema: testSchema(),
			Rows:   []table.Row{{"id": i, "name": "x", "score": float64(i)}},
		})
	}

	res, err := tbl.Optimize()
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if res.FilesRemoved != 3 {
		t.Errorf("expected 3 files removed, got %d", res.FilesRemoved)
	}
	if res.FilesAdded != 1 {
		t.Errorf("expected 1 file added, got %d", res.FilesAdded)
	}

	// After optimize, snapshot should have exactly 1 file.
	r, err := tbl.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Files) != 1 {
		t.Errorf("expected 1 file after optimize, got %d", len(r.Files))
	}
}

func TestOpenExisting(t *testing.T) {
	dir := t.TempDir()

	tbl, _ := table.Create(dir, testSchema())
	_ = tbl.Write(table.WriteRequest{
		Schema: testSchema(),
		Rows:   []table.Row{{"id": 42, "name": "reopen", "score": 1.0}},
	})

	// Re-open the table (simulates a different process).
	tbl2, err := table.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	v, _ := tbl2.Version()
	if v != 1 {
		t.Errorf("expected version 1 after reopen, got %d", v)
	}
}

func TestLogFilesArePresent(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := table.Create(dir, testSchema())
	_ = tbl.Write(table.WriteRequest{
		Schema: testSchema(),
		Rows:   []table.Row{{"id": 1, "name": "a", "score": 1.0}},
	})

	// There should be commit files in _delta_log/.
	entries, err := os.ReadDir(filepath.Join(dir, "_delta_log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 { // version 0 (Create) + version 1 (Write)
		t.Errorf("expected at least 2 commit files, got %d", len(entries))
	}
}
