package mysql

import (
	"testing"
	"context"
	"os"
	"time"
)

func TestConnect(t *testing.T) {

	_, e := Connect("root", "", "test", "db")
	if e != nil {
		t.Errorf("%v", e)
	}
}


func TestSchema(t *testing.T) {

	var schemaFile = "schema_test.sql"
	ctx, _:= context.WithTimeout(context.Background(), 10*time.Second)

	db, e := Connect("root", "", "test", "db")
	if e != nil {
		t.Errorf("%v", e)
	}

	// Open the file
	file, e := os.Open(schemaFile)
	if e != nil {
		t.Errorf("%v", e)
	}
	defer file.Close()
	

	e = db.Schema(ctx, file)
	if e != nil {
		t.Errorf("%v", e)
	}

	
}
