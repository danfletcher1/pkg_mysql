package mysql

/*
	Name: Dan Fletcher
	Date: 04/05/18
	Title: MSQL Connector
	Codebase:
	Description:

	This provides a method to simplify communications with SQL databases
	This is a generic sql query package, here we are using the mysql driver but can be swapped out for postgres or others if required
	It provides just query, add, delete, update solution here.

	I provide 2 methods of query, old style that allows you to write any query, and prepare statement which is restricted to simple select.
	the simple query uses a string, the prepare style uses single map[string]interface{} key/value pair array.
	Returned is always a map[int]map[string]interface{} each returned row having an int, and each containing key/value pairs for every column

	The add, update, delete all use a single map[string]interface{} key/value pair array. They must all include a unique(id) key
	Previous work has always used this approch and its a good way to extraplate the database layer away.

	USAGE:
	// Always connect first
	sql.Connect("myusername", "mypassword", "mydb", "127.0.0.1")
	defer sql.Close()


	// Old style Query
	// This risks SQL injection attacks but allows you to write any SQL statement
	// All items are returned as strings (thats the driver not me)
	sql.FetchAny("select * from mytable")


	// Prepare style Query
	// This is the prefered method, but only allows simple SQL selects statements
	// Returns all items as their type (only string/int are provisioned by the mysql driver)
	myval := make(map[string]interface{})
	myval["field1"] = 0
	myval["field6"] = 10
	sql.Fetch("mytable", myval)


	// Update Statement
	// Must include a unique key called id
	myval := make(map[string]string)
	myval["field1"] = "newvalue"
	myval["id"] = "1"
	sql.Update(myval, "mytable")


	// Insert Statement
	myval := make(map[string]string)
	myval["field1"] = false
	myval["field2"] = "0"
	myval["field3"] = "0"
	sql.Insert(myval, "mytable")


	// Delete Statement
	// Must include a uniqueue key called id, everything else is ignored
	myval = make(map[string]interface{})
	myval["id"] = 13
	sql.Delete(where, "mytable")

*/

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// this is a pointer to db not a value
type DB struct {
	db *sql.DB
}

// Connect function opens a new database connection this must be done before reading/writing to the database
func (d *DB) Connect(username, password, database, host string) error {
	// Attempt to open the database connection
	var err error
	d.db, err = sql.Open("mysql", username+":"+password+"@("+host+")/"+database)
	if err != nil {
		fmt.Println(err)
		return errors.New("Unable to connect to DB" + err.Error())
	}

	// Open doesn't open a connection. Validate DSN data:
	err = d.db.Ping()
	if err != nil {
		return errors.New("Unable to validate DB:DSN" + err.Error())
	}

	d.db.SetMaxIdleConns(20)
	d.db.SetMaxOpenConns(20)
	return nil
}

// Close should executed right after connect with the defer keyword
func (d *DB) Close() {
	d.db.Close()
}

// func (d *DB) Conn() (*db.Conn, error) {
// 	return d.db.Conn()
// }

// Fetch will do a select query and return as string or int
func (d *DB) Fetch(where map[string]interface{}, table string) (map[int]map[string]interface{}, error) {

	// start to build the insert statement
	i := 0
	sql := "SELECT * FROM " + table + " WHERE "
	// Prepare style SQL has placeholders ? for values and the values are added seporately
	// create an interface for the values as they may be any data type
	whereVals := make([]interface{}, len(where))

	// Loop adding the (k)ey and (v)alue pairs to the statements
	for k, v := range where {
		sql = sql + k + "=? AND "
		whereVals[i] = v
		i++
	}

	// trim that excess ,
	query := strings.TrimSuffix(sql, " AND ")

	// Execute the query (the easy bit)
	stmt, err := d.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Pull a list of rows
	rows, err := stmt.Query(whereVals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Pull the column headers
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// create an array of interfaces to load the return row into
	vals := make([]interface{}, len(cols))

	// create an array to return the data
	allRows := make(map[int]map[string]interface{})

	// starting with row 0
	r := 0
	// loop through each row and scan the values into vals
	for rows.Next() {

		// this will change to pointes not values
		// This should be necessary here but I can't figure it out
		for k, _ := range cols {
			vals[k] = &vals[k]
		}

		err = rows.Scan(vals...)
		if err != nil {
			return nil, err
		}
		// create a new row in allRows
		allRows[r] = make(map[string]interface{})

		// complete the key/value pairs for allRows
		for k, v := range vals {
			switch v.(type) {
			case int64:
				allRows[r][cols[k]] = v.(int64)
			case nil:
				allRows[r][cols[k]] = nil
			default:
				allRows[r][cols[k]] = string(v.([]byte))
			}
		}
		r++
	}
	return allRows, nil
}

// FetchAny uses the old style query giving more freedom but insecure, all returns are strings
func (d *DB) FetchAny(query string) (map[int]map[string]string, error) {
	// Execute the query (the easy bit)
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Pull the column headers
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// create an array of interfaces to load the return row into
	vals := make([]interface{}, len(cols))

	// create an array to return the data
	allRows := make(map[int]map[string]string)

	// starting with row 0
	r := 0
	// loop through each row and scan the values into vals
	for rows.Next() {
		// this will change to pointes not values
		// This should be necessary here but I can't figure it out
		for k, _ := range cols {
			vals[k] = &vals[k]
		}

		err = rows.Scan(vals...)
		if err != nil {
			return nil, err
		}
		// create a new row in allRows
		allRows[r] = make(map[string]string)

		// complete the key/value pairs for allRows
		for k, v := range vals {
			switch v.(type) {
			case nil:
				allRows[r][cols[k]] = ""
			default:
				allRows[r][cols[k]] = string(v.([]byte))
			}
		}
		r++
	}
	return allRows, nil
}

// Insert will insert the records, and return an error if there is a problem.
// it will return the new inserted ID and a count of records affected
func (d *DB) Insert(updates map[string]interface{}, table string) (id int64, count int64, err error) {

	// start to build the insert statement
	i := 0
	sql := "INSERT INTO " + table + " SET "
	// Prepare style SQL has placeholders ? for values and the values are added seporately
	// create an interface for the values as they may be any data type
	vals := make([]interface{}, len(updates))

	// Loop adding the (k)ey and (v)alue pairs to the statements
	for k, v := range updates {
		sql = sql + k + "=?,"
		vals[i] = v
		i++
	}

	// trim that excess ,
	query := strings.TrimSuffix(sql, ",")

	// execute the statement with vals values
	res, err := d.db.Exec(query, vals...)
	if err != nil {
		return 0, 0, err
	}

	// find the last insert ID
	lastId, err := res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}

	// count the rows affected, normally 1 for an insert
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}

	return lastId, rowCnt, nil
}

// Update will update the record, and return an error if there is a problem.
// it will return the count of records affected
func (d *DB) Update(updates map[string]interface{}, table string) (count int64, err error) {
	var id interface{}

	// Start to build the update statement
	i := 0
	sql := "UPDATE " + table + " SET "
	// Prepare style SQL has placeholders ? for values and the values are added seporately
	// create an interface for the values as they may be any data type
	vals := make([]interface{}, len(updates))

	// Loop adding the (k)ey and (v)alue pairs to the statements
	for k, v := range updates {
		if k == "id" {
			id = v
		} else {
			sql = sql + k + "=?,"
			vals[i] = v
			i++
		}
	}

	// trim the excess ,
	query := strings.TrimSuffix(sql, ",") + " WHERE id=?"
	vals[i] = id

	if id == "" {
		return 0, errors.New("Row [" + string(i) + "] is missing the ID, please always include a row ID column")
	}

	// execute the statement with vals values
	res, err := d.db.Exec(query, vals...)
	if err != nil {
		return 0, err
	}

	// count the rows affected, normally 1 for an insert
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowCnt, nil
}

// Delete will remove the records, and return an error if there is a problem.
// It expects a unique field called ID, and returns a count of records affected
func (d *DB) Delete(updates map[string]interface{}, table string) (count int64, err error) {

	// Prepare style SQL has placeholders ? for values and the values are added seporately
	// create an interface for the values as they may be any data type
	vals := make([]interface{}, 1)

	vals[0] = updates["id"]
	query := "DELETE FROM " + table + " WHERE id=?"

	// execute the statement with vals values
	res, err := d.db.Exec(query, vals...)
	if err != nil {
		return 0, err
	}

	// count the rows affected, normally 1 for an insert
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowCnt, nil
}
