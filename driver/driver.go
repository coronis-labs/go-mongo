package driver

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

/*
Client Object
*/
type Client struct {
	cl *mongo.Client
	cr *Credentials
	db *mongo.Database
	co *mongo.Collection
	u  string
}

/*
Credentials object
Contains user and password
*/
type Credentials struct {
	username string
	password string
}

/*
Create a new Client object

	string: username to authenticate with
	string: password to authenticate with
	string: url of db after the credentials. ex: @dbname.smchw.mongodb.net/test

Returns:

	*Client pointer to a client object
*/
func NewClient(_username string, _password string, _url string) *Client {
	client := Client{
		u: _url,
		cr: &Credentials{
			username: _username,
			password: _password,
		},
	}
	return &client
}

/*
Creates a Connection to the database

Returns:

	a boolean - bool

	an err - error
*/
func (c *Client) Connect() error {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c.cl, err = mongo.Connect(ctx, options.Client().ApplyURI(`mongodb+srv://`+c.cr.username+`:`+c.cr.password+c.u))
	if err != nil {
		return err
	}
	return nil
}

/*
Disconnect Client

Returns:

	a boolean - bool

	an err - error
*/
func (c *Client) Disconnect() (bool, error) {
	if err := c.cl.Disconnect(context.TODO()); err != nil {
		return false, err
	}
	return true, nil
}

/*
Ping Server to make sure its connected
We use this to make sure we are connected to the server before making any changes

Returns:

	a boolean - bool

	an err - error
*/
func (c *Client) Ping() error {
	err := c.cl.Ping(context.TODO(), readpref.Primary())
	if err != nil { // if ping fails
		err = c.Connect() // try to reconnect
		if err != nil {   // if that fails return the error
			return err
		}
	}
	return err
}

/*
Sets the database we want to access
*/
func (c *Client) SetDatabase(db_name string) {
	c.db = c.cl.Database(db_name)
}

/*
Sets the collection we want to access
If no database has been set yet it should return an error

Returns:

	a boolean  - bool

	an err - error
*/
func (c *Client) SetCollection(cl_name string) (bool, error) {
	if c.db == nil {
		return false, errors.New("please set a database before setting a collection")
	} else {
		c.co = c.db.Collection(cl_name)
		return true, nil
	}
}

/*
Finds an object from the collection using a filter and returns it

	interface{} filter to query object by

Returns:

	an interface object - interface{}
*/
func (c *Client) FindOne(filter interface{}) *mongo.SingleResult {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}

	return c.co.FindOne(context.Background(), filter)
}

/*
Finds many objects by a filter in the collection and returns it

	interface{} filter to query objects by

	interface{} options to query collection with

Returns:

	an array of interfaces - []interface{}
*/
func (c *Client) FindMany(filter interface{}, options *options.FindOptions) *mongo.Cursor {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}

	cursor, err := c.co.Find(context.Background(), filter, options)
	// if there is an error return nil
	if err != nil {
		return nil
	}
	return cursor
}

/*
Insert one object into the collection and return the object

	interface{} object to insert in collection

	interface{} options to inserting into the collection

Returns:

	an object - interface{}
*/
func (c *Client) InsertOne(object interface{}, options *options.InsertOneOptions) any {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}
	_, err := c.co.InsertOne(context.Background(), object, options)
	if err != nil { // we try again
		_, err := c.co.InsertOne(context.Background(), object, options)
		if err != nil {
			return err
		}
	}
	return object
}

/*
TODO: NOT IMPLEMENTED
Insert one object into the collection and return the object

	interface{} objects to insert in collection

	interface{} options to inserting into the collection

Returns:

	an object - interface{}
*/
func (c *Client) InsertMany(objects []interface{}, options *options.InsertManyOptions) any {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}
	return nil
}

/*
Update one object from the collection

	interface{} filter to query objects by

	interface{} update changes to made to the document

	interface{} options to update the collection with

Returns:

	the updated object - interface{}
*/
func (c *Client) UpdateOne(filter interface{}, update interface{}, options *options.UpdateOptions) *mongo.SingleResult {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}
	_, err := c.co.UpdateOne(context.Background(), filter, update, options)
	if err != nil { // try again
		_, err := c.co.UpdateOne(context.Background(), filter, update, options)
		if err != nil {
			return nil
		}
	}
	return c.FindOne(filter)
}

/*
Update one object from the collection

	interface{} filter to query objects by

	interface{} update changes to made to the document

	interface{} options to update the collection with

Returns:

	the updated object - interface{}
*/
func (c *Client) UpdateMany(filter interface{}, updates interface{}, options *options.UpdateOptions) any {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}
	return nil
}

/*
Remove one object from the collection

	interface{} filter to query object by

	interface{} options to delete object from the collection with

	returns a boolean if successful

Returns:

	boolean - bool
*/
func (c *Client) RemoveOne(filter interface{}, options *options.DeleteOptions) bool {
	// ping database
	if err := c.Ping(); err != nil {
		return false
	}
	_, err := c.co.DeleteOne(context.Background(), filter, options)
	if err != nil { // try again
		_, err := c.co.DeleteOne(context.Background(), filter, options)
		if err != nil {
			return false
		}
	}
	return true
}

/*
Remove one object from the collection

	interface{} filter to query objects by

	interface{} options to delete object from the collection with

	returns a boolean if successful

Returns:

	boolean - bool
*/
func (c *Client) RemoveMany(filter interface{}, options *options.DeleteOptions) bool {
	// ping database
	if err := c.Ping(); err != nil {
		return false
	}
	_, err := c.co.DeleteMany(context.Background(), filter, options)
	if err != nil { // try again
		_, err := c.co.DeleteMany(context.Background(), filter, options)
		if err != nil {
			return false
		}
	}
	return true
}

/*
Remove one object from the collection

	interface{} filter to query objects by

	interface{} options to delete object from the collection with

	returns a boolean if successful

Returns:

	boolean - bool
*/
func (c *Client) ReplaceOne(filter interface{}, replacement interface{}, options *options.ReplaceOptions) *mongo.SingleResult {
	// ping database
	if err := c.Ping(); err != nil {
		return nil
	}
	_, err := c.co.ReplaceOne(context.Background(), filter, replacement, options)
	if err != nil { // try again
		_, err := c.co.ReplaceOne(context.Background(), filter, replacement, options)
		if err != nil {
			return nil
		}
	}
	return c.FindOne(filter)
}
