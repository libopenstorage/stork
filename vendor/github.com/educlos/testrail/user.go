package testrail

import "strconv"

// User represents a User
type User struct {
	Email    string `json:"email"`
	ID       int    `json:"id"`
	IsActive bool   `json:"is_active"`
	Name     string `json:"name"`
}

// GetUser returns the user userID
func (c *Client) GetUser(userID int) (User, error) {
	returnUser := User{}
	err := c.sendRequest("GET", "get_user/"+strconv.Itoa(userID), nil, &returnUser)
	return returnUser, err
}

// GetUserByEmail returns the user corresponding to email email
func (c *Client) GetUserByEmail(email string) (User, error) {
	returnUser := User{}
	err := c.sendRequest("GET", "get_user_by_email&email="+email, nil, &returnUser)
	return returnUser, err
}

// GetUsers returns the list of users
func (c *Client) GetUsers() ([]User, error) {
	returnUser := []User{}
	err := c.sendRequest("GET", "get_users", nil, &returnUser)
	return returnUser, err
}
