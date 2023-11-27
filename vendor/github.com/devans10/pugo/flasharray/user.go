/*
   Copyright 2018 David Evans

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package flasharray

import (
	"fmt"
)

// UserService struct for user API endpoints
type UserService struct {
	client *Client
}

// listUsers is the private function for returning dictionaries
// which describes remote access
func (n *UserService) listUsers(params map[string]string) ([]User, error) {

	req, _ := n.client.NewRequest("GET", "admin", params, nil)
	m := []User{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListAdmins lists attributes for Admins
func (n *UserService) ListAdmins() ([]User, error) {

	m, err := n.listUsers(nil)
	if err != nil {
		return nil, err
	}

	return m, err
}

// CreateAdmin creates an Admin
func (n *UserService) CreateAdmin(name string) (*User, error) {

	path := fmt.Sprintf("admin/%s", name)
	req, _ := n.client.NewRequest("POST", path, nil, nil)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteAdmin deletes an Admin
func (n *UserService) DeleteAdmin(name string) (*User, error) {

	path := fmt.Sprintf("admin/%s", name)
	req, _ := n.client.NewRequest("DELETE", path, nil, nil)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetAdmin modifies Admin Attributes
func (n *UserService) SetAdmin(name string, data interface{}) (*User, error) {

	path := fmt.Sprintf("admin/%s", name)
	req, _ := n.client.NewRequest("PUT", path, nil, data)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetAdmin lists attributes for specified Admin
func (n *UserService) GetAdmin(name string) (*User, error) {

	path := fmt.Sprintf("admin/%s", name)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetAPIToken returns an API Token
func (n *UserService) GetAPIToken(name string) (*Token, error) {

	path := fmt.Sprintf("admin/%s/apitoken", name)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &Token{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateAPIToken creates an API Token
func (n *UserService) CreateAPIToken(name string) (*Token, error) {

	path := fmt.Sprintf("admin/%s/apitoken", name)
	req, _ := n.client.NewRequest("POST", path, nil, nil)
	m := &Token{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteAPIToken deletes an  API Token
func (n *UserService) DeleteAPIToken(name string) (*Token, error) {

	path := fmt.Sprintf("admin/%s/apitoken", name)
	req, _ := n.client.NewRequest("DELETE", path, nil, nil)
	m := &Token{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListPublicKeys returns a list of public keys
func (n *UserService) ListPublicKeys() ([]User, error) {

	data := map[string]string{"publickey": "true"}
	m, err := n.listUsers(data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// ListAPITokens returns a list of API Tokens
func (n *UserService) ListAPITokens() ([]Token, error) {

	params := map[string]string{"api_token": "true"}
	req, _ := n.client.NewRequest("GET", "admin", params, nil)
	m := []Token{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, err
}

// RefreshAdmin refreshes the admin permission cache for the specified admin
func (n *UserService) RefreshAdmin(name string) (*User, error) {

	data := map[string]string{"action": "refresh"}
	m, err := n.SetAdmin(name, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// RefreshAdmins clear the admin permission cache.
func (n *UserService) RefreshAdmins() (*User, error) {
	data := make(map[string]interface{})
	data["action"] = "refresh"
	data["clear"] = true
	req, _ := n.client.NewRequest("PUT", "admin", nil, data)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetPublicKey modifies public key for the specified admin
func (n *UserService) SetPublicKey(name string, key string) (*User, error) {

	data := map[string]string{"publickey": key}
	m, err := n.SetAdmin(name, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// SetPassword sets the password for the specified admin
func (n *UserService) SetPassword(name string, newPassword string, oldPassword string) (*User, error) {

	data := map[string]string{"password": newPassword, "old_password": oldPassword}
	m, err := n.SetAdmin(name, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// GetGlobalAdminAttr returns a map describing the existing global admin attributes
func (n *UserService) GetGlobalAdminAttr() (*GlobalAdmin, error) {

	req, _ := n.client.NewRequest("GET", "admin/settings", nil, nil)
	m := &GlobalAdmin{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetGlobalAdminAttr modifies global admin attributes
func (n *UserService) SetGlobalAdminAttr(data interface{}) (*GlobalAdmin, error) {

	req, _ := n.client.NewRequest("PUT", "admin/settings", nil, data)
	m := &GlobalAdmin{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListAdminUser return a map describing lockout information for locked out admins
func (n *UserService) ListAdminUser() ([]User, error) {

	data := map[string]string{"lockout": "true"}
	m, err := n.listUsers(data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// GetAdminUser return a map describing lockout information specified admins
func (n *UserService) GetAdminUser(name string) (*User, error) {

	path := fmt.Sprintf("admin/%s", name)
	params := map[string]string{"lockout": "true"}
	req, _ := n.client.NewRequest("GET", path, params, nil)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// UnlockAdmin unlocks an admin
func (n *UserService) UnlockAdmin(name string) (*User, error) {

	path := fmt.Sprintf("admin/%s/lockout", name)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &User{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
