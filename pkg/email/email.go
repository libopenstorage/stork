package email

import (
	"crypto/tls"

	gomail "gopkg.in/gomail.v2"
)

// Email contains details about email
type Email struct {
	Subject         string
	From            string
	To              []string
	Content         string
	EmailHostServer string
}

const (
	EmailPortKey = 25
)

// SendEmail sends email to recipients
func (email *Email) SendEmail() error {
	m := gomail.NewMessage()
	m.SetHeader("From", email.From)
	m.SetHeader("Subject", email.Subject)
	tos := email.To
	m.SetHeader("To", tos...)
	m.SetBody("text/html", email.Content)
	dialer := gomail.Dialer{Host: email.EmailHostServer, Port: EmailPortKey, Username: "", Password: ""}
	dialer.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	if err := dialer.DialAndSend(m); err != nil {
		return err
	}
	return nil
}
