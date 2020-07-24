package email

import (
	"fmt"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

// Email contains details about email
type Email struct {
	Subject        string
	From           string
	To             []string
	Content        string
	SendGridAPIKey string
}

const (
	templateID       = "93bb7323-6314-4075-b2ee-a500dbae2d99"
	sendgridHost     = "https://api.sendgrid.com"
	sendgridEndpoint = "/v3/mail/send"
)

// SendEmail sends email to recipients
func (email *Email) SendEmail() error {
	from := mail.NewEmail("", email.From)
	m := mail.NewV3Mail()
	m.SetFrom(from)
	m.Subject = email.Subject
	tos := []*mail.Email{}
	for _, toEmailID := range email.To {
		tos = append(tos, mail.NewEmail("User", toEmailID))
	}
	p := mail.NewPersonalization()
	content := mail.NewContent("text/html", email.Content)
	p.AddTos(tos...)
	m.AddContent(content)
	m.AddPersonalizations(p)
	m.SetTemplateID(templateID)

	request := sendgrid.GetRequest(email.SendGridAPIKey, sendgridEndpoint, sendgridHost)
	request.Method = "POST"
	request.Body = mail.GetRequestBody(m)
	response, err := sendgrid.API(request)
	if err != nil {
		return fmt.Errorf("Error while sending email. Response:[%+v], Error:[%v]", response, err)
	}
	return nil
}
