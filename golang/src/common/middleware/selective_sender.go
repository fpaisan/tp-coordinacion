package middleware

type SelectiveSender interface {
	Middleware
	SendTo(msg Message, routingKey string) error
}
