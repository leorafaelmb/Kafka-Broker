package protocol

type APIKeyHandler struct {
	Name    string
	Handler func(header RequestHeader) []byte
}

func (b *Broker) APIDispatcher(requestAPIKey int16) APIKeyHandler {
	switch requestAPIKey {
	case ApiVersionsKey:
		return APIKeyHandler{Name: "ApiVersions", Handler: getAPIVersionsResponse}
	case DescribeTopicPartitionsKey:
		return APIKeyHandler{Name: "DescribeTopicPartitions", Handler: getDescribeTopicPartitionsResponse}
	default:
		return APIKeyHandler{}
	}

}
