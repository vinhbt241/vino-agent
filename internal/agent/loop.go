package agent

// buildMessages assembles the message list for the first LLM call: an optional
// system prompt followed by the caller-supplied history.
func buildMessages(in RunInput) []Message {
	msgs := make([]Message, 0, len(in.Messages)+1)
	if in.SystemPrompt != "" {
		msgs = append(msgs, Message{Role: RoleSystem, Content: in.SystemPrompt})
	}
	msgs = append(msgs, in.Messages...)
	return msgs
}
