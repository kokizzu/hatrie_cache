package hatJournal

import "hatrie_cache/hat/hatCommand"

// Record is one durable command-journal entry. Command semantics are defined
// by hatCommand so recovery tooling can inspect the stable wire contract.
type Record struct {
	Sequence uint64             `json:"sequence"`
	Request  hatCommand.Request `json:"request"`
}
