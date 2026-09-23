package hatPeer

import (
	"bytes"
	"context"
	"errors"
)

var (
	// ErrCompactPeerRoleAuthorizerNil indicates that a nil role authorizer was used.
	ErrCompactPeerRoleAuthorizerNil = errors.New("hatPeer: compact peer role authorizer is nil")
	// ErrCompactPeerRolePolicyInvalid indicates an empty or malformed policy.
	ErrCompactPeerRolePolicyInvalid = errors.New("hatPeer: compact peer role policy is invalid")
	// ErrCompactPeerRequestDenied indicates that no role rule permits a request.
	ErrCompactPeerRequestDenied = errors.New("hatPeer: compact peer request denied")
)

// CompactPeerAuthorizationRequest contains the server-side identity and the
// request being authorized. Command and payload are borrowed for the duration
// of the callback and must be treated as read-only.
type CompactPeerAuthorizationRequest struct {
	PeerID  string
	Roles   []string
	Command []byte
	Payload []byte
	Space   string
}

// CompactPeerSpaceExtractor obtains the logical space name from one request.
// It is only called when CompactPeerSessionOptions.AuthorizeRequest is set.
// The frame is borrowed and must not be retained.
type CompactPeerSpaceExtractor func(ctx context.Context, request CompactFrame) (string, error)

// CompactPeerRequestAuthorizer applies request-level authorization after the
// connection-level listener authorization and before the request handler.
type CompactPeerRequestAuthorizer func(ctx context.Context, request CompactPeerAuthorizationRequest) error

// CompactPeerRoleRule grants one role access to one command and optionally one
// space. An empty Space or "*" matches every space; Command "*" matches every
// command. Role names are always exact matches.
type CompactPeerRoleRule struct {
	Role    string
	Command string
	Space   string
}

// CompactPeerRoleAuthorizerOptions configures a default-deny role policy.
type CompactPeerRoleAuthorizerOptions struct {
	Rules []CompactPeerRoleRule
}

type compactPeerRoleRule struct {
	role       string
	command    []byte
	anyCommand bool
	space      string
	anySpace   bool
}

// CompactPeerRoleAuthorizer evaluates exact role, command, and space rules.
// Roles are supplied by trusted server-side session configuration; they must
// not be copied from an untrusted request payload.
type CompactPeerRoleAuthorizer struct {
	rules []compactPeerRoleRule
}

// NewCompactPeerRoleAuthorizer creates a default-deny role authorizer.
func NewCompactPeerRoleAuthorizer(options CompactPeerRoleAuthorizerOptions) (*CompactPeerRoleAuthorizer, error) {
	if len(options.Rules) == 0 {
		return nil, ErrCompactPeerRolePolicyInvalid
	}
	rules := make([]compactPeerRoleRule, 0, len(options.Rules))
	for _, rule := range options.Rules {
		if rule.Role == "" || rule.Command == "" || rule.Role == "*" {
			return nil, ErrCompactPeerRolePolicyInvalid
		}
		normalized := compactPeerRoleRule{
			role:       rule.Role,
			anyCommand: rule.Command == "*",
			space:      rule.Space,
			anySpace:   rule.Space == "" || rule.Space == "*",
		}
		if !normalized.anyCommand {
			normalized.command = []byte(rule.Command)
		}
		rules = append(rules, normalized)
	}
	return &CompactPeerRoleAuthorizer{rules: rules}, nil
}

// Authorize permits a request when at least one configured role rule matches.
func (authorizer *CompactPeerRoleAuthorizer) Authorize(ctx context.Context, request CompactPeerAuthorizationRequest) error {
	if authorizer == nil {
		return ErrCompactPeerRoleAuthorizerNil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if len(request.Roles) == 0 || len(request.Command) == 0 {
		return ErrCompactPeerRequestDenied
	}
	for _, role := range request.Roles {
		for _, rule := range authorizer.rules {
			if role != rule.role || (!rule.anyCommand && !bytes.Equal(rule.command, request.Command)) {
				continue
			}
			if rule.anySpace || rule.space == request.Space {
				return nil
			}
		}
	}
	return ErrCompactPeerRequestDenied
}
