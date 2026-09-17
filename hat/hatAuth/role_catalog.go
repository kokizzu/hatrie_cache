package hatAuth

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	ErrRoleCatalogNil           = errors.New("hatriecache: role catalog is required")
	ErrRoleCatalogInvalid       = errors.New("hatriecache: invalid role catalog resource")
	ErrRoleCatalogAlreadyExists = errors.New("hatriecache: role catalog resource already exists")
	ErrRoleCatalogNotFound      = errors.New("hatriecache: role catalog resource not found")
	ErrRoleCatalogAccessDenied  = errors.New("hatriecache: role catalog access denied")
	ErrRoleCatalogLimit         = errors.New("hatriecache: role catalog limit exceeded")
	ErrRoleCatalogInUse         = errors.New("hatriecache: role catalog resource is in use")
	ErrRoleCatalogConflict      = errors.New("hatriecache: role catalog version conflict")
	ErrRoleCatalogCycle         = errors.New("hatriecache: role catalog hierarchy cycle")
)

const (
	DefaultRoleCatalogMaxRoles          = 1024
	DefaultRoleCatalogMaxNamespaces     = 4096
	DefaultRoleCatalogMaxGrants         = 16384
	DefaultRoleCatalogMaxMemberships    = 16384
	DefaultRoleCatalogMaxRoleParents    = 16
	DefaultRoleCatalogMaxSelectors      = 64
	DefaultRoleCatalogMaxNameBytes      = 256
	DefaultRoleCatalogMaxPrincipalBytes = 256
)

// RoleCatalogOptions bounds the optional role and namespace catalog. Zero
// values select conservative defaults; negative values are rejected.
type RoleCatalogOptions struct {
	MaxRoles          int
	MaxNamespaces     int
	MaxGrants         int
	MaxMemberships    int
	MaxRoleParents    int
	MaxSelectors      int
	MaxNameBytes      int
	MaxPrincipalBytes int
}

type roleCatalogLimits struct {
	maxRoles          int
	maxNamespaces     int
	maxGrants         int
	maxMemberships    int
	maxRoleParents    int
	maxSelectors      int
	maxNameBytes      int
	maxPrincipalBytes int
}

// RoleCatalog is an opt-in, in-memory catalog for role ownership, role
// inheritance, namespace ownership, grants, and principal membership. It is
// independent from the legacy Policy type so existing authorization remains
// unchanged until a caller selects this catalog.
type RoleCatalog struct {
	mu              sync.RWMutex
	limits          roleCatalogLimits
	version         uint64
	nextGrantID     uint64
	membershipCount int
	roles           map[string]roleCatalogRole
	namespaces      map[string]roleCatalogNamespace
	grants          map[uint64]RoleGrant
	grantsByRole    map[string][]uint64
	memberships     map[string]map[string]RoleMembership
}

type roleCatalogRole struct {
	metadata RoleMetadata
}

type roleCatalogNamespace struct {
	metadata NamespaceMetadata
}

// RoleSpec defines a role and its inherited parent roles. The actor passed to
// CreateRole must equal Owner and own every parent role.
type RoleSpec struct {
	Name    string   `json:"name"`
	Owner   string   `json:"owner"`
	Parents []string `json:"parents,omitempty"`
}

// RoleMetadata is the redacted catalog representation of a role.
type RoleMetadata struct {
	Name    string   `json:"name"`
	Owner   string   `json:"owner"`
	Parents []string `json:"parents,omitempty"`
}

// NamespaceSpec defines a slash-delimited namespace and its optional parent.
// A child namespace inherits its owner's management scope from its parent.
type NamespaceSpec struct {
	Name   string `json:"name"`
	Parent string `json:"parent,omitempty"`
	Owner  string `json:"owner"`
}

// NamespaceMetadata is the catalog representation of a namespace.
type NamespaceMetadata struct {
	Name   string `json:"name"`
	Parent string `json:"parent,omitempty"`
	Owner  string `json:"owner"`
}

// RoleGrantSpec attaches one existing Rule to an existing role.
type RoleGrantSpec struct {
	Role string `json:"role"`
	Rule Rule   `json:"rule"`
}

// RoleGrant is an immutable grant record. Version is the catalog version that
// created the record.
type RoleGrant struct {
	ID        uint64 `json:"id"`
	Role      string `json:"role"`
	Rule      Rule   `json:"rule"`
	GrantedBy string `json:"granted_by"`
	Version   uint64 `json:"version"`
}

// RoleMembership records a principal-to-role assignment.
type RoleMembership struct {
	Principal string `json:"principal"`
	Role      string `json:"role"`
	GrantedBy string `json:"granted_by"`
	Version   uint64 `json:"version"`
}

// RoleCatalogSnapshot is deterministic and contains no hidden mutable maps.
// It can be persisted by the caller and restored after validating its version
// and hierarchy.
type RoleCatalogSnapshot struct {
	Version     uint64              `json:"version"`
	NextGrantID uint64              `json:"next_grant_id"`
	Roles       []RoleMetadata      `json:"roles,omitempty"`
	Namespaces  []NamespaceMetadata `json:"namespaces,omitempty"`
	Grants      []RoleGrant         `json:"grants,omitempty"`
	Memberships []RoleMembership    `json:"memberships,omitempty"`
}

// NewRoleCatalog creates a bounded, disabled-by-default authorization catalog.
func NewRoleCatalog(options RoleCatalogOptions) (*RoleCatalog, error) {
	limits, err := newRoleCatalogLimits(options)
	if err != nil {
		return nil, err
	}
	return &RoleCatalog{
		limits:       limits,
		roles:        make(map[string]roleCatalogRole),
		namespaces:   make(map[string]roleCatalogNamespace),
		grants:       make(map[uint64]RoleGrant),
		grantsByRole: make(map[string][]uint64),
		memberships:  make(map[string]map[string]RoleMembership),
	}, nil
}

func newRoleCatalogLimits(options RoleCatalogOptions) (roleCatalogLimits, error) {
	limits := roleCatalogLimits{}
	var err error
	if limits.maxRoles, err = roleCatalogLimit(options.MaxRoles, DefaultRoleCatalogMaxRoles, "roles"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxNamespaces, err = roleCatalogLimit(options.MaxNamespaces, DefaultRoleCatalogMaxNamespaces, "namespaces"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxGrants, err = roleCatalogLimit(options.MaxGrants, DefaultRoleCatalogMaxGrants, "grants"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxMemberships, err = roleCatalogLimit(options.MaxMemberships, DefaultRoleCatalogMaxMemberships, "memberships"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxRoleParents, err = roleCatalogLimit(options.MaxRoleParents, DefaultRoleCatalogMaxRoleParents, "role parents"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxSelectors, err = roleCatalogLimit(options.MaxSelectors, DefaultRoleCatalogMaxSelectors, "selectors"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxNameBytes, err = roleCatalogLimit(options.MaxNameBytes, DefaultRoleCatalogMaxNameBytes, "name bytes"); err != nil {
		return roleCatalogLimits{}, err
	}
	if limits.maxPrincipalBytes, err = roleCatalogLimit(options.MaxPrincipalBytes, DefaultRoleCatalogMaxPrincipalBytes, "principal bytes"); err != nil {
		return roleCatalogLimits{}, err
	}
	return limits, nil
}

func roleCatalogLimit(value, fallback int, label string) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: %s cannot be negative", ErrRoleCatalogInvalid, label)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}

// Version returns the current catalog mutation version. A nil catalog has
// version zero.
func (catalog *RoleCatalog) Version() uint64 {
	if catalog == nil {
		return 0
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	return catalog.version
}

// CreateRole creates a role owned by actor and optionally inheriting owned
// parent roles.
func (catalog *RoleCatalog) CreateRole(actor string, spec RoleSpec) (RoleMetadata, error) {
	if catalog == nil {
		return RoleMetadata{}, ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return RoleMetadata{}, err
	}
	name, err := normalizeRoleCatalogName(spec.Name, catalog.limits.maxNameBytes)
	if err != nil {
		return RoleMetadata{}, err
	}
	owner, err := catalog.normalizePrincipal(spec.Owner)
	if err != nil {
		return RoleMetadata{}, err
	}
	if actor != owner {
		return RoleMetadata{}, ErrRoleCatalogAccessDenied
	}
	parents, err := normalizeRoleParents(spec.Parents, catalog.limits)
	if err != nil {
		return RoleMetadata{}, err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, exists := catalog.roles[name]; exists {
		return RoleMetadata{}, ErrRoleCatalogAlreadyExists
	}
	if len(catalog.roles) >= catalog.limits.maxRoles {
		return RoleMetadata{}, ErrRoleCatalogLimit
	}
	for _, parent := range parents {
		parentRole, exists := catalog.roles[parent]
		if !exists {
			return RoleMetadata{}, fmt.Errorf("%w: parent role %q", ErrRoleCatalogNotFound, parent)
		}
		if parentRole.metadata.Owner != actor {
			return RoleMetadata{}, ErrRoleCatalogAccessDenied
		}
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return RoleMetadata{}, err
	}
	metadata := RoleMetadata{Name: name, Owner: owner, Parents: parents}
	catalog.roles[name] = roleCatalogRole{metadata: cloneRoleMetadata(metadata)}
	return cloneRoleMetadata(metadata), nil
}

// CreateNamespace creates a namespace owned by actor. A child must be below
// its declared parent using slash-delimited names.
func (catalog *RoleCatalog) CreateNamespace(actor string, spec NamespaceSpec) (NamespaceMetadata, error) {
	if catalog == nil {
		return NamespaceMetadata{}, ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return NamespaceMetadata{}, err
	}
	name, err := normalizeNamespaceCatalogName(spec.Name, catalog.limits.maxNameBytes)
	if err != nil {
		return NamespaceMetadata{}, err
	}
	owner, err := catalog.normalizePrincipal(spec.Owner)
	if err != nil {
		return NamespaceMetadata{}, err
	}
	if actor != owner {
		return NamespaceMetadata{}, ErrRoleCatalogAccessDenied
	}
	parent := strings.TrimSpace(spec.Parent)
	if spec.Parent != parent {
		return NamespaceMetadata{}, fmt.Errorf("%w: namespace parent whitespace", ErrRoleCatalogInvalid)
	}
	if parent != "" {
		if _, err := normalizeNamespaceCatalogName(parent, catalog.limits.maxNameBytes); err != nil {
			return NamespaceMetadata{}, err
		}
	} else if strings.Contains(name, "/") {
		return NamespaceMetadata{}, fmt.Errorf("%w: nested namespace requires a parent", ErrRoleCatalogInvalid)
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, exists := catalog.namespaces[name]; exists {
		return NamespaceMetadata{}, ErrRoleCatalogAlreadyExists
	}
	if len(catalog.namespaces) >= catalog.limits.maxNamespaces {
		return NamespaceMetadata{}, ErrRoleCatalogLimit
	}
	if parent != "" {
		parentEntry, exists := catalog.namespaces[parent]
		if !exists {
			return NamespaceMetadata{}, fmt.Errorf("%w: parent namespace %q", ErrRoleCatalogNotFound, parent)
		}
		if parentEntry.metadata.Owner != actor {
			return NamespaceMetadata{}, ErrRoleCatalogAccessDenied
		}
		if !namespaceCatalogDescendant(name, parent) {
			return NamespaceMetadata{}, fmt.Errorf("%w: namespace %q is outside parent %q", ErrRoleCatalogInvalid, name, parent)
		}
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return NamespaceMetadata{}, err
	}
	metadata := NamespaceMetadata{Name: name, Parent: parent, Owner: owner}
	catalog.namespaces[name] = roleCatalogNamespace{metadata: metadata}
	return metadata, nil
}

// GrantRole assigns role to principal. Only the role owner can change its
// membership.
func (catalog *RoleCatalog) GrantRole(actor, principal, role string) (RoleMembership, error) {
	if catalog == nil {
		return RoleMembership{}, ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return RoleMembership{}, err
	}
	principal, err = catalog.normalizePrincipal(principal)
	if err != nil {
		return RoleMembership{}, err
	}
	role, err = normalizeRoleCatalogName(role, catalog.limits.maxNameBytes)
	if err != nil {
		return RoleMembership{}, err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	roleEntry, exists := catalog.roles[role]
	if !exists {
		return RoleMembership{}, ErrRoleCatalogNotFound
	}
	if roleEntry.metadata.Owner != actor {
		return RoleMembership{}, ErrRoleCatalogAccessDenied
	}
	assigned := catalog.memberships[principal]
	if assigned == nil {
		assigned = make(map[string]RoleMembership)
	}
	if _, exists := assigned[role]; exists {
		return RoleMembership{}, ErrRoleCatalogAlreadyExists
	}
	if catalog.membershipCount >= catalog.limits.maxMemberships {
		return RoleMembership{}, ErrRoleCatalogLimit
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return RoleMembership{}, err
	}
	membership := RoleMembership{Principal: principal, Role: role, GrantedBy: actor, Version: catalog.version}
	assigned[role] = membership
	catalog.memberships[principal] = assigned
	catalog.membershipCount++
	return membership, nil
}

// Grant adds one validated rule to a role. Scoped namespace grants require
// the actor to own the referenced namespace as well as the role.
func (catalog *RoleCatalog) Grant(actor string, spec RoleGrantSpec) (RoleGrant, error) {
	if catalog == nil {
		return RoleGrant{}, ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return RoleGrant{}, err
	}
	role, err := normalizeRoleCatalogName(spec.Role, catalog.limits.maxNameBytes)
	if err != nil {
		return RoleGrant{}, err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	roleEntry, exists := catalog.roles[role]
	if !exists {
		return RoleGrant{}, ErrRoleCatalogNotFound
	}
	if roleEntry.metadata.Owner != actor {
		return RoleGrant{}, ErrRoleCatalogAccessDenied
	}
	normalizedRule, err := normalizeRoleCatalogRule(spec.Rule, catalog.limits, catalog.namespaces)
	if err != nil {
		return RoleGrant{}, err
	}
	if err := catalog.validateGrantNamespaceOwnershipLocked(actor, normalizedRule); err != nil {
		return RoleGrant{}, err
	}
	for _, grantID := range catalog.grantsByRole[role] {
		if existingGrant, exists := catalog.grants[grantID]; exists && sameRoleCatalogRule(existingGrant.Rule, normalizedRule) {
			return RoleGrant{}, ErrRoleCatalogAlreadyExists
		}
	}
	if len(catalog.grants) >= catalog.limits.maxGrants {
		return RoleGrant{}, ErrRoleCatalogLimit
	}
	if catalog.nextGrantID == ^uint64(0) {
		return RoleGrant{}, fmt.Errorf("%w: grant id overflow", ErrRoleCatalogInvalid)
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return RoleGrant{}, err
	}
	catalog.nextGrantID++
	grant := RoleGrant{ID: catalog.nextGrantID, Role: role, Rule: cloneRule(normalizedRule), GrantedBy: actor, Version: catalog.version}
	catalog.grants[grant.ID] = cloneRoleGrant(grant)
	catalog.grantsByRole[role] = append(catalog.grantsByRole[role], grant.ID)
	return cloneRoleGrant(grant), nil
}

// Authorize applies the catalog's default-deny role, grant, and namespace
// hierarchy rules. An empty catalog denies every request.
func (catalog *RoleCatalog) Authorize(principal string, request AuthorizationRequest) bool {
	if catalog == nil {
		return false
	}
	if !validRoleCatalogText(principal, catalog.limits.maxPrincipalBytes) {
		return false
	}
	principal = strings.TrimSpace(principal)
	if principal == "" || !validRoleCatalogRequest(request, catalog.limits.maxNameBytes) {
		return false
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	assigned := catalog.memberships[principal]
	if len(assigned) == 0 {
		return false
	}
	if len(assigned) == 1 {
		for roleName := range assigned {
			roleEntry, exists := catalog.roles[roleName]
			if !exists || len(roleEntry.metadata.Parents) != 0 {
				break
			}
			for _, grantID := range catalog.grantsByRole[roleName] {
				grant, exists := catalog.grants[grantID]
				if exists && roleCatalogRuleMatches(grant.Rule, request) {
					return true
				}
			}
			return false
		}
	}
	visited := make(map[string]struct{}, len(assigned))
	stack := make([]string, 0, len(assigned))
	for role := range assigned {
		stack = append(stack, role)
	}
	for len(stack) > 0 {
		last := len(stack) - 1
		roleName := stack[last]
		stack = stack[:last]
		if _, seen := visited[roleName]; seen {
			continue
		}
		roleEntry, exists := catalog.roles[roleName]
		if !exists {
			continue
		}
		visited[roleName] = struct{}{}
		for _, parent := range roleEntry.metadata.Parents {
			stack = append(stack, parent)
		}
		for _, grantID := range catalog.grantsByRole[roleName] {
			grant, exists := catalog.grants[grantID]
			if exists && roleCatalogRuleMatches(grant.Rule, request) {
				return true
			}
		}
	}
	return false
}

// CanManageRole reports whether principal owns the named role.
func (catalog *RoleCatalog) CanManageRole(principal, role string) bool {
	if catalog == nil {
		return false
	}
	if !validRoleCatalogText(principal, catalog.limits.maxPrincipalBytes) || !validRoleCatalogText(role, catalog.limits.maxNameBytes) {
		return false
	}
	principal = strings.TrimSpace(principal)
	role = strings.TrimSpace(role)
	if principal == "" || role == "" {
		return false
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, exists := catalog.roles[role]
	return exists && entry.metadata.Owner == principal
}

// CanManageNamespace reports whether principal owns the namespace or one of
// its registered ancestors.
func (catalog *RoleCatalog) CanManageNamespace(principal, namespace string) bool {
	if catalog == nil {
		return false
	}
	if !validRoleCatalogText(principal, catalog.limits.maxPrincipalBytes) || !validRoleCatalogText(namespace, catalog.limits.maxNameBytes) {
		return false
	}
	principal = strings.TrimSpace(principal)
	namespace = strings.TrimSpace(namespace)
	if principal == "" || namespace == "" {
		return false
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	owner, exists := namespaceCatalogOwnerLocked(catalog.namespaces, namespace)
	return exists && owner == principal
}

// RevokeRole removes a membership using an exact catalog version check.
func (catalog *RoleCatalog) RevokeRole(actor, principal, role string, expectedVersion uint64) error {
	if catalog == nil {
		return ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return err
	}
	principal, err = catalog.normalizePrincipal(principal)
	if err != nil {
		return err
	}
	role, err = normalizeRoleCatalogName(role, catalog.limits.maxNameBytes)
	if err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if err := catalog.checkVersionLocked(expectedVersion); err != nil {
		return err
	}
	roleEntry, exists := catalog.roles[role]
	if !exists {
		return ErrRoleCatalogNotFound
	}
	if roleEntry.metadata.Owner != actor {
		return ErrRoleCatalogAccessDenied
	}
	assigned := catalog.memberships[principal]
	if _, exists := assigned[role]; !exists {
		return ErrRoleCatalogNotFound
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return err
	}
	delete(assigned, role)
	catalog.membershipCount--
	if len(assigned) == 0 {
		delete(catalog.memberships, principal)
	}
	return nil
}

// RevokeGrant removes a grant using an exact catalog version check.
func (catalog *RoleCatalog) RevokeGrant(actor string, grantID, expectedVersion uint64) error {
	if catalog == nil {
		return ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if err := catalog.checkVersionLocked(expectedVersion); err != nil {
		return err
	}
	grant, exists := catalog.grants[grantID]
	if !exists {
		return ErrRoleCatalogNotFound
	}
	roleEntry, exists := catalog.roles[grant.Role]
	if !exists {
		return ErrRoleCatalogNotFound
	}
	if roleEntry.metadata.Owner != actor {
		return ErrRoleCatalogAccessDenied
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return err
	}
	delete(catalog.grants, grantID)
	ids := catalog.grantsByRole[grant.Role]
	for index, id := range ids {
		if id == grantID {
			ids = append(ids[:index], ids[index+1:]...)
			break
		}
	}
	if len(ids) == 0 {
		delete(catalog.grantsByRole, grant.Role)
	} else {
		catalog.grantsByRole[grant.Role] = ids
	}
	return nil
}

// DeleteRole removes an unused role using an exact catalog version check.
func (catalog *RoleCatalog) DeleteRole(actor, role string, expectedVersion uint64) error {
	if catalog == nil {
		return ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return err
	}
	role, err = normalizeRoleCatalogName(role, catalog.limits.maxNameBytes)
	if err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if err := catalog.checkVersionLocked(expectedVersion); err != nil {
		return err
	}
	entry, exists := catalog.roles[role]
	if !exists {
		return ErrRoleCatalogNotFound
	}
	if entry.metadata.Owner != actor {
		return ErrRoleCatalogAccessDenied
	}
	if catalog.roleInUseLocked(role) {
		return ErrRoleCatalogInUse
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return err
	}
	delete(catalog.roles, role)
	return nil
}

// DeleteNamespace removes an unused namespace using an exact catalog version
// check. Child namespaces and scoped grants keep a namespace in use.
func (catalog *RoleCatalog) DeleteNamespace(actor, namespace string, expectedVersion uint64) error {
	if catalog == nil {
		return ErrRoleCatalogNil
	}
	actor, err := catalog.normalizePrincipal(actor)
	if err != nil {
		return err
	}
	namespace, err = normalizeNamespaceCatalogName(namespace, catalog.limits.maxNameBytes)
	if err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if err := catalog.checkVersionLocked(expectedVersion); err != nil {
		return err
	}
	entry, exists := catalog.namespaces[namespace]
	if !exists {
		return ErrRoleCatalogNotFound
	}
	if entry.metadata.Owner != actor {
		return ErrRoleCatalogAccessDenied
	}
	for _, child := range catalog.namespaces {
		if child.metadata.Parent == namespace {
			return ErrRoleCatalogInUse
		}
	}
	for _, grant := range catalog.grants {
		for _, selector := range grant.Rule.Namespaces {
			if namespaceSelectorBase(selector) == namespace {
				return ErrRoleCatalogInUse
			}
		}
	}
	if err := catalog.bumpVersionLocked(); err != nil {
		return err
	}
	delete(catalog.namespaces, namespace)
	return nil
}

// Snapshot returns a deterministic deep copy of the catalog.
func (catalog *RoleCatalog) Snapshot() RoleCatalogSnapshot {
	if catalog == nil {
		return RoleCatalogSnapshot{}
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	snapshot := RoleCatalogSnapshot{Version: catalog.version, NextGrantID: catalog.nextGrantID}
	roleNames := make([]string, 0, len(catalog.roles))
	for name := range catalog.roles {
		roleNames = append(roleNames, name)
	}
	sort.Strings(roleNames)
	snapshot.Roles = make([]RoleMetadata, 0, len(roleNames))
	for _, name := range roleNames {
		snapshot.Roles = append(snapshot.Roles, cloneRoleMetadata(catalog.roles[name].metadata))
	}
	namespaceNames := make([]string, 0, len(catalog.namespaces))
	for name := range catalog.namespaces {
		namespaceNames = append(namespaceNames, name)
	}
	sort.Strings(namespaceNames)
	snapshot.Namespaces = make([]NamespaceMetadata, 0, len(namespaceNames))
	for _, name := range namespaceNames {
		snapshot.Namespaces = append(snapshot.Namespaces, catalog.namespaces[name].metadata)
	}
	grantIDs := make([]uint64, 0, len(catalog.grants))
	for id := range catalog.grants {
		grantIDs = append(grantIDs, id)
	}
	sort.Slice(grantIDs, func(left, right int) bool { return grantIDs[left] < grantIDs[right] })
	snapshot.Grants = make([]RoleGrant, 0, len(grantIDs))
	for _, id := range grantIDs {
		snapshot.Grants = append(snapshot.Grants, cloneRoleGrant(catalog.grants[id]))
	}
	principals := make([]string, 0, len(catalog.memberships))
	for principal := range catalog.memberships {
		principals = append(principals, principal)
	}
	sort.Strings(principals)
	for _, principal := range principals {
		roles := make([]string, 0, len(catalog.memberships[principal]))
		for role := range catalog.memberships[principal] {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		for _, role := range roles {
			snapshot.Memberships = append(snapshot.Memberships, catalog.memberships[principal][role])
		}
	}
	return snapshot
}

// Restore replaces the catalog with a validated snapshot if expectedVersion
// still matches. A caller should persist the snapshot and its own durability
// marker atomically; this method only validates and installs memory state.
func (catalog *RoleCatalog) Restore(snapshot RoleCatalogSnapshot, expectedVersion uint64) error {
	if catalog == nil {
		return ErrRoleCatalogNil
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if err := catalog.checkVersionLocked(expectedVersion); err != nil {
		return err
	}
	state, err := validateRoleCatalogSnapshot(snapshot, catalog.limits)
	if err != nil {
		return err
	}
	catalog.version = snapshot.Version
	catalog.nextGrantID = snapshot.NextGrantID
	catalog.membershipCount = state.membershipCount
	catalog.roles = state.roles
	catalog.namespaces = state.namespaces
	catalog.grants = state.grants
	catalog.grantsByRole = state.grantsByRole
	catalog.memberships = state.memberships
	return nil
}

type roleCatalogSnapshotState struct {
	roles           map[string]roleCatalogRole
	namespaces      map[string]roleCatalogNamespace
	grants          map[uint64]RoleGrant
	grantsByRole    map[string][]uint64
	memberships     map[string]map[string]RoleMembership
	membershipCount int
}

func validateRoleCatalogSnapshot(snapshot RoleCatalogSnapshot, limits roleCatalogLimits) (roleCatalogSnapshotState, error) {
	state := roleCatalogSnapshotState{
		roles:        make(map[string]roleCatalogRole, len(snapshot.Roles)),
		namespaces:   make(map[string]roleCatalogNamespace, len(snapshot.Namespaces)),
		grants:       make(map[uint64]RoleGrant, len(snapshot.Grants)),
		grantsByRole: make(map[string][]uint64),
		memberships:  make(map[string]map[string]RoleMembership),
	}
	if len(snapshot.Roles) > limits.maxRoles || len(snapshot.Namespaces) > limits.maxNamespaces || len(snapshot.Grants) > limits.maxGrants || len(snapshot.Memberships) > limits.maxMemberships {
		return roleCatalogSnapshotState{}, ErrRoleCatalogLimit
	}
	if snapshot.Version == 0 && (len(snapshot.Roles) != 0 || len(snapshot.Namespaces) != 0 || len(snapshot.Grants) != 0 || len(snapshot.Memberships) != 0) {
		return roleCatalogSnapshotState{}, fmt.Errorf("%w: non-empty snapshot has zero version", ErrRoleCatalogInvalid)
	}
	for _, metadata := range snapshot.Roles {
		name, err := normalizeRoleCatalogName(metadata.Name, limits.maxNameBytes)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		owner, err := normalizeRoleCatalogPrincipal(metadata.Owner, limits)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		parents, err := normalizeRoleParents(metadata.Parents, limits)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		if _, exists := state.roles[name]; exists {
			return roleCatalogSnapshotState{}, ErrRoleCatalogAlreadyExists
		}
		state.roles[name] = roleCatalogRole{metadata: RoleMetadata{Name: name, Owner: owner, Parents: parents}}
	}
	for name, role := range state.roles {
		for _, parent := range role.metadata.Parents {
			parentRole, exists := state.roles[parent]
			if !exists {
				return roleCatalogSnapshotState{}, fmt.Errorf("%w: parent role %q", ErrRoleCatalogNotFound, parent)
			}
			if parentRole.metadata.Owner != role.metadata.Owner {
				return roleCatalogSnapshotState{}, ErrRoleCatalogAccessDenied
			}
		}
		if err := validateRoleCatalogRoleAcyclic(state.roles, name, nil); err != nil {
			return roleCatalogSnapshotState{}, err
		}
	}
	for _, metadata := range snapshot.Namespaces {
		name, err := normalizeNamespaceCatalogName(metadata.Name, limits.maxNameBytes)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		owner, err := normalizeRoleCatalogPrincipal(metadata.Owner, limits)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		parent := metadata.Parent
		if parent != "" {
			var parentErr error
			parent, parentErr = normalizeNamespaceCatalogName(parent, limits.maxNameBytes)
			if parentErr != nil {
				return roleCatalogSnapshotState{}, parentErr
			}
		} else if strings.Contains(name, "/") {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: nested namespace requires a parent", ErrRoleCatalogInvalid)
		}
		if _, exists := state.namespaces[name]; exists {
			return roleCatalogSnapshotState{}, ErrRoleCatalogAlreadyExists
		}
		state.namespaces[name] = roleCatalogNamespace{metadata: NamespaceMetadata{Name: name, Parent: parent, Owner: owner}}
	}
	for name, namespace := range state.namespaces {
		if namespace.metadata.Parent == "" {
			continue
		}
		parent, exists := state.namespaces[namespace.metadata.Parent]
		if !exists {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: parent namespace %q", ErrRoleCatalogNotFound, namespace.metadata.Parent)
		}
		if parent.metadata.Owner != namespace.metadata.Owner || !namespaceCatalogDescendant(name, namespace.metadata.Parent) {
			return roleCatalogSnapshotState{}, ErrRoleCatalogInvalid
		}
	}
	for _, grant := range snapshot.Grants {
		if grant.ID == 0 {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: grant id must be non-zero", ErrRoleCatalogInvalid)
		}
		if grant.ID > snapshot.NextGrantID {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: grant id exceeds next id", ErrRoleCatalogInvalid)
		}
		if _, exists := state.grants[grant.ID]; exists {
			return roleCatalogSnapshotState{}, ErrRoleCatalogAlreadyExists
		}
		role, exists := state.roles[grant.Role]
		if !exists {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: grant role %q", ErrRoleCatalogNotFound, grant.Role)
		}
		grantedBy, err := normalizeRoleCatalogPrincipal(grant.GrantedBy, limits)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		if grantedBy != role.metadata.Owner {
			return roleCatalogSnapshotState{}, ErrRoleCatalogAccessDenied
		}
		normalizedRule, err := normalizeRoleCatalogRule(grant.Rule, limits, state.namespaces)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		if err := validateRoleCatalogGrantNamespaceOwnership(grantedBy, normalizedRule, state.namespaces); err != nil {
			return roleCatalogSnapshotState{}, err
		}
		if grant.Version > snapshot.Version {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: grant version exceeds snapshot", ErrRoleCatalogInvalid)
		}
		stored := RoleGrant{ID: grant.ID, Role: grant.Role, Rule: normalizedRule, GrantedBy: grantedBy, Version: grant.Version}
		state.grants[stored.ID] = stored
		state.grantsByRole[stored.Role] = append(state.grantsByRole[stored.Role], stored.ID)
	}
	for _, membership := range snapshot.Memberships {
		principal, err := normalizeRoleCatalogPrincipal(membership.Principal, limits)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		role, exists := state.roles[membership.Role]
		if !exists {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: membership role %q", ErrRoleCatalogNotFound, membership.Role)
		}
		grantedBy, err := normalizeRoleCatalogPrincipal(membership.GrantedBy, limits)
		if err != nil {
			return roleCatalogSnapshotState{}, err
		}
		if grantedBy != role.metadata.Owner {
			return roleCatalogSnapshotState{}, ErrRoleCatalogAccessDenied
		}
		assigned := state.memberships[principal]
		if assigned == nil {
			assigned = make(map[string]RoleMembership)
			state.memberships[principal] = assigned
		}
		if _, exists := assigned[membership.Role]; exists {
			return roleCatalogSnapshotState{}, ErrRoleCatalogAlreadyExists
		}
		if membership.Version > snapshot.Version {
			return roleCatalogSnapshotState{}, fmt.Errorf("%w: membership version exceeds snapshot", ErrRoleCatalogInvalid)
		}
		assigned[membership.Role] = RoleMembership{Principal: principal, Role: membership.Role, GrantedBy: grantedBy, Version: membership.Version}
		state.membershipCount++
	}
	for role, ids := range state.grantsByRole {
		sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
		state.grantsByRole[role] = ids
	}
	return state, nil
}

func validateRoleCatalogRoleAcyclic(roles map[string]roleCatalogRole, name string, visiting map[string]bool) error {
	if visiting == nil {
		visiting = make(map[string]bool)
	}
	if visiting[name] {
		return ErrRoleCatalogCycle
	}
	visiting[name] = true
	for _, parent := range roles[name].metadata.Parents {
		if err := validateRoleCatalogRoleAcyclic(roles, parent, visiting); err != nil {
			return err
		}
	}
	delete(visiting, name)
	return nil
}

func (catalog *RoleCatalog) roleInUseLocked(role string) bool {
	for _, entry := range catalog.roles {
		for _, parent := range entry.metadata.Parents {
			if parent == role {
				return true
			}
		}
	}
	for _, assigned := range catalog.memberships {
		if _, exists := assigned[role]; exists {
			return true
		}
	}
	return len(catalog.grantsByRole[role]) != 0
}

func (catalog *RoleCatalog) validateGrantNamespaceOwnershipLocked(actor string, rule Rule) error {
	return validateRoleCatalogGrantNamespaceOwnership(actor, rule, catalog.namespaces)
}

func validateRoleCatalogGrantNamespaceOwnership(actor string, rule Rule, namespaces map[string]roleCatalogNamespace) error {
	for _, selector := range rule.Namespaces {
		if selector == "*" {
			continue
		}
		owner, exists := namespaceCatalogOwnerLocked(namespaces, namespaceSelectorBase(selector))
		if !exists {
			return ErrRoleCatalogNotFound
		}
		if owner != actor {
			return ErrRoleCatalogAccessDenied
		}
	}
	return nil
}

func (catalog *RoleCatalog) normalizePrincipal(value string) (string, error) {
	return normalizeRoleCatalogPrincipal(value, catalog.limits)
}

func normalizeRoleCatalogPrincipal(value string, limits roleCatalogLimits) (string, error) {
	if !validRoleCatalogText(value, limits.maxPrincipalBytes) {
		return "", fmt.Errorf("%w: principal", ErrRoleCatalogInvalid)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%w: principal", ErrRoleCatalogInvalid)
	}
	return value, nil
}

func normalizeRoleParents(values []string, limits roleCatalogLimits) ([]string, error) {
	return normalizeRoleCatalogSelectors(values, limits.maxRoleParents, limits.maxNameBytes, false)
}

func normalizeRoleCatalogName(value string, maxBytes int) (string, error) {
	if !validRoleCatalogText(value, maxBytes) {
		return "", fmt.Errorf("%w: role name", ErrRoleCatalogInvalid)
	}
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "/") || strings.Contains(value, "*") {
		return "", fmt.Errorf("%w: role name", ErrRoleCatalogInvalid)
	}
	return value, nil
}

func normalizeNamespaceCatalogName(value string, maxBytes int) (string, error) {
	if !validRoleCatalogText(value, maxBytes) {
		return "", fmt.Errorf("%w: namespace name", ErrRoleCatalogInvalid)
	}
	value = strings.TrimSpace(value)
	if value == "" || value == "." || value == ".." || strings.Contains(value, "*") || strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") {
		return "", fmt.Errorf("%w: namespace name", ErrRoleCatalogInvalid)
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "." || segment == ".." || segment == "" {
			return "", fmt.Errorf("%w: namespace path", ErrRoleCatalogInvalid)
		}
	}
	return value, nil
}

func normalizeRoleCatalogSelectors(values []string, maxCount, maxBytes int, uppercase bool) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > maxCount {
		return nil, ErrRoleCatalogLimit
	}
	seen := make(map[string]struct{}, len(values))
	selectors := make([]string, 0, len(values))
	for _, value := range values {
		if !validRoleCatalogText(value, maxBytes) {
			return nil, fmt.Errorf("%w: selector", ErrRoleCatalogInvalid)
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%w: selector", ErrRoleCatalogInvalid)
		}
		if strings.Contains(value, "*") && value != "*" && !strings.HasSuffix(value, "*") {
			return nil, fmt.Errorf("%w: wildcard selector", ErrRoleCatalogInvalid)
		}
		if uppercase {
			value = strings.ToUpper(value)
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		selectors = append(selectors, value)
	}
	sort.Strings(selectors)
	return selectors, nil
}

func normalizeRoleCatalogRule(rule Rule, limits roleCatalogLimits, namespaces map[string]roleCatalogNamespace) (Rule, error) {
	commands, err := normalizeRoleCatalogSelectors(rule.Commands, limits.maxSelectors, limits.maxNameBytes, true)
	if err != nil {
		return Rule{}, err
	}
	namespaceSelectors, err := normalizeRoleCatalogSelectors(rule.Namespaces, limits.maxSelectors, limits.maxNameBytes, false)
	if err != nil {
		return Rule{}, err
	}
	for _, selector := range namespaceSelectors {
		if selector == "*" {
			continue
		}
		if _, exists := namespaceCatalogOwnerLocked(namespaces, namespaceSelectorBase(selector)); !exists {
			return Rule{}, fmt.Errorf("%w: namespace selector %q", ErrRoleCatalogNotFound, selector)
		}
	}
	sources, err := normalizeRoleCatalogSelectors(rule.Sources, limits.maxSelectors, limits.maxNameBytes, false)
	if err != nil {
		return Rule{}, err
	}
	objects, err := normalizeRoleCatalogSelectors(rule.Objects, limits.maxSelectors, limits.maxNameBytes, false)
	if err != nil {
		return Rule{}, err
	}
	return Rule{Commands: commands, Namespaces: namespaceSelectors, Sources: sources, Objects: objects}, nil
}

func namespaceSelectorBase(selector string) string {
	selector = strings.TrimSpace(selector)
	if strings.HasSuffix(selector, "*") {
		selector = strings.TrimSuffix(selector, "*")
		selector = strings.TrimRight(selector, ":/")
	}
	return selector
}

func namespaceCatalogDescendant(name, parent string) bool {
	return name != parent && strings.HasPrefix(name, parent+"/")
}

func namespaceCatalogOwnerLocked(namespaces map[string]roleCatalogNamespace, namespace string) (string, bool) {
	for current := strings.TrimSpace(namespace); current != ""; {
		if entry, exists := namespaces[current]; exists {
			return entry.metadata.Owner, true
		}
		separator := strings.LastIndexByte(current, '/')
		if separator < 0 {
			break
		}
		current = current[:separator]
	}
	return "", false
}

func roleCatalogRuleMatches(rule Rule, request AuthorizationRequest) bool {
	return roleCatalogCommandMatches(rule.Commands, request.Command) &&
		roleCatalogSelectorMatches(rule.Namespaces, request.Namespace, true) &&
		roleCatalogSelectorMatches(rule.Sources, request.Source, false) &&
		roleCatalogSelectorMatches(rule.Objects, request.Object, false)
}

func roleCatalogCommandMatches(selectors []string, value string) bool {
	if len(selectors) == 0 {
		return true
	}
	if strings.TrimSpace(value) == "" {
		return false
	}
	value = strings.ToUpper(strings.TrimSpace(value))
	for _, selector := range selectors {
		selector = strings.ToUpper(strings.TrimSpace(selector))
		if selector == "*" || selector == value {
			return true
		}
		if strings.HasSuffix(selector, "*") && strings.HasPrefix(value, strings.TrimSuffix(selector, "*")) {
			return true
		}
	}
	return false
}

func roleCatalogSelectorMatches(selectors []string, value string, namespace bool) bool {
	if len(selectors) == 0 {
		return true
	}
	if strings.TrimSpace(value) == "" {
		return false
	}
	value = strings.TrimSpace(value)
	for _, selector := range selectors {
		if selector == "*" {
			return true
		}
		if namespace {
			base := namespaceSelectorBase(selector)
			if value == base || strings.HasPrefix(value, base+"/") || strings.HasPrefix(value, base+":") {
				return true
			}
			continue
		}
		if selector == value {
			return true
		}
		if strings.HasSuffix(selector, "*") && strings.HasPrefix(value, strings.TrimSuffix(selector, "*")) {
			return true
		}
	}
	return false
}

func sameRoleCatalogRule(left, right Rule) bool {
	return sameStringSlice(left.Commands, right.Commands) && sameStringSlice(left.Namespaces, right.Namespaces) && sameStringSlice(left.Sources, right.Sources) && sameStringSlice(left.Objects, right.Objects)
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validRoleCatalogText(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes {
		return false
	}
	nonASCII := false
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character < 0x20 || character == 0x7f {
			return false
		}
		if character >= 0x80 {
			nonASCII = true
		}
	}
	return !nonASCII || utf8.ValidString(value)
}

func validRoleCatalogRequest(request AuthorizationRequest, maxBytes int) bool {
	return validOptionalRoleCatalogText(request.Command, maxBytes) &&
		validOptionalRoleCatalogText(request.Namespace, maxBytes) &&
		validOptionalRoleCatalogText(request.Source, maxBytes) &&
		validOptionalRoleCatalogText(request.Object, maxBytes)
}

func validOptionalRoleCatalogText(value string, maxBytes int) bool {
	return value == "" || validRoleCatalogText(value, maxBytes)
}

func (catalog *RoleCatalog) bumpVersionLocked() error {
	if catalog.version == ^uint64(0) {
		return fmt.Errorf("%w: version overflow", ErrRoleCatalogInvalid)
	}
	catalog.version++
	return nil
}

func (catalog *RoleCatalog) checkVersionLocked(expected uint64) error {
	if expected != catalog.version {
		return ErrRoleCatalogConflict
	}
	return nil
}

func cloneRoleMetadata(metadata RoleMetadata) RoleMetadata {
	metadata.Parents = append([]string(nil), metadata.Parents...)
	return metadata
}

func cloneRule(rule Rule) Rule {
	return Rule{
		Commands:   append([]string(nil), rule.Commands...),
		Namespaces: append([]string(nil), rule.Namespaces...),
		Sources:    append([]string(nil), rule.Sources...),
		Objects:    append([]string(nil), rule.Objects...),
	}
}

func cloneRoleGrant(grant RoleGrant) RoleGrant {
	grant.Rule = cloneRule(grant.Rule)
	return grant
}
