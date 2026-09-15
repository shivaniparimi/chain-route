package quote

// RouteKey identifies one (source chain, destination chain, asset)
// combination ChainRoute may support in testnet-execution mode.
type RouteKey struct {
	SourceChainID      int64
	DestinationChainID int64
	Asset              string
}

// Registry is a small, static, in-process map from a supported route to
// the provider(s) that can quote it -- built once at server startup
// (design doc §3). It is not a service, not a cache, not backed by a
// database table.
type Registry struct {
	providers map[RouteKey][]Provider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[RouteKey][]Provider)}
}

// Register adds p as a quoter for key. Multiple providers may be
// registered for the same key (not exercised in Phase 8, which registers
// exactly one).
func (r *Registry) Register(key RouteKey, p Provider) {
	r.providers[key] = append(r.providers[key], p)
}

// ProvidersFor returns every provider registered for key, or an empty
// (nil) slice if key is not supported -- callers must treat an empty
// result as "this route is not supported for live execution," never as
// an error to retry.
func (r *Registry) ProvidersFor(key RouteKey) []Provider {
	return r.providers[key]
}
