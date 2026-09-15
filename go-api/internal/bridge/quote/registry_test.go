package quote

import (
	"context"
	"testing"
)

type fakeProvider struct{ name string }

func (f *fakeProvider) Name() string { return f.name }
func (f *fakeProvider) GetQuote(ctx context.Context, req Request) (Quote, error) {
	return Quote{ProviderName: f.name}, nil
}

func TestRegistry_ProvidersFor_RegisteredRouteReturnsProvider(t *testing.T) {
	r := NewRegistry()
	key := RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	p := &fakeProvider{name: "across"}
	r.Register(key, p)

	got := r.ProvidersFor(key)
	if len(got) != 1 || got[0].Name() != "across" {
		t.Fatalf("expected exactly [across], got %v", got)
	}
}

func TestRegistry_ProvidersFor_UnregisteredRouteReturnsEmpty(t *testing.T) {
	r := NewRegistry()
	got := r.ProvidersFor(RouteKey{SourceChainID: 1, DestinationChainID: 2, Asset: "USDC"})
	if len(got) != 0 {
		t.Fatalf("expected no providers for an unregistered route, got %v", got)
	}
}

func TestRegistry_ProvidersFor_DifferentAssetSameChainsIsUnregistered(t *testing.T) {
	r := NewRegistry()
	key := RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	r.Register(key, &fakeProvider{name: "across"})

	got := r.ProvidersFor(RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "USDC"})
	if len(got) != 0 {
		t.Fatalf("expected registering WETH not to also register USDC for the same chain pair, got %v", got)
	}
}
