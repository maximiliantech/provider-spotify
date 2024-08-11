/*
Copyright 2022 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package playlist

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/crossplane/crossplane-runtime/pkg/connection"
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/pkg/errors"
	"github.com/zmb3/spotify"
	spotifyauth "github.com/zmb3/spotify/v2/auth"
	"golang.org/x/oauth2/clientcredentials"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/maximiliantech/provider-spotify/apis/playlist/v1alpha1"
	apisv1alpha1 "github.com/maximiliantech/provider-spotify/apis/v1alpha1"
	"github.com/maximiliantech/provider-spotify/internal/features"
)

const (
	errNotPlaylist  = "managed resource is not a Playlist custom resource"
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errGetCreds     = "cannot get credentials"

	errNewClient        = "cannot create new Service"
	errExtractSecretKey = "cannot extract from secret when Secret reference not specified"
	errUnmarshalCreds   = "ClientID and ClientSecret could not be unmarshalled from Secret"
)

var (
	spotifyService = func(creds []byte) (*spotify.Client, error) {
		var credentials Credentials
		err := json.Unmarshal(creds, &credentials)
		if err != nil {
			return nil, errors.Wrap(err, errUnmarshalCreds)
		}
		config := &clientcredentials.Config{
			ClientID:     credentials.ClientID,
			ClientSecret: credentials.ClientSecret,
			TokenURL:     spotify.TokenURL,
		}
		ctx := context.Background()
		token, err := config.Token(ctx)
		if err != nil {
			return nil, err
		}
		httpClient := spotifyauth.New().Client(ctx, token)
		client := spotify.NewClient(httpClient)

		return &client, nil
	}
)

type Credentials struct {
	ClientID     string `json:"clientID"`
	ClientSecret string `json:"clientSecret"`
}

// Setup adds a controller that reconciles Playlist managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.PlaylistGroupKind)

	cps := []managed.ConnectionPublisher{managed.NewAPISecretPublisher(mgr.GetClient(), mgr.GetScheme())}
	if o.Features.Enabled(features.EnableAlphaExternalSecretStores) {
		cps = append(cps, connection.NewDetailsManager(mgr.GetClient(), apisv1alpha1.StoreConfigGroupVersionKind))
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.PlaylistGroupVersionKind),
		managed.WithExternalConnecter(&connector{
			kube:         mgr.GetClient(),
			usage:        resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newServiceFn: spotifyService}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
		managed.WithConnectionPublishers(cps...))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.Playlist{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// A connector is expected to produce an ExternalClient when its Connect method
// is called.
type connector struct {
	kube         client.Client
	usage        resource.Tracker
	newServiceFn func(creds []byte) (*spotify.Client, error)
}

// Connect typically produces an ExternalClient by:
// 1. Tracking that the managed resource is using a ProviderConfig.
// 2. Getting the managed resource's ProviderConfig.
// 3. Getting the credentials specified by the ProviderConfig.
// 4. Using the credentials to form a client.
func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Playlist)
	if !ok {
		return nil, errors.New(errNotPlaylist)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &apisv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	cd := pc.Spec.Credentials

	creds, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
	if err != nil {
		return nil, errors.Wrap(err, errGetCreds)
	}

	svc, err := c.newServiceFn(creds)
	if err != nil {
		return nil, errors.Wrap(err, errNewClient)
	}

	return &external{service: svc}, nil
}

// An ExternalClient observes, then either creates, updates, or deletes an
// external resource to ensure it reflects the managed resource's desired state.
type external struct {
	// A 'client' used to connect to the Spotify API.
	service *spotify.Client
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Playlist)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotPlaylist)
	}

	if cr.Status.AtProvider.Id == "" {
		return managed.ExternalObservation{}, nil
	}

	playlist, err := c.service.GetPlaylist(spotify.ID(cr.Status.AtProvider.Id))
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if playlist == nil {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	return managed.ExternalObservation{
		// Return false when the external resource does not exist. This lets
		// the managed resource reconciler know that it needs to call Create to
		// (re)create the resource, or that it has successfully been deleted.
		ResourceExists: true,

		// Return false when the external resource exists, but it not up to date
		// with the desired managed resource state. This lets the managed
		// resource reconciler know that it needs to call Update.
		ResourceUpToDate: c.isUpToDate(playlist, cr),

		// Return any details that may be required to connect to the external
		// resource. These will be stored as the connection secret.
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Playlist)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotPlaylist)
	}
	var desc string
	if cr.Spec.ForProvider.Description == nil {
		desc = ""
	}
	_, err := c.service.CreatePlaylistForUser(cr.Spec.ForProvider.UserID, cr.Spec.ForProvider.Name, desc, *cr.Spec.ForProvider.Public)
	if err != nil {
		return managed.ExternalCreation{}, err
	}

	return managed.ExternalCreation{
		// Optionally return any details that may be required to connect to the
		// external resource. These will be stored as the connection secret.
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Playlist)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotPlaylist)
	}

	fmt.Printf("Updating: %+v", cr)

	return managed.ExternalUpdate{
		// Optionally return any details that may be required to connect to the
		// external resource. These will be stored as the connection secret.
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Playlist)
	if !ok {
		return errors.New(errNotPlaylist)
	}

	fmt.Printf("Deleting: %+v", cr)

	return nil
}

// isUpToDate checks whether the actual state matches the desired state.
func (c *external) isUpToDate(playlist *spotify.FullPlaylist, cr *v1alpha1.Playlist) bool {
	if playlist.Name != cr.Spec.ForProvider.Name {
		return false
	}
	if playlist.IsPublic != *cr.Spec.ForProvider.Public {
		return false
	}
	if playlist.Collaborative != *cr.Spec.ForProvider.Collaborative {
		return false
	}
	if playlist.Description != *cr.Spec.ForProvider.Description {
		return false
	}
	return true
}
