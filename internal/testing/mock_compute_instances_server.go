/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package testing

import (
	"context"
	"fmt"
	"sync"

	ffv1 "github.com/osac-project/fulfillment-common/api/fulfillment/v1"
	sharedv1 "github.com/osac-project/fulfillment-common/api/shared/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockComputeInstancesServer is a mock implementation of the ComputeInstancesServer
type MockComputeInstancesServer struct {
	ffv1.UnimplementedComputeInstancesServer
	scenario  *ComputeInstanceScenario
	instances map[string]*ffv1.ComputeInstance
	mu        sync.RWMutex
	nextID    int
}

// NewMockComputeInstancesServer creates a new mock compute instances server
func NewMockComputeInstancesServer(scenario *ComputeInstanceScenario) *MockComputeInstancesServer {
	server := &MockComputeInstancesServer{
		scenario:  scenario,
		instances: make(map[string]*ffv1.ComputeInstance),
		nextID:    1000,
	}

	// Pre-populate with scenario instances
	for _, instanceData := range scenario.Instances {
		server.instances[instanceData.ID] = instanceData.ToProtoInstance()
	}

	return server
}

// Create creates a new compute instance
func (s *MockComputeInstancesServer) Create(ctx context.Context, request *ffv1.ComputeInstancesCreateRequest) (*ffv1.ComputeInstancesCreateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	instance := request.GetObject()
	if instance == nil {
		return nil, status.Error(codes.InvalidArgument, "object is required")
	}

	// Generate ID if not provided
	if instance.Id == "" {
		s.nextID++
		instance.Id = fmt.Sprintf("ci-test-%d", s.nextID)
	}

	// Set state to PROGRESSING if not set
	if instance.Status == nil {
		instance.Status = &ffv1.ComputeInstanceStatus{}
	}
	if instance.Status.State == ffv1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_UNSPECIFIED {
		instance.Status.State = ffv1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_STARTING
	}

	// Store the instance
	s.instances[instance.Id] = instance

	return &ffv1.ComputeInstancesCreateResponse{Object: instance}, nil
}

// Get retrieves a compute instance by ID
func (s *MockComputeInstancesServer) Get(ctx context.Context, request *ffv1.ComputeInstancesGetRequest) (*ffv1.ComputeInstancesGetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instance, exists := s.instances[request.Id]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "compute instance %q not found", request.Id)
	}

	return &ffv1.ComputeInstancesGetResponse{Object: instance}, nil
}

// List lists all compute instances
func (s *MockComputeInstancesServer) List(ctx context.Context, request *ffv1.ComputeInstancesListRequest) (*ffv1.ComputeInstancesListResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances := make([]*ffv1.ComputeInstance, 0, len(s.instances))
	for _, instance := range s.instances {
		instances = append(instances, instance)
	}

	size := int32(len(instances))
	total := int32(len(instances))

	return &ffv1.ComputeInstancesListResponse{
		Items: instances,
		Size:  &size,
		Total: &total,
	}, nil
}

// Delete deletes a compute instance by ID
func (s *MockComputeInstancesServer) Delete(ctx context.Context, request *ffv1.ComputeInstancesDeleteRequest) (*ffv1.ComputeInstancesDeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.instances[request.Id]; !exists {
		return nil, status.Errorf(codes.NotFound, "compute instance %q not found", request.Id)
	}

	delete(s.instances, request.Id)

	return &ffv1.ComputeInstancesDeleteResponse{}, nil
}

// MockComputeInstanceTemplatesServer is a mock implementation of the ComputeInstanceTemplatesServer
type MockComputeInstanceTemplatesServer struct {
	ffv1.UnimplementedComputeInstanceTemplatesServer
	scenario *ComputeInstanceScenario
}

// NewMockComputeInstanceTemplatesServer creates a new mock compute instance templates server
func NewMockComputeInstanceTemplatesServer(scenario *ComputeInstanceScenario) *MockComputeInstanceTemplatesServer {
	return &MockComputeInstanceTemplatesServer{
		scenario: scenario,
	}
}

// Get retrieves a compute instance template by ID
func (s *MockComputeInstanceTemplatesServer) Get(ctx context.Context, request *ffv1.ComputeInstanceTemplatesGetRequest) (*ffv1.ComputeInstanceTemplatesGetResponse, error) {
	for _, templateData := range s.scenario.Templates {
		if templateData.ID == request.Id {
			return &ffv1.ComputeInstanceTemplatesGetResponse{Object: templateData.ToProtoTemplate()}, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "compute instance template %q not found", request.Id)
}

// List lists all compute instance templates
func (s *MockComputeInstanceTemplatesServer) List(ctx context.Context, request *ffv1.ComputeInstanceTemplatesListRequest) (*ffv1.ComputeInstanceTemplatesListResponse, error) {
	templates := make([]*ffv1.ComputeInstanceTemplate, len(s.scenario.Templates))
	for i, templateData := range s.scenario.Templates {
		templates[i] = templateData.ToProtoTemplate()
	}

	size := int32(len(templates))
	total := int32(len(templates))

	return &ffv1.ComputeInstanceTemplatesListResponse{
		Items: templates,
		Size:  &size,
		Total: &total,
	}, nil
}

// NewMetadata creates a new metadata object with the given name
func NewMetadata(name string) *sharedv1.Metadata {
	return &sharedv1.Metadata{
		Name: name,
	}
}
