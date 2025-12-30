/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

// This is a test server that simulates the fulfillment service for testing the watch functionality.
// Run this server, then in another terminal run: ./fulfillment-cli get clusters --watch
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	eventsv1 "github.com/osac-project/fulfillment-common/api/events/v1"
	ffv1 "github.com/osac-project/fulfillment-common/api/fulfillment/v1"
	metadatav1 "github.com/osac-project/fulfillment-common/api/metadata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/osac-project/fulfillment-cli/internal/testing"
)

const (
	serverPort                         = "8080"
	defaultScenarioFile                = "internal/testing/testdata/cluster-lifecycle.yaml"
	defaultComputeInstanceScenarioFile = "internal/testing/testdata/compute-instances-and-templates.yaml"
)

// loggingEventsServer wraps EventsServerFuncs to add logging for the standalone server
type loggingEventsServer struct {
	*testing.EventsServerFuncs
}

func (s *loggingEventsServer) Watch(request *eventsv1.EventsWatchRequest, stream eventsv1.Events_WatchServer) error {
	filter := request.GetFilter()
	log.Printf("Client connected. Filter: %s", filter)

	// Wrap the stream to add logging
	loggingStream := &loggingWatchServer{Events_WatchServer: stream}

	// Call the underlying Watch function
	err := s.EventsServerFuncs.Watch(request, loggingStream)

	if err != nil {
		log.Printf("Client disconnected: %v", err)
	} else {
		log.Println("Client disconnected")
	}

	return err
}

// loggingWatchServer wraps the stream to log sent events
type loggingWatchServer struct {
	eventsv1.Events_WatchServer
}

func (s *loggingWatchServer) Send(response *eventsv1.EventsWatchResponse) error {
	event := response.GetEvent()
	log.Printf("Sending %s event for %s...", event.Type, testing.GetEventObjectID(event))
	return s.Events_WatchServer.Send(response)
}

// Dummy clusters server - just to satisfy the CLI's requirements
type clustersServer struct {
	ffv1.UnimplementedClustersServer
}

func (s *clustersServer) List(ctx context.Context, request *ffv1.ClustersListRequest) (*ffv1.ClustersListResponse, error) {
	// Return empty list
	return &ffv1.ClustersListResponse{}, nil
}

// Simple mock compute instances server for testing
type computeInstancesServer struct {
	ffv1.UnimplementedComputeInstancesServer
	scenario *testing.ComputeInstanceScenario
}

func (s *computeInstancesServer) Create(ctx context.Context, request *ffv1.ComputeInstancesCreateRequest) (*ffv1.ComputeInstancesCreateResponse, error) {
	instance := request.GetObject()

	// Set mock ID and state if not already set
	if instance.Id == "" {
		instance.Id = "ci-mock-12345"
	}
	if instance.Status == nil {
		instance.Status = &ffv1.ComputeInstanceStatus{
			State: ffv1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_STARTING,
		}
	}

	log.Printf("Created compute instance: %s (name: %s, template: %s)",
		instance.Id,
		instance.GetMetadata().GetName(),
		instance.GetSpec().GetTemplate())

	return &ffv1.ComputeInstancesCreateResponse{Object: instance}, nil
}

func (s *computeInstancesServer) Get(ctx context.Context, request *ffv1.ComputeInstancesGetRequest) (*ffv1.ComputeInstancesGetResponse, error) {
	// Find instance by ID in scenario
	for _, instanceData := range s.scenario.Instances {
		if instanceData.ID == request.Id {
			log.Printf("Retrieved compute instance: %s", request.Id)
			return &ffv1.ComputeInstancesGetResponse{Object: instanceData.ToProtoInstance()}, nil
		}
	}

	// Return NotFound error if instance not in scenario
	log.Printf("Compute instance not found: %s", request.Id)
	return nil, status.Errorf(codes.NotFound, "compute instance %q not found", request.Id)
}

func (s *computeInstancesServer) List(ctx context.Context, request *ffv1.ComputeInstancesListRequest) (*ffv1.ComputeInstancesListResponse, error) {
	// Convert all scenario instances to proto
	allInstances := make([]*ffv1.ComputeInstance, len(s.scenario.Instances))
	for i, instanceData := range s.scenario.Instances {
		allInstances[i] = instanceData.ToProtoInstance()
	}

	// Apply filter if provided (simple string matching for mock purposes)
	filter := request.GetFilter()
	var instances []*ffv1.ComputeInstance

	if filter != "" {
		// For mock purposes, handle common CEL filters
		// If filter is about deletion_timestamp or other metadata, return all non-deleted instances
		if strings.Contains(filter, "deletion_timestamp") {
			instances = allInstances // Mock instances don't have deletion_timestamp
		} else {
			// Simple filter: check if filter contains the instance ID or name
			// This is a mock implementation - real server would parse CEL expressions
			for _, inst := range allInstances {
				// Check if filter mentions this instance's ID or name
				if strings.Contains(filter, inst.Id) || strings.Contains(filter, inst.GetMetadata().GetName()) {
					instances = append(instances, inst)
				}
			}
		}
	} else {
		instances = allInstances
	}

	size := int32(len(instances))
	total := int32(len(instances))
	log.Printf("Listed compute instances (filter: %q, matches: %d)", filter, len(instances))
	return &ffv1.ComputeInstancesListResponse{
		Items: instances,
		Size:  &size,
		Total: &total,
	}, nil
}

// Simple mock compute instance templates server
type computeInstanceTemplatesServer struct {
	ffv1.UnimplementedComputeInstanceTemplatesServer
	scenario *testing.ComputeInstanceScenario
}

func (s *computeInstanceTemplatesServer) Get(ctx context.Context, request *ffv1.ComputeInstanceTemplatesGetRequest) (*ffv1.ComputeInstanceTemplatesGetResponse, error) {
	// Find template by ID
	for _, templateData := range s.scenario.Templates {
		if templateData.ID == request.Id {
			log.Printf("Retrieved compute instance template: %s", request.Id)
			return &ffv1.ComputeInstanceTemplatesGetResponse{Object: templateData.ToProtoTemplate()}, nil
		}
	}

	// Return NotFound error if template not in scenario
	log.Printf("Compute instance template not found: %s", request.Id)
	return nil, status.Errorf(codes.NotFound, "compute instance template %q not found", request.Id)
}

func (s *computeInstanceTemplatesServer) List(ctx context.Context, request *ffv1.ComputeInstanceTemplatesListRequest) (*ffv1.ComputeInstanceTemplatesListResponse, error) {
	// Convert all scenario templates to proto
	allTemplates := make([]*ffv1.ComputeInstanceTemplate, len(s.scenario.Templates))
	for i, templateData := range s.scenario.Templates {
		allTemplates[i] = templateData.ToProtoTemplate()
	}

	// Apply filter if provided (simple string matching for mock purposes)
	filter := request.GetFilter()
	var templates []*ffv1.ComputeInstanceTemplate

	if filter != "" {
		// For mock purposes, handle common CEL filters
		// If filter is about deletion_timestamp or other metadata, return all non-deleted templates
		if strings.Contains(filter, "deletion_timestamp") {
			templates = allTemplates // Mock templates don't have deletion_timestamp
		} else {
			// Simple filter: check if filter contains the template ID or name
			// This is a mock implementation - real server would parse CEL expressions
			for _, tmpl := range allTemplates {
				// Check if filter mentions this template's ID or name
				if strings.Contains(filter, tmpl.Id) || strings.Contains(filter, tmpl.GetMetadata().GetName()) {
					templates = append(templates, tmpl)
				}
			}
		}
	} else {
		templates = allTemplates
	}

	size := int32(len(templates))
	total := int32(len(templates))
	log.Printf("Listed compute instance templates (filter: %q, matches: %d)", filter, len(templates))
	return &ffv1.ComputeInstanceTemplatesListResponse{
		Items: templates,
		Size:  &size,
		Total: &total,
	}, nil
}

// Dummy metadata server - required for login
type metadataServer struct {
	metadatav1.UnimplementedMetadataServer
}

func (s *metadataServer) Get(ctx context.Context, request *metadatav1.MetadataGetRequest) (*metadatav1.MetadataGetResponse, error) {
	// Return minimal metadata - no authentication required for test server
	return &metadatav1.MetadataGetResponse{}, nil
}

func main() {
	// Parse command line flags
	scenarioFile := flag.String("scenario", defaultScenarioFile, "Path to event scenario YAML file")
	flag.Parse()

	// Load event scenario from file
	scenario, err := testing.LoadScenarioFromFile(*scenarioFile)
	if err != nil {
		log.Fatalf("Failed to load event scenario from %s: %v", *scenarioFile, err)
	}
	log.Printf("Loaded event scenario: %s - %s", scenario.Name, scenario.Description)

	// Load compute instance scenario from file
	ciScenario, err := testing.LoadComputeInstanceScenarioFromFile(defaultComputeInstanceScenarioFile)
	if err != nil {
		log.Fatalf("Failed to load compute instance scenario from %s: %v", defaultComputeInstanceScenarioFile, err)
	}
	log.Printf("Loaded compute instance scenario: %s - %s", ciScenario.Name, ciScenario.Description)

	listener, err := net.Listen("tcp", "127.0.0.1:"+serverPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Create events server using the builder with loaded scenario
	eventsServerFuncs := testing.NewMockEventsServerBuilder().
		WithScenario(scenario).
		Build()
	eventsv1.RegisterEventsServer(grpcServer, &loggingEventsServer{EventsServerFuncs: eventsServerFuncs})

	ffv1.RegisterClustersServer(grpcServer, &clustersServer{})
	ffv1.RegisterComputeInstancesServer(grpcServer, &computeInstancesServer{scenario: ciScenario})
	ffv1.RegisterComputeInstanceTemplatesServer(grpcServer, &computeInstanceTemplatesServer{scenario: ciScenario})
	metadatav1.RegisterMetadataServer(grpcServer, &metadataServer{})

	// Register health service
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	reflection.Register(grpcServer)

	fmt.Println("========================================")
	fmt.Println("Mock Fulfillment Service Started")
	fmt.Println("========================================")
	fmt.Printf("Listening on: %s\n", listener.Addr().String())
	fmt.Println("")
	fmt.Println("To test with the CLI, run in another terminal:")
	fmt.Println("")
	fmt.Println("1. Login:")
	fmt.Printf("  ./fulfillment-cli login --plaintext http://127.0.0.1:%s\n", serverPort)
	fmt.Println("")
	fmt.Println("2. Test commands:")
	fmt.Println("  ./fulfillment-cli create computeinstance --template tpl-small-001 --name test-instance")
	fmt.Println("  ./fulfillment-cli describe computeinstance ci-mock-12345")
	fmt.Println("  ./fulfillment-cli get clusters --watch")
	fmt.Println("")
	fmt.Println("Press Ctrl+C to stop the server")
	fmt.Println("========================================")
	fmt.Println("")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
