# Flow Tracer Development Makefile

.PHONY: test test-flow test-devstack-subset analyze clean

# Run basic flow tracer tests
test-flow:
	go test -v ./...

# Run devstack subset with flow tracing (Phase 1 target)
test-devstack-subset:
	@echo "Phase 1: Create devstack integration test"
	@echo "This requires creating: op-devstack/example/flow_test.go"
	@echo "That test will import this package and add tracer to devstack systems"
	@echo "See README.md for example integration code"

# Analyze trace results (Phase 3 target)
analyze:
	@echo "Phase 3: Implement analysis tools"
	@echo "This will run: go run ./cmd/analyzer trace-events.json"
	@echo "Currently not implemented - see FLOW_TRACER_PLAN.md"

# Clean up trace files
clean:
	rm -rf /tmp/flow-traces/
	rm -f trace-*.json flow-*.json

# Quick development feedback loop (target: <5 minutes)
dev-loop: clean test-devstack-subset analyze
	@echo "=== Flow Tracer Development Loop Complete ==="
	@echo "Check flow-completeness-report.json for results"

# Phase-specific targets

phase1: test-flow
	@echo "Phase 1: Basic event capture infrastructure"
	@echo "✅ Flow tracer tests pass"
	@echo "Next: Integrate with devstack"

phase2: phase1
	@echo "Phase 2: AST generation from captured events"
	@echo "Next: Implement BuildAST() method"

phase3: phase2 analyze
	@echo "Phase 3: Completeness validation and reporting"
	@echo "Next: Add migration planning"

phase4: phase3
	@echo "Phase 4: Pattern analysis and refactoring insights"
	@echo "Ready for production refactoring!"

# Development helpers
format:
	go fmt ./...

lint:
	golangci-lint run ./...

# Show current implementation status
status:
	@echo "=== Flow Tracer Implementation Status ==="
	@echo "Phase 1 - Foundation:"
	@echo "  ✅ Basic tracer infrastructure (tracer.go)"
	@echo "  ✅ Test framework (devstack_test.go)"
	@echo "  🚧 Devstack integration (pending)"
	@echo ""
	@echo "Phase 2 - AST Generation:"
	@echo "  ✅ AST data structures (ast.go)"
	@echo "  🚧 BuildAST implementation (pending)"
	@echo "  🚧 Pattern recognition (pending)"
	@echo ""
	@echo "Phase 3 - Validation:"
	@echo "  🚧 Completeness metrics (pending)"
	@echo "  🚧 Analysis reporting (pending)"
	@echo ""
	@echo "Phase 4 - Analysis:"
	@echo "  🚧 Migration recommendations (pending)"
	@echo ""
	@echo "Use 'make phase1' to start development"
