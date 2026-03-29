package taxonomy

// standardEntries is the single source of truth for all 48 well-known capabilities.
// Do not hardcode capability data elsewhere — use the catalog interface.
var standardEntries = []TaxonomyEntry{
	// nlp (8)
	{Name: "nlp.summarization", Category: "nlp", Action: "summarization", Description: "Summarize text content", InputTypes: []string{"text/plain", "application/json"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "nlp.translation", Category: "nlp", Action: "translation", Description: "Translate between languages", InputTypes: []string{"text/plain"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "nlp.sentiment", Category: "nlp", Action: "sentiment", Description: "Analyze text sentiment", InputTypes: []string{"text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "nlp.extraction", Category: "nlp", Action: "extraction", Description: "Extract entities from text", InputTypes: []string{"text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "nlp.classification", Category: "nlp", Action: "classification", Description: "Classify text into categories", InputTypes: []string{"text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "nlp.generation", Category: "nlp", Action: "generation", Description: "Generate text from prompts", InputTypes: []string{"text/plain", "application/json"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "nlp.embedding", Category: "nlp", Action: "embedding", Description: "Generate vector embeddings", InputTypes: []string{"text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "nlp.qa", Category: "nlp", Action: "qa", Description: "Answer questions from context", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},

	// code (7)
	{Name: "code.generation", Category: "code", Action: "generation", Description: "Generate source code", InputTypes: []string{"text/plain", "application/json"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "code.review", Category: "code", Action: "review", Description: "Review code for issues", InputTypes: []string{"text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "code.refactoring", Category: "code", Action: "refactoring", Description: "Refactor existing code", InputTypes: []string{"text/plain"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "code.testing", Category: "code", Action: "testing", Description: "Generate test cases", InputTypes: []string{"text/plain"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "code.documentation", Category: "code", Action: "documentation", Description: "Generate documentation", InputTypes: []string{"text/plain"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "code.completion", Category: "code", Action: "completion", Description: "Complete partial code", InputTypes: []string{"text/plain"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "code.analysis", Category: "code", Action: "analysis", Description: "Static analysis and metrics", InputTypes: []string{"text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},

	// vision (6)
	{Name: "vision.ocr", Category: "vision", Action: "ocr", Description: "Extract text from images", InputTypes: []string{"image/png", "image/jpeg"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},
	{Name: "vision.classification", Category: "vision", Action: "classification", Description: "Classify images", InputTypes: []string{"image/png", "image/jpeg"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "vision.detection", Category: "vision", Action: "detection", Description: "Detect objects in images", InputTypes: []string{"image/png", "image/jpeg"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "vision.segmentation", Category: "vision", Action: "segmentation", Description: "Segment image regions", InputTypes: []string{"image/png", "image/jpeg"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "vision.generation", Category: "vision", Action: "generation", Description: "Generate images from prompts", InputTypes: []string{"text/plain"}, OutputTypes: []string{"image/png"}, CatalogVersion: "v1.0"},
	{Name: "vision.captioning", Category: "vision", Action: "captioning", Description: "Generate image captions", InputTypes: []string{"image/png", "image/jpeg"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},

	// data (6)
	{Name: "data.extraction", Category: "data", Action: "extraction", Description: "Extract structured data from sources", InputTypes: []string{"application/json", "text/plain"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "data.transformation", Category: "data", Action: "transformation", Description: "Transform data between formats", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "data.validation", Category: "data", Action: "validation", Description: "Validate data against rules", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "data.enrichment", Category: "data", Action: "enrichment", Description: "Enrich data with external sources", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "data.aggregation", Category: "data", Action: "aggregation", Description: "Aggregate and summarize datasets", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "data.search", Category: "data", Action: "search", Description: "Search across data sources", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},

	// decision (6)
	{Name: "decision.approval", Category: "decision", Action: "approval", Description: "Request human or automated approval", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "decision.routing", Category: "decision", Action: "routing", Description: "Route requests to appropriate handler", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "decision.classification", Category: "decision", Action: "classification", Description: "Classify inputs for decision paths", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "decision.scoring", Category: "decision", Action: "scoring", Description: "Score items against criteria", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "decision.recommendation", Category: "decision", Action: "recommendation", Description: "Generate recommendations", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "decision.escalation", Category: "decision", Action: "escalation", Description: "Escalate to higher authority", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},

	// communication (5)
	{Name: "communication.notification", Category: "communication", Action: "notification", Description: "Send notifications", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "communication.email", Category: "communication", Action: "email", Description: "Send and manage email", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "communication.messaging", Category: "communication", Action: "messaging", Description: "Send messages via channels", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "communication.scheduling", Category: "communication", Action: "scheduling", Description: "Schedule meetings and events", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "communication.translation", Category: "communication", Action: "translation", Description: "Translate communications", InputTypes: []string{"text/plain"}, OutputTypes: []string{"text/plain"}, CatalogVersion: "v1.0"},

	// orchestration (5)
	{Name: "orchestration.workflow", Category: "orchestration", Action: "workflow", Description: "Manage sub-workflows", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "orchestration.delegation", Category: "orchestration", Action: "delegation", Description: "Delegate tasks to other agents", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "orchestration.monitoring", Category: "orchestration", Action: "monitoring", Description: "Monitor agent and system status", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "orchestration.retry", Category: "orchestration", Action: "retry", Description: "Retry failed operations", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "orchestration.aggregation", Category: "orchestration", Action: "aggregation", Description: "Aggregate results from multiple agents", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},

	// security (5)
	{Name: "security.scanning", Category: "security", Action: "scanning", Description: "Scan for vulnerabilities", InputTypes: []string{"text/plain", "application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "security.validation", Category: "security", Action: "validation", Description: "Validate credentials and tokens", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "security.audit", Category: "security", Action: "audit", Description: "Audit trail and compliance", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
	{Name: "security.encryption", Category: "security", Action: "encryption", Description: "Encrypt and decrypt data", InputTypes: []string{"application/octet-stream"}, OutputTypes: []string{"application/octet-stream"}, CatalogVersion: "v1.0"},
	{Name: "security.authentication", Category: "security", Action: "authentication", Description: "Authenticate users and agents", InputTypes: []string{"application/json"}, OutputTypes: []string{"application/json"}, CatalogVersion: "v1.0"},
}

// standardCategories lists the 8 top-level categories.
var standardCategories = []TaxonomyCategory{
	{Name: "nlp", Description: "Natural language processing and understanding"},
	{Name: "code", Description: "Source code analysis, generation, and transformation"},
	{Name: "vision", Description: "Image and visual content processing"},
	{Name: "data", Description: "Data extraction, transformation, and management"},
	{Name: "decision", Description: "Decision support, approval, and routing"},
	{Name: "communication", Description: "Messaging, notification, and scheduling"},
	{Name: "orchestration", Description: "Workflow coordination and monitoring"},
	{Name: "security", Description: "Security scanning, validation, and audit"},
}
