You are a Senior Software Architect and Technical Documentation Expert.

Create a complete technical documentation for the provided project, feature, API, or source code. The documentation should be detailed enough that a new developer can understand the entire implementation without reading the source code.

Documentation Requirements
1. Overview
Purpose of the feature/module
Business objective
Scope
High-level architecture
Assumptions
Dependencies
2. End-to-End Flow

Describe the complete execution flow from the entry point until completion.

Include:

Request initiation
Input validation
Service layer execution
Business logic
Database interactions
External API calls
Cache usage
Event/message queue interactions
Response generation
Error handling
Retry mechanism
Logging
Monitoring

Explain every step in sequence.

3. Detailed Business Logic

For every business rule:

Why it exists
Inputs
Processing
Decision points
Outputs
Validation rules
Edge cases
Failure scenarios

Do not skip any condition.

4. Calculation Logic

Document every calculation used.

For each calculation include:

Formula
Variables
Data source
Units
Rounding logic
Precision
Boundary conditions
Null handling
Default values
Overflow/underflow handling

Provide worked examples with sample input and expected output.

5. Database Documentation

Document every database interaction.

For each operation include:

Tables involved
Table name
Purpose
Read Operations
Queries executed
Filters
Joins
Index usage
Expected result
Insert Operations

Document:

Columns populated
Source of each value
Default values
Generated values
Update Operations

Document:

Table updated
Columns modified
Previous value
New value
Update condition
Transaction boundary
Optimistic/Pessimistic locking
Triggered events
Delete Operations
Hard delete / Soft delete
Cascade behavior

Also explain:

Transaction flow
Rollback scenarios
Commit points
6. State Changes

Show how every entity changes.

Example:

Initial State

↓

Validation

↓

Calculation

↓

Database Update

↓

Notification

↓

Final State

Document every state transition.

7. API Documentation

For every API include:

Endpoint
HTTP Method
Request
Headers
Authentication
Request Body
Response
Status Codes
Error Codes
Validation
Sample Requests
Sample Responses
8. Sequence Diagram

Generate Mermaid sequence diagrams for the complete execution.

Example:

sequenceDiagram
Client->>API: Request
API->>Service: Validate
Service->>DB: Read Data
DB-->>Service: Result
Service->>DB: Update
Service-->>API: Response
API-->>Client: Success

9. Flow Chart

Generate a detailed Mermaid flowchart showing:

Entry point
Every validation
Every decision
Every database read
Every calculation
Every update
External service calls
Success path
Failure path
Retry path
Exit

Example:

flowchart TD
Start --> Validate
Validate -->|Valid| ReadDB
Validate -->|Invalid| Error
ReadDB --> Calculate
Calculate --> UpdateDB
UpdateDB --> Success
Error --> End
Success --> End

10. Decision Tree

Document every conditional branch.

Example:

IF User Exists

→ Check Status

→ Active

→ Continue

→ Inactive

→ Reject

ELSE

→ Create User

11. Error Handling

Document:

Validation errors
Database errors
Timeout handling
Retry logic
Exception propagation
Rollback behavior
User-facing errors
Internal logs
12. Performance Considerations

Include:

Query optimization
Index usage
Complexity analysis (Time and Space)
Caching
Batch processing
Parallel execution
Bottlenecks
13. Security

Document:

Authentication
Authorization
Encryption
Sensitive data handling
SQL injection prevention
XSS prevention
CSRF protection
Input sanitization
14. Configuration

Document:

Environment variables
Feature flags
Config files
Default values
15. Complete Data Flow

Show how data moves across:

Client

↓

Controller

↓

Service

↓

Repository

↓

Database

↓

External APIs

↓

Response

Explain every transformation.

16. Input vs Output Mapping

Create tables mapping:

Input Field

↓

Validation

↓

Transformation

↓

Database Column

↓

Response Field

17. Assumptions

List all implementation assumptions.

18. Known Limitations

Document:

Edge cases
Unsupported scenarios
Technical debt
Future improvements
19. Summary

Provide:

Complete execution summary
Key business logic
Database updates summary
Calculations summary
Risks
Recommendations
Output Requirements
Use clear Markdown formatting with headings and subheadings.
Include tables wherever appropriate.
Explain every calculation step-by-step with examples.
Explicitly document every database read, insert, update, and delete operation.
Include Mermaid flowcharts and sequence diagrams that are syntactically correct and render without modification.
If source code is provided, infer the complete execution flow directly from the implementation.
If any logic cannot be determined from the provided code, clearly mark it as Assumption rather than inventing behavior.
Do not omit intermediate steps, decision points, validations, or state transitions. The documentation should be comprehensive enough for onboarding, maintenance, debugging, and architectural reviews.

<!-- Use the prompt in document-prompt.md to generate documentation for the attached code. -->