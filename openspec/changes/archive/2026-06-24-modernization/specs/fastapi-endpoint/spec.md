## ADDED Requirements

### Requirement: FastAPI REST Service Implementation
The system MUST provide a FastAPI-based REST service that exposes the SQL verification endpoint at `/verify-sql`.

#### Scenario: Successful SQL verification
- **WHEN** a client sends a POST request to `/verify-sql` with a valid JSON payload containing the query string `sql`, a dictionary `config`, and optionally a string `dialect`
- **THEN** the system returns a status code of 200 and a JSON response containing `allowed`, `errors`, `fixed`, and `risk` fields.

### Requirement: Request Schema Validation via Pydantic
The REST endpoint MUST validate incoming JSON payloads using strict Pydantic schemas.

#### Scenario: Invalid JSON schema payload
- **WHEN** a client sends a POST request with missing fields (such as missing `sql` or missing `config`) or invalid types
- **THEN** the system automatically returns a status code of 422 (Unprocessable Entity) with structured validation error messages.
