# Multi-Tenancy Setup Guide

## Overview
Wahaku now supports multi-tenancy. Each tenant (organization/company) has isolated data, users, devices, knowledge files, and AI context.

## Architecture

### Database Schema
- **tenants**: List of organizations
- **users**: Each user belongs to a tenant (tenant_id)
- **user_devices**: Devices scoped to tenant
- **followup_tasks**: Follow-up tasks per tenant
- **tenant_knowledge_files**: Knowledge files per tenant

### Tenant Identification
Tenant is identified via HTTP header: `X-Tenant-ID`
- Default tenant ID: `1` (if header not provided)
- Tenant ID is stored in session after authentication

## Setup

### 1. Initial Setup
On first run, the system automatically:
- Creates `tenants` table
- Inserts default tenant with name "default" (id=1)
- Migrates existing users to tenant_id=1

### 2. Creating Additional Tenants
Currently, tenants need to be created directly in database:
```sql
INSERT INTO tenants (name) VALUES ('tenant_a');
INSERT INTO tenants (name) VALUES ('tenant_b');
```

Or via Go code:
```go
authDB.Exec("INSERT INTO tenants (name) VALUES (?)", "tenant_a")
```

### 3. Using the API with Multi-Tenancy

#### Include Header in Requests
```bash
# For Tenant 1 (default)
curl -H "X-Tenant-ID: 1" http://localhost:4500/api/users

# For Tenant 2
curl -H "X-Tenant-ID: 2" http://localhost:4500/api/users
```

#### Frontend Integration
```javascript
// Set tenant ID in fetch requests
const tenantId = 1; // From user selection/session
fetch('/api/users', {
    headers: {
        'X-Tenant-ID': tenantId
    }
})
```

### 4. Data Isolation
- Users can only see users in their tenant
- Knowledge files are isolated per tenant
- AI context uses only tenant's knowledge base
- Devices and follow-up tasks are tenant-scoped

## Migration Notes

### Existing Database
If you have existing data:
1. Run the application - it will auto-create tenants table with default tenant (id=1)
2. Existing users, devices, followups will remain with tenant_id=1
3. New users will be created with tenant_id from request header

### Adding Tenant ID to Existing Tables
The migration code handles:
- Adding `tenant_id` column to `users`, `user_devices`, `followup_tasks`
- Moving `device_jid` from `users` to `user_devices` with proper tenant_id

## Testing Multi-Tenancy

### Test Scenario
```bash
# Tenant 1: Register user A
curl -H "X-Tenant-ID: 1" -X POST http://localhost:4500/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"6281234567890","email":"a@tenant1.com","password":"password123"}'

# Tenant 2: Register user B
curl -H "X-Tenant-ID: 2" -X POST http://localhost:4500/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"6289876543210","email":"b@tenant2.com","password":"password123"}'

# Tenant 1 admin: List users (should only see user A)
curl -H "X-Tenant-ID: 1" http://localhost:4500/api/users

# Tenant 2 admin: List users (should only see user B)
curl -H "X-Tenant-ID: 2" http://localhost:4500/api/users
```

### Knowledge Base Isolation
```bash
# Tenant 1 upload file
curl -H "X-Tenant-ID: 1" -X POST http://localhost:4500/api/upload \
  -F "file=@doc1.pdf"

# Tenant 2 upload file
curl -H "X-Tenant-ID: 2" -X POST http://localhost:4500/api/upload \
  -F "file=@doc2.pdf"

# Tenant 1 config shows only tenant 1 files
curl -H "X-Tenant-ID: 1" http://localhost:4500/api/config

# Tenant 2 config shows only tenant 2 files
curl -H "X-Tenant-ID: 2" http://localhost:4500/api/config
```

## Troubleshooting

### Issue: "tenant not found"
**Solution:** Ensure `tenants` table has the requested tenant_id. Insert default if missing:
```sql
INSERT OR IGNORE INTO tenants (id, name) VALUES (1, 'default');
```

### Issue: Users can see other tenant's data
**Solution:** Check that all queries include `WHERE tenant_id = ?` clause. The middleware should set tenantID in context.

### Issue: Knowledge files mixing between tenants
**Solution:** Files are stored in `tenant_knowledge_files` table with tenant_id. The `rebuildTenantKnowledge` function filters by tenant.

### Issue: Incorrect admin bot for OTP
**Solution:** `getSystemBot(tenantID)` fetches admin user filtered by tenant_id. Ensure admin user exists per tenant:
```sql
INSERT INTO users (username, password, tenant_id, is_admin, is_active)
VALUES ('admin2', 'hashedpass', 2, 1, 1);
```

## Security Considerations

1. **Tenant ID Validation**: Middleware validates tenant exists before processing
2. **Default Fallback**: If X-Tenant-ID missing, defaults to 1 (default tenant)
3. **Session Scoping**: Session stores tenantID; subsequent requests validated
4. **Query Isolation**: All SELECT/UPDATE/DELETE include tenant_id condition
5. **Admin Restriction**: Admin operations limited to own tenant

## Performance

- Tenant-specific knowledge cached in memory (`tenantKnowledge map[int]string`)
- Mutex protects concurrent access to knowledge cache
- Each tenant's knowledge rebuilt independently via `rebuildTenantKnowledge`

## Future Improvements

- Tenant-specific AI provider configuration
- Tenant-specific system prompt customization
- Per-tenant rate limits
- Tenant-specific Google Sheets integration

## Tenant Management API

Admin dapat mengelola tenants melalui API:

### List Tenants
```bash
curl -H "X-Tenant-ID: 1" -b "session_id=..." http://localhost:4500/api/tenants
```

### Create Tenant
```bash
curl -H "X-Tenant-ID: 1" -X POST http://localhost:4500/api/tenants \
  -H "Content-Type: application/json" \
  -d '{"name":"tenant_new"}'
```

### Delete Tenant
```bash
curl -H "X-Tenant-ID: 1" -X DELETE http://localhost:4500/api/tenants/2
```

## Running Tests
