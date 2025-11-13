# Domain: User Management

## AI Quick Start (Cold Start Context)
Handles authentication, authorization, multi-tenant support, and subscription management.
Manages API access control, user dashboards, and billing for multiple product tiers.

**Key AI Request Patterns**: "authentication", "user access", "subscriptions", "API limits", "billing", "multi-tenant"

**Start Files**: `services/auth_service.py`, `services/subscription_service.py`, `services/api_access_service.py`

## When to Work Here
- User asks about login issues, access control, or permission problems
- Requests related to subscription management, billing, or user tiers
- API rate limiting, access control, or authentication integration
- Multi-tenant features or organization-level access control

---

## Current Implementation (AI-Maintained)
*Last updated: 2025-01-12 by AI Assistant*

### Business Purpose
Enables YodaBuffett's multi-product platform strategy by providing secure, scalable user management that supports everything from individual retail users to large institutional clients with complex access requirements.

### Key Capabilities
- **Authentication & Authorization**: Secure login with role-based access control
- **Multi-Tenant Architecture**: Organization-level isolation and management
- **Subscription Management**: Flexible billing for different product tiers
- **API Access Control**: Rate limiting and usage tracking per subscription level
- **User Dashboard**: Personalized interfaces based on subscription and usage

### Architecture Overview
```
User Request → Authentication → Authorization → Rate Limiting → Domain Access → Response
     ↓             ↓              ↓              ↓              ↓             ↓
login_flow → auth_service → permission_check → rate_limiter → api_gateway → user_data
jwt_tokens   user_store     role_manager       usage_tracker  audit_log     billing
```

### Services in Production
- `AuthenticationService`: JWT-based authentication with secure password handling
- `AuthorizationService`: Role-based permissions and feature access control
- `SubscriptionService`: Billing integration and subscription lifecycle management
- `ApiAccessService`: Rate limiting, usage tracking, and API key management
- `OrganizationService`: Multi-tenant organization management and user grouping

### Core Models
- `User`: Individual user accounts with authentication and profile information
- `Organization`: Multi-tenant organization structure with billing and user management
- `Subscription`: Product tier, billing status, and feature access definitions
- `ApiKey`: API access credentials with rate limits and usage tracking
- `Permission`: Granular access control for features and data domains

### API Endpoints (AI-Maintained)
- `POST /auth/login`: User authentication with JWT token generation
- `POST /auth/refresh`: JWT token refresh and session management
- `GET /users/{id}/profile`: User profile information and subscription status
- `POST /organizations/{id}/invite`: Invite users to organization with role assignment
- `GET /subscriptions/{id}/usage`: Current usage statistics and billing information

### Performance Characteristics (AI-Updated)
- **Authentication**: <200ms for login with JWT generation
- **Authorization Check**: <50ms for permission validation
- **Rate Limiting**: <10ms for API rate limit checking
- **User Dashboard Load**: <500ms for complete user profile and analytics
- **Multi-tenant Isolation**: 99.9% data isolation guarantee
- **Concurrent Users**: Supports 10,000+ simultaneous authenticated users

### Dependencies
- **Redis**: Session management, rate limiting counters, and authentication caching
- **PostgreSQL**: User data, organization structure, and subscription information
- **Payment Gateway**: Stripe integration for subscription billing
- **Email Service**: User notifications, password reset, and organization invites
- **All Other Domains**: Authentication and authorization for domain-specific features

### Cross-Domain Integration
- **→ All Domains**: Provides authentication and authorization for all API endpoints
- **← Analytics Domain**: User-specific analytics and dashboard customization
- **← Document Intelligence**: Document processing quotas based on subscription tier
- **← Market Data**: API access limits and data source access based on subscription

### Testing Coverage (AI-Updated)
- **Unit Tests**: 94% coverage across all services (last updated 2025-01-12)
- **Integration Tests**: End-to-end authentication and authorization flows
- **Security Tests**: Penetration testing and vulnerability scanning
- **Performance Tests**: Load testing with 10,000+ concurrent authentication requests
- **Multi-tenant Tests**: Data isolation validation across organization boundaries

### Recent Changes (AI-Generated Log)
- **2025-01-12**: Initial domain structure created with comprehensive documentation
- **[Future updates will be added here by AI assistants]**

---

## Common Patterns and Examples

### Authentication Flow Pattern
```python
# Standard user authentication and JWT generation
auth_service = AuthenticationService()
user = auth_service.authenticate(email, password)
jwt_token = auth_service.generate_jwt(user, expires_in="24h")
```

### Authorization Check Pattern
```python
# Verify user permissions for domain-specific actions
auth_service = AuthorizationService()
can_access = auth_service.check_permission(
    user_id=user.id,
    resource="analytics.correlation_analysis",
    action="read"
)
```

### API Rate Limiting Pattern
```python
# Check and enforce API rate limits
api_service = ApiAccessService()
usage_check = api_service.check_rate_limit(
    api_key=request.api_key,
    endpoint="/analytics/correlations",
    limit_type="per_minute"
)
```

---

## Subscription Tiers and Access Control

### Product Tiers
- **Professional Research**: $500-800/month
  - Full analytics access
  - Real-time data feeds
  - Custom dashboard
  - API access: 10,000 requests/day

- **Retail Tools**: $50-200/month  
  - Basic analytics
  - Delayed market data
  - Standard dashboard
  - API access: 1,000 requests/day

- **Developer API**: Usage-based pricing
  - Full API access
  - Custom rate limits
  - Webhook support
  - Advanced analytics

- **Enterprise**: Custom contracts
  - White-label capabilities
  - Custom integrations
  - Dedicated support
  - Unlimited API access

### Permission Matrix
| Feature | Retail | Professional | Developer | Enterprise |
|---------|--------|-------------|-----------|------------|
| Real-time Data | ❌ | ✅ | ✅ | ✅ |
| Advanced Analytics | ❌ | ✅ | ✅ | ✅ |
| API Access | Limited | Full | Full | Unlimited |
| Custom Dashboards | ❌ | ✅ | ✅ | ✅ |
| Multi-user Organizations | ❌ | Limited | ✅ | ✅ |

---

## AI Maintenance Instructions

### Auto-Update Triggers
Update this file immediately when:
- ✅ New authentication or authorization services added
- ✅ API endpoints created, modified, or removed
- ✅ Subscription tiers or pricing changes
- ✅ Permission matrix or access control changes
- ✅ Performance characteristics change significantly
- ✅ New integrations with external services (payment, email, etc.)

### Update Templates

**New Service Added:**
```markdown
- `[ServiceName]`: [Brief description of user management capability]
```

**Performance Change:**
```markdown
- [Service/Operation]: <[new_time] for [specific_scenario] (was [old_time])
```

**New Subscription Tier:**
```markdown
- **[Tier Name]**: [Price point]
  - [Feature 1]: [Access level]
  - [Feature 2]: [Access level]
  - API access: [Rate limit]
```

**New API Endpoint:**
```markdown
- `[METHOD] [endpoint_path]`: [Description of user management function]
```

### AI Update Checklist
Before finalizing work in this domain:
- [ ] Added any new services to "Services in Production" section
- [ ] Updated performance characteristics if they changed
- [ ] Added new API endpoints to the endpoint list
- [ ] Updated subscription tiers and permission matrix if changed
- [ ] Added entry to "Recent Changes" log with date and description
- [ ] Updated testing coverage statistics
- [ ] Verified cross-domain authentication integration is documented

### Cross-Reference Maintenance
When modifying services in this domain, check if documentation needs updates in:
- `ARCHITECTURE_MAP.md` (if authentication flow or performance changes)
- All other domain documentation (if permission requirements change)
- API documentation (if rate limiting or access control changes)
- Billing/subscription documentation (if tier access changes)