# The Security Problem

Traditional Approach (BAD):

```
┌─────────────┐         ┌──────────────┐         ┌──────────┐
│  Your App   │ ──────▶ │   S3 Bucket  │         │          │
│             │  static │  (all data)  │         │          │
│  Has AWS    │  key    │              │         │          │
│  master key │────────▶│              │         │          │
└─────────────┘         └──────────────┘         └──────────┘
         │
         └──► If app is compromised → Attacker gets FULL access to ALL data

```

The Vended Credentials Solution (GOOD)

Zero-Trust Approach:

```text
┌─────────────┐         ┌──────────────┐         ┌──────────┐
│  Your App   │ ──────▶ │    REST      │ ──────▶ │   S3     │
│             │  auth   │   Catalog    │  STS    │ (scoped) │
│  No S3 keys │────────▶│  (Polaris)   │────────▶│          │
│  at all!    │         │              │  token  │          │
└─────────────┘         └──────────────┘         └──────────┘
┌─────────────┐         ┌──────────────┐
│  App gets   │ ◀────── │   Temporary  │
│  STS token  │  15min  │   token for  │
│  (limited)  │  expiry │   specific   │
└─────────────┘         │   table only │
                        └──────────────┘
```

Why This Architecture?

1. Principle of Least Privilege
- App only gets credentials for the specific table it's accessing
- Token expires in 15-60 minutes
- Cannot access other tables even if compromised
2. Centralized Access Control
- Polaris decides: "You can read table X, but not table Y"
- All access goes through catalog's authorization layer
- Audit trail of who accessed what
3. No Long-Lived Secrets
- No AWS access keys in application code
- No keys in environment variables
- If app is compromised, tokens expire quickly

Real-World Scenario:

```text
User A requests: SELECT * FROM sales_data
           │
           ▼
    Polaris checks: "Does User A have READ on sales_data?"
           │
           ├── YES ──▶ Generate STS token for s3://warehouse/sales_data/*
           │           (expires in 15 min, READ-only)
           │
           └── NO ───▶ Deny access
```

User A's app can ONLY access sales_data for 15 minutes.
Cannot access: hr_data, financials, other tables
Your Setup vs. Production:
Aspect	Your Setup (MinIO)
Auth method	Static keys
Security	Basic
Token lifetime	Permanent
Scope	All or nothing
Audit	Limited
Bottom line: The vended-credentials header enables enterprise-grade security where the catalog acts as a security gateway, issuing just-in-time, least-privilege access tokens. It's overkill for MinIO but essential for multi-tenant cloud data lakes.
