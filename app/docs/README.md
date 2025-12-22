# 🚀 Phase 1: Centralized Ledger - Setup & Installation Guide

## Overview

The **Centralized Ledger** is the foundational component of the BAT Payment Engine. It provides a single source of truth for all financial transactions, implementing double-entry accounting, payment hierarchy, and complete audit trails.

## 📋 Prerequisites

- Python 3.10+
- PostgreSQL 14+ (or SQLite for development)
- Poetry or pip for dependency management
- Alembic for database migrations

## 🏗️ Project Structure

```
app/
├── ledger/
│   ├── __init__.py
│   ├── models.py              # SQLAlchemy models
│   ├── schemas.py             # Pydantic schemas
│   ├── repository.py          # Data access layer
│   ├── service.py             # Business logic
│   ├── router.py              # FastAPI endpoints
│   ├── exceptions.py          # Custom exceptions
│   └── tests/
│       ├── test_service.py    # Unit tests
│       └── test_integration.py # Integration tests
├── core/
│   ├── db.py                  # Database configuration
│   ├── config.py              # Application config
│   └── dependencies.py        # Dependency injection
└── main.py                    # FastAPI application
```

## 📦 Installation

### Step 1: Install Dependencies

```bash
# Using pip
pip install -r requirements.txt

# Using poetry
poetry install
```

**Required packages**:
```txt
fastapi>=0.104.0
uvicorn>=0.24.0
sqlalchemy>=2.0.0
alembic>=1.12.0
pydantic>=2.0.0
python-jose>=3.3.0
passlib>=1.7.4
pytest>=7.4.0
pytest-cov>=4.1.0
```

### Step 2: Configure Environment

Create `.env` file:
```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/bat_database

# For development (SQLite)
# DATABASE_URL=sqlite:///./bat_dev.db

# Application
ENV=development
SECRET_KEY=your-secret-key-here
ALGORITHM=HS256

# Logging
LOG_LEVEL=INFO
```

### Step 3: Update Main Application

Add ledger router to `app/main.py`:

```python
from fastapi import FastAPI
from app.ledger.router import router as ledger_router

app = FastAPI(title="BAT Payment Engine")

# Include ledger router
app.include_router(ledger_router)

# Other routers...
```

### Step 4: Run Database Migration

```bash
# Generate migration (if needed)
alembic revision --autogenerate -m "create_ledger_tables"

# Run migration
alembic upgrade head

# Verify tables created
# Check: ledger_postings, ledger_balances, payment_allocations
```

### Step 5: Verify Installation

```bash
# Run unit tests
pytest app/ledger/tests/test_service.py -v

# Run integration tests
pytest app/ledger/tests/test_integration.py -v

# Check test coverage
pytest app/ledger/tests/ --cov=app.ledger --cov-report=html

# View coverage report
open htmlcov/index.html
```

## 🚀 Quick Start

### Start the Application

```bash
# Development mode
uvicorn app.main:app --reload --port 8000

# Production mode
gunicorn app.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Access API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 📝 Usage Examples

### 1. Create an Obligation

```bash
curl -X POST "http://localhost:8000/ledger/obligations" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "driver_id": 123,
    "lease_id": 456,
    "category": "EZPASS",
    "original_amount": 25.50,
    "reference_type": "EZPASS_TRANSACTION",
    "reference_id": "EZP-001",
    "payment_period_start": "2025-10-26T00:00:00",
    "payment_period_end": "2025-11-01T23:59:59",
    "description": "GWB toll"
  }'
```

### 2. Get Driver Balance

```bash
curl -X GET "http://localhost:8000/ledger/balances/driver/123/lease/456" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### 3. Apply Payment with Hierarchy

```bash
curl -X POST "http://localhost:8000/ledger/payments/apply-hierarchy" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "driver_id": 123,
    "lease_id": 456,
    "payment_amount": 500.00,
    "payment_period_start": "2025-10-26T00:00:00",
    "payment_period_end": "2025-11-01T23:59:59",
    "source_type": "DTR_WEEKLY_ALLOCATION",
    "source_id": "DTR-001"
  }'
```

### 4. Void a Posting

```bash
curl -X POST "http://localhost:8000/ledger/postings/void" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "posting_id": "LP-2025-000001",
    "reason": "Incorrect amount entered"
  }'
```

## 🧪 Testing

### Run All Tests

```bash
# All tests with coverage
pytest app/ledger/tests/ -v --cov=app.ledger --cov-report=term-missing

# Specific test file
pytest app/ledger/tests/test_service.py -v

# Specific test
pytest app/ledger/tests/test_service.py::TestCreatePosting::test_create_posting_success -v

# With detailed output
pytest app/ledger/tests/ -vv -s
```

### Coverage Goals

- **Overall**: 90%+ coverage
- **Service Layer**: 95%+ coverage
- **Repository Layer**: 85%+ coverage

## 📊 Database Schema

### Core Tables

**ledger_postings**
- Every financial transaction (immutable)
- DEBIT = obligation, CREDIT = payment
- Links to driver, lease, vehicle, medallion

**ledger_balances**
- Aggregated obligations
- Tracks payment application
- Supports payment hierarchy (FIFO by due date)

**payment_allocations**
- Payment application history
- Links payments to balances
- Complete audit trail

### Key Indexes

```sql
-- Fast lookups by driver/lease
CREATE INDEX idx_posting_driver_lease ON ledger_postings(driver_id, lease_id);
CREATE INDEX idx_balance_driver_lease_status ON ledger_balances(driver_id, lease_id, status);

-- Payment hierarchy queries
CREATE INDEX idx_balance_category_due_date ON ledger_balances(category, due_date);

-- Source reference lookups
CREATE INDEX idx_posting_source ON ledger_postings(source_type, source_id);
```

## 🔧 Configuration

### Payment Hierarchy

Edit `app/ledger/service.py` if hierarchy needs adjustment:

```python
PAYMENT_HIERARCHY = [
    PostingCategory.TAXES,      # Priority 1
    PostingCategory.EZPASS,     # Priority 2
    PostingCategory.LEASE,      # Priority 3
    PostingCategory.PVB,        # Priority 4
    PostingCategory.TLC,        # Priority 5
    PostingCategory.REPAIRS,    # Priority 6
    PostingCategory.LOANS,      # Priority 7
    PostingCategory.MISC,       # Priority 8
]
```

### ID Formats

Customize ID generation in service layer:

```python
def _generate_posting_id(self) -> str:
    return f"LP-{year}-{count:06d}"  # LP-2025-000001

def _generate_balance_id(self) -> str:
    return f"LB-{year}-{count:06d}"  # LB-2025-000001

def _generate_allocation_id(self) -> str:
    return f"PA-{year}-{count:06d}"  # PA-2025-000001
```

## 🐛 Troubleshooting

### Common Issues

**1. Migration Fails**
```bash
# Drop all tables and recreate
alembic downgrade base
alembic upgrade head

# Or manually check PostgreSQL
psql -U user -d bat_database
\dt  # List tables
```

**2. Import Errors**
```bash
# Check Python path
export PYTHONPATH="${PYTHONPATH}:/path/to/project"

# Or use absolute imports
from app.ledger.service import LedgerService
```

**3. Test Database Issues**
```bash
# Use separate test database
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/bat_test

# Or SQLite for tests
TEST_DATABASE_URL=sqlite:///./test.db
```

**4. Foreign Key Constraints**
```bash
# Ensure referenced tables exist
# Check: drivers, leases, vehicles, medallions, users tables
```

## 📈 Performance Tuning

### Database Optimization

```sql
-- Analyze tables for query optimization
ANALYZE ledger_postings;
ANALYZE ledger_balances;
ANALYZE payment_allocations;

-- Add missing indexes if needed
CREATE INDEX idx_custom ON table_name(column);

-- Check slow queries
SELECT * FROM pg_stat_statements 
WHERE query LIKE '%ledger%' 
ORDER BY total_time DESC;
```

### Application Optimization

```python
# Use bulk operations for imports
session.bulk_insert_mappings(LedgerPosting, postings_list)

# Eager load relationships
session.query(LedgerPosting).options(
    joinedload(LedgerPosting.driver)
).all()

# Use pagination for large result sets
filters.limit = 100
filters.offset = page * 100
```

## 🔐 Security Checklist

- [ ] API authentication enabled
- [ ] Role-based access control implemented
- [ ] Audit logging enabled
- [ ] SQL injection prevention (parameterized queries)
- [ ] Input validation (Pydantic schemas)
- [ ] HTTPS enabled in production
- [ ] Database credentials secured (env variables)
- [ ] Rate limiting configured

## 📚 Additional Resources

- [Payments Roadmap](payments-roadmap.md) - Complete implementation plan
- [Theory Documentation](ledger_theory_docs.md) - Architecture deep dive
- [Usage Examples](ledger_usage_examples.py) - Code examples
- [FastAPI Docs](https://fastapi.tiangolo.com) - FastAPI reference
- [SQLAlchemy 2.0 Docs](https://docs.sqlalchemy.org/en/20/) - ORM reference

## 🤝 Contributing

### Code Style

```bash
# Format code
black app/ledger/

# Lint
pylint app/ledger/

# Type checking
mypy app/ledger/
```

### Pull Request Process

1. Create feature branch: `git checkout -b feature/ledger-enhancement`
2. Write tests for new functionality
3. Ensure all tests pass: `pytest app/ledger/tests/ -v`
4. Update documentation
5. Submit PR with clear description

## 📞 Support

For questions or issues:

- Email: dev-team@batconnect.com
- Slack: #payment-engine channel
- GitHub Issues: [Link to repo]

## ✅ Success Criteria

Before moving to Phase 2:

- [ ] All tests passing (90%+ coverage)
- [ ] Database migration successful
- [ ] API endpoints functional
- [ ] Performance < 200ms response time
- [ ] Finance team training completed
- [ ] Stakeholder sign-off received
- [ ] Documentation reviewed

## 🎯 Next Steps

Once Phase 1 is complete:

1. **Phase 2A**: CURB Import (trips and earnings)
2. **Phase 2B**: EZPass Import (toll matching)
3. **Phase 2C**: PVB Import (violation tracking)
4. Continue through remaining phases...

---

**Version**: 1.0  
**Last Updated**: October 26, 2025  
**Status**: Production Ready ✅