#!/bin/bash
"""
Enhanced deployment script for CFDB ML Pipeline
Validates, tests, and deploys Asset Bundle with safety checks
"""

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TARGET=${1:-dev}
SKIP_TESTS=${2:-false}

echo -e "${BLUE}🚀 CFDB ML Pipeline Deployment${NC}"
echo -e "${BLUE}Target: ${TARGET}${NC}"
echo -e "${BLUE}Skip Tests: ${SKIP_TESTS}${NC}"
echo ""

# Validate target
if [[ "$TARGET" != "dev" && "$TARGET" != "prod" ]]; then
    echo -e "${RED}❌ Invalid target: $TARGET. Use 'dev' or 'prod'${NC}"
    exit 1
fi

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}❌ Databricks CLI not found. Please install it first.${NC}"
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "databricks.yml" ]]; then
    echo -e "${RED}❌ Not in project root (databricks.yml not found)${NC}"
    exit 1
fi

echo -e "${YELLOW}📋 Pre-deployment checks...${NC}"

# 1. Validate bundle configuration
echo -e "${BLUE}🔍 Validating bundle configuration...${NC}"
if ! databricks bundle validate --target "$TARGET"; then
    echo -e "${RED}❌ Bundle validation failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Bundle validation passed${NC}"

# 2. Run tests (unless skipped)
if [[ "$SKIP_TESTS" != "true" ]]; then
    echo -e "${BLUE}🧪 Running tests...${NC}"
    if ! python scripts/run_tests.py --all; then
        echo -e "${RED}❌ Tests failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ All tests passed${NC}"
else
    echo -e "${YELLOW}⚠️  Skipping tests (as requested)${NC}"
fi

# 3. Check for sensitive information
echo -e "${BLUE}🔒 Checking for sensitive information...${NC}"
if grep -r -i "password\|secret\|token\|key" src/ --include="*.py" --include="*.yml" | grep -v ".example" | grep -v "# Replace"; then
    echo -e "${RED}❌ Potential sensitive information found in source code${NC}"
    echo -e "${YELLOW}Please review the above matches and ensure no secrets are committed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ No sensitive information detected${NC}"

# 4. Production-specific checks
if [[ "$TARGET" == "prod" ]]; then
    echo -e "${YELLOW}🏭 Production deployment checks...${NC}"
    
    # Confirm production deployment
    echo -e "${YELLOW}⚠️  You are about to deploy to PRODUCTION${NC}"
    read -p "Are you sure you want to continue? (yes/no): " -r
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        echo -e "${YELLOW}Deployment cancelled${NC}"
        exit 0
    fi
    
    # Check git status
    if [[ -n $(git status --porcelain) ]]; then
        echo -e "${RED}❌ Uncommitted changes detected. Please commit all changes before production deployment${NC}"
        exit 1
    fi
    
    # Check if we're on main/master branch
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$BRANCH" != "main" && "$BRANCH" != "master" ]]; then
        echo -e "${YELLOW}⚠️  You are not on main/master branch (current: $BRANCH)${NC}"
        read -p "Continue anyway? (yes/no): " -r
        if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
            echo -e "${YELLOW}Deployment cancelled${NC}"
            exit 0
        fi
    fi
fi

echo ""
echo -e "${BLUE}🚀 Deploying to $TARGET environment...${NC}"

# Deploy the bundle
if databricks bundle deploy --target "$TARGET"; then
    echo ""
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    
    # Show deployment summary
    echo -e "${BLUE}📊 Deployment Summary:${NC}"
    echo -e "Target: ${TARGET}"
    echo -e "Catalog: cfdb_free_${TARGET}"
    echo -e "Timestamp: $(date)"
    
    if [[ "$TARGET" == "dev" ]]; then
        echo ""
        echo -e "${BLUE}🔗 Next steps for development:${NC}"
        echo -e "1. Run the DLT pipeline: databricks bundle run cfdb_data_refresh --target dev"
        echo -e "2. Check pipeline status in Databricks workspace"
        echo -e "3. Test MCP server connection"
    else
        echo ""
        echo -e "${BLUE}🔗 Production deployment complete:${NC}"
        echo -e "1. Monitor pipeline execution"
        echo -e "2. Verify data quality monitors"
        echo -e "3. Check dashboard availability"
    fi
    
else
    echo -e "${RED}❌ Deployment failed${NC}"
    exit 1
fi