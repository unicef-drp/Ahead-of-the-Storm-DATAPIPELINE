#!/bin/bash
# Deployment script for SPCS
# This script builds, tags, and pushes the container to Snowflake registry

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================="
echo "SPCS Deployment Script"
echo "==========================================${NC}"
echo ""

# Configuration - UPDATE THESE VALUES IF NEEDED
DATABASE="AOTS"  # Snowflake database name
SCHEMA="TC_ECMWF"  # Snowflake schema name
IMAGE_REPOSITORY="AOTS_SERVICES"  # Image repository name in Snowflake
IMAGE_NAME="impact-analysis-pipeline"
IMAGE_TAG="latest"

# Convert to lowercase for Docker tag (required by Snowflake)
DATABASE_LOWER=$(echo "$DATABASE" | tr '[:upper:]' '[:lower:]')
SCHEMA_LOWER=$(echo "$SCHEMA" | tr '[:upper:]' '[:lower:]')
REPOSITORY_LOWER=$(echo "$IMAGE_REPOSITORY" | tr '[:upper:]' '[:lower:]')

echo -e "${BLUE}Configuration:${NC}"
echo "  Database: $DATABASE ($DATABASE_LOWER)"
echo "  Schema: $SCHEMA ($SCHEMA_LOWER)"
echo "  Image Repository: $IMAGE_REPOSITORY ($REPOSITORY_LOWER)"
echo "  Image: $IMAGE_NAME:$IMAGE_TAG"
echo ""

# Step 1: Build the container
echo -e "${BLUE}Step 1: Building container...${NC}"
echo "Command: docker build -f snowflake/Dockerfile -t $IMAGE_NAME:$IMAGE_TAG . --platform=linux/amd64"
echo ""
docker build -f snowflake/Dockerfile -t $IMAGE_NAME:$IMAGE_TAG . --platform=linux/amd64

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Container built successfully${NC}"
else
    echo -e "${RED}✗ Container build failed${NC}"
    exit 1
fi

echo ""

# Step 2: Get registry URL
echo -e "${BLUE}Step 2: Getting Snowflake registry URL...${NC}"
REGISTRY_URL=$(snow spcs image-registry url --connection default 2>/dev/null | head -1)

if [ -z "$REGISTRY_URL" ]; then
    echo -e "${RED}✗ Failed to get registry URL. Make sure Snowflake CLI is configured.${NC}"
    echo "Run: snow connection add <your-connection-name>"
    exit 1
fi

echo -e "${GREEN}✓ Registry URL: $REGISTRY_URL${NC}"
echo ""

# Step 3: Tag the image
FULL_IMAGE_TAG="${REGISTRY_URL}/${DATABASE_LOWER}/${SCHEMA_LOWER}/${REPOSITORY_LOWER}/${IMAGE_NAME}:${IMAGE_TAG}"

echo -e "${BLUE}Step 3: Tagging image...${NC}"
echo "Command: docker tag $IMAGE_NAME:$IMAGE_TAG $FULL_IMAGE_TAG"
echo ""
docker tag $IMAGE_NAME:$IMAGE_TAG $FULL_IMAGE_TAG

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Image tagged successfully${NC}"
else
    echo -e "${RED}✗ Image tagging failed${NC}"
    exit 1
fi

echo ""

# Step 4: Login to registry
echo -e "${BLUE}Step 4: Logging into Snowflake registry...${NC}"
echo -e "${YELLOW}Note: If login fails, you may need to create the image repository first in Snowflake:${NC}"
echo "  CREATE OR REPLACE IMAGE REPOSITORY ${IMAGE_REPOSITORY};"
echo ""
snow spcs image-registry login --connection default

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Logged into registry successfully${NC}"
else
    echo -e "${RED}✗ Registry login failed${NC}"
    echo -e "${YELLOW}Try running: snow spcs image-registry login --connection default${NC}"
    exit 1
fi

echo ""

# Step 5: Push the image
echo -e "${BLUE}Step 5: Pushing image to Snowflake registry...${NC}"
echo "This may take several minutes depending on image size..."
echo "Command: docker push $FULL_IMAGE_TAG"
echo ""
echo -e "${YELLOW}If you get a 401 Unauthorized error:${NC}"
echo "  1. Ensure image repository exists: CREATE OR REPLACE IMAGE REPOSITORY ${SERVICE_LOWER};"
echo "  2. Check your permissions on the database/schema"
echo "  3. Try logging in again: snow spcs image-registry login --connection default"
echo ""
docker push $FULL_IMAGE_TAG

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Image pushed successfully${NC}"
else
    echo -e "${RED}✗ Image push failed${NC}"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "1. Verify image repository exists in Snowflake:"
    echo "   SELECT * FROM INFORMATION_SCHEMA.IMAGE_REPOSITORIES WHERE REPOSITORY_NAME = '${IMAGE_REPOSITORY}';"
    echo ""
    echo "2. If it doesn't exist, create it:"
    echo "   USE DATABASE ${DATABASE};"
    echo "   USE SCHEMA ${SCHEMA};"
    echo "   CREATE OR REPLACE IMAGE REPOSITORY ${IMAGE_REPOSITORY};"
    echo ""
    echo "3. Re-authenticate:"
    echo "   snow spcs image-registry login --connection default"
    echo ""
    echo "4. Try pushing again:"
    echo "   docker push $FULL_IMAGE_TAG"
    exit 1
fi

echo ""
echo -e "${GREEN}=========================================="
echo "✓ Deployment completed successfully!"
echo "==========================================${NC}"
echo ""