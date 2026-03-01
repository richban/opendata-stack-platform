#!/bin/bash

##############################################################################
# Kafka Cleanup Script
# Removes all Kafka data and resets topics
##############################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Kafka Cleanup Script${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
  echo -e "${RED}Error: Docker is not running${NC}"
  exit 1
fi

# Display current size
echo -e "${YELLOW}Current Kafka data size:${NC}"
docker volume inspect opendata-stack-platform_kafka_data 2>/dev/null | grep -q "." &&
  echo "  Volume exists" || echo "  Volume does not exist"
echo ""

# Ask for confirmation
read -p "Are you sure you want to delete ALL Kafka data? This will free up space but lose all topics. (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
  echo -e "${YELLOW}Cancelled.${NC}"
  exit 0
fi

echo -e "${YELLOW}Starting cleanup...${NC}"
echo ""

# Step 1: Stop containers
echo -e "${YELLOW}Step 1: Stopping Docker containers...${NC}"
docker-compose down 2>/dev/null || true
sleep 2
echo -e "${GREEN}✓ Containers stopped${NC}"
echo ""

# Step 2: Remove Kafka volume
echo -e "${YELLOW}Step 2: Removing Kafka volume...${NC}"
docker volume rm opendata-stack-platform_kafka_data 2>/dev/null || true
echo -e "${GREEN}✓ Kafka volume removed${NC}"
echo ""

# Step 3: Remove Zookeeper volume (optional, for complete reset)
read -p "Also reset Zookeeper? (yes/no): " -r
echo
if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
  echo -e "${YELLOW}Step 3: Removing Zookeeper volume...${NC}"
  docker volume rm opendata-stack-platform_zookeeper_data 2>/dev/null || true
  docker volume rm opendata-stack-platform_zookeeper_logs 2>/dev/null || true
  echo -e "${GREEN}✓ Zookeeper volumes removed${NC}"
  echo ""
fi
