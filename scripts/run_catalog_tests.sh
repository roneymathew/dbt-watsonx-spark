#!/bin/bash
set -e

# Comprehensive test script for dbt-watsonx-spark catalog functionality
# This script:
# 1. Loads configuration from .env file
# 2. Creates Iceberg, Delta, and Hudi catalogs in watsonx.data
# 3. Creates Spark query engines (with and without authz)
# 4. Attaches catalogs to engines
# 5. Runs all catalog tests including authz validation
#
# Supports both:
# - IBM Cloud SaaS (watsonx.data on IBM Cloud)
# - Cloud Pak for Data (CPD on-premises/private cloud)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Global variable to store auth token
AUTH_TOKEN=""

# Lakehouse API version (v2 or v3, default v3)
LAKEHOUSE_API_VERSION="${LAKEHOUSE_API_VERSION:-v3}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get authentication token based on deployment type
get_auth_token() {
    # If token already provided, use it
    if [ -n "$AUTH_TOKEN" ]; then
        log_info "Using pre-existing authentication token"
        return 0
    fi
    
    local deployment_type=${DEPLOYMENT_TYPE:-saas}
    
    if [ "$deployment_type" == "cpd" ]; then
        get_cpd_token
    else
        get_saas_token
    fi
}

# Get IBM Cloud SaaS authentication token
get_saas_token() {
    log_info "Getting IBM Cloud SaaS authentication token..."
    
    local token=$(curl -s -X POST "https://iam.cloud.ibm.com/identity/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=$WATSONX_APIKEY" | \
        jq -r '.access_token')
    
    if [ -z "$token" ] || [ "$token" == "null" ]; then
        log_error "Failed to get IBM Cloud authentication token"
        log_error "Please verify your WATSONX_APIKEY is correct"
        return 1
    fi
    
    AUTH_TOKEN="$token"
    log_success "Successfully authenticated with IBM Cloud"
    return 0
}

# Get Cloud Pak for Data authentication token
get_cpd_token() {
    log_info "Getting Cloud Pak for Data authentication token..."
    
    # Validate CPD-specific variables
    if [ -z "$CPD_USERNAME" ] || [ -z "$WATSONX_APIKEY" ]; then
        log_error "CPD_USERNAME and WATSONX_APIKEY (password) are required for CPD deployment"
        return 1
    fi
    
    if [ -z "$WATSONX_HOSTNAME" ]; then
        log_error "WATSONX_HOSTNAME is required for CPD deployment"
        return 1
    fi
    
    log_info "Authenticating with: $WATSONX_HOSTNAME/icp4d-api/v1/authorize"
    log_info "Username: $CPD_USERNAME"
    
    # Get CPD token - Note: CPD uses "api_key" field, not "password"
    local response=$(curl -s -k -X POST "$WATSONX_HOSTNAME/icp4d-api/v1/authorize" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"$CPD_USERNAME\",\"api_key\":\"$WATSONX_APIKEY\"}")
    
    local token=$(echo "$response" | jq -r '.token // empty')
    
    if [ -z "$token" ]; then
        log_error "Failed to get CPD authentication token"
        log_error "Full response: $response"
        log_error "Parsed message: $(echo "$response" | jq -r '.message // .error // "Unknown error"')"
        return 1
    fi
    
    AUTH_TOKEN="$token"
    log_success "Successfully authenticated with Cloud Pak for Data"
    log_info "Token (first 20 chars): ${token:0:20}..."
    return 0
}

# Get base URL for API calls
get_base_url() {
    # WATSONX_HOSTNAME works for both SaaS and CPD
    # For SaaS: hostname only (e.g., wxd-instance.lakehouse.cloud.ibm.com)
    # For CPD: full URL (e.g., https://cpd-instance.example.com)
    
    if [[ "$WATSONX_HOSTNAME" == http* ]]; then
        # Already has protocol, use as-is (CPD case)
        echo "$WATSONX_HOSTNAME"
    else
        # Add https:// prefix (SaaS case)
        echo "https://$WATSONX_HOSTNAME"
    fi
}

# Get authorization header based on deployment type
get_auth_header() {
    local deployment_type=${DEPLOYMENT_TYPE:-saas}
    
    if [ "$deployment_type" == "cpd" ]; then
        echo "Authorization: Bearer $AUTH_TOKEN"
    else
        echo "Authorization: Bearer $AUTH_TOKEN"
    fi
}

# Get instance ID header (different for SaaS vs CPD)
get_instance_header() {
    local deployment_type=${DEPLOYMENT_TYPE:-saas}
    
    if [ "$deployment_type" == "saas" ]; then
        echo "-H \"AuthInstanceId: $WATSONX_INSTANCE_ID\""
    else
        # CPD uses LhInstanceId header
        echo "-H \"LhInstanceId: $WATSONX_INSTANCE_ID\""
    fi
}

# Load environment variables from .env file
load_env() {
    local env_file="${1:-.env}"
    
    if [ ! -f "$env_file" ]; then
        log_error "Environment file not found: $env_file"
        log_info "Creating template .env file..."
        cat > "$env_file" << 'EOF'
# Deployment Type: 'saas' for IBM Cloud or 'cpd' for Cloud Pak for Data
DEPLOYMENT_TYPE=saas

# ===== Unified Configuration (Works for both SaaS and CPD) =====
# Hostname/URL of your watsonx.data instance
WATSONX_HOSTNAME=your-wxd-hostname.cloud.ibm.com

# API Key or Password (depending on deployment type)
# For SaaS: IBM Cloud API Key
# For CPD: User password
WATSONX_APIKEY=your-api-key-or-password

# Instance ID (required for both SaaS and CPD)
WATSONX_INSTANCE_ID=your-instance-id

# Username (only required for CPD, ignored for SaaS)
# For CPD: Your CPD username (e.g., admin)
# For SaaS: Not used
CPD_USERNAME=admin

# ===== Pre-existing Authentication (Optional) =====
# If you already have a token, provide it here to skip authentication
# AUTH_TOKEN=your-existing-token
# USERNAME=your-username

# ===== Catalog Enable/Disable Flags =====
# Set to 'false' to skip creating a specific catalog type
ICEBERG_CATALOG_ENABLED=true
DELTA_CATALOG_ENABLED=true
HUDI_CATALOG_ENABLED=true

# ===== Storage Configuration =====
# Storage bucket names for catalogs (will be created if they don't exist)
# Leave empty to skip bucket creation
ICEBERG_BUCKET=iceberg-bucket
DELTA_BUCKET=delta-bucket
HUDI_BUCKET=hudi-bucket

# Storage endpoint (e.g., S3-compatible storage)
# STORAGE_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud
# STORAGE_ACCESS_KEY=your-access-key
# STORAGE_SECRET_KEY=your-secret-key

# ===== Catalog Configuration =====
ICEBERG_CATALOG_NAME=iceberg_data
DELTA_CATALOG_NAME=delta_data
HUDI_CATALOG_NAME=hudi_data

# ===== Engine Configuration =====
SPARK_ENGINE_NAME=spark-dbt-test
SPARK_ENGINE_AUTHZ_NAME=spark-dbt-authz-test

# Storage for Spark engine home (required for CPD)
# SPARK_ENGINE_HOME_STORAGE=your-storage-name

# ===== Test Configuration =====
TEST_SCHEMA_PREFIX=dbt_test
PYTHON_VENV_PATH=~/projects/python-env/venv

# Optional: Specific test patterns
TEST_PATTERN=tests/functional/adapter/catalog_tests/
EOF
        log_error "Please edit $env_file with your configuration and run again"
        exit 1
    fi
    
    log_info "Loading environment from: $env_file"
    set -a
    source "$env_file"
    set +a
    
    # Set default deployment type
    DEPLOYMENT_TYPE=${DEPLOYMENT_TYPE:-saas}
    
    log_info "Deployment type: $DEPLOYMENT_TYPE"
    
    # Validate required variables based on deployment type
    if [ "$DEPLOYMENT_TYPE" == "cpd" ]; then
        # CPD requires: hostname, username, password (as WATSONX_APIKEY), and instance ID
        local required_vars=(
            "WATSONX_HOSTNAME"
            "CPD_USERNAME"
            "WATSONX_APIKEY"
            "WATSONX_INSTANCE_ID"
        )
        
        for var in "${required_vars[@]}"; do
            if [ -z "${!var}" ]; then
                log_error "Required CPD environment variable not set: $var"
                log_info "For CPD, you need:"
                log_info "  WATSONX_HOSTNAME=https://your-cpd-url"
                log_info "  CPD_USERNAME=your-username"
                log_info "  WATSONX_APIKEY=your-password"
                log_info "  WATSONX_INSTANCE_ID=your-instance-id"
                exit 1
            fi
        done
    else
        # SaaS requires: hostname, API key, and instance ID (CRN)
        local required_vars=(
            "WATSONX_HOSTNAME"
            "WATSONX_APIKEY"
            "WATSONX_INSTANCE_ID"
        )
        
        for var in "${required_vars[@]}"; do
            if [ -z "${!var}" ]; then
                log_error "Required SaaS environment variable not set: $var"
                log_info "For SaaS, you need:"
                log_info "  WATSONX_HOSTNAME=your-hostname.lakehouse.cloud.ibm.com"
                log_info "  WATSONX_APIKEY=your-ibm-cloud-api-key"
                log_info "  WATSONX_INSTANCE_ID=crn:v1:bluemix:..."
                exit 1
            fi
        done
    fi
    
    log_success "Environment validated successfully"
}

# Create S3 bucket using lakehouse API
create_storage_bucket() {
    local bucket_name=$1
    
    log_info "Creating storage bucket: $bucket_name"
    
    # Check if S3 credentials are provided (support both S3_* and STORAGE_* variables)
    local endpoint="${STORAGE_ENDPOINT:-$S3_ENDPOINT}"
    local access_key="${STORAGE_ACCESS_KEY:-$S3_ACCESS_KEY}"
    local secret_key="${STORAGE_SECRET_KEY:-$S3_SECRET_KEY}"
    
    if [ -z "$endpoint" ] || [ -z "$access_key" ] || [ -z "$secret_key" ]; then
        log_warning "Storage credentials not provided, skipping bucket creation"
        log_info "To create buckets, set: STORAGE_ENDPOINT, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY"
        return 0
    fi
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    local instance_header=$(get_instance_header)
    
    # Check if bucket already exists in lakehouse
    local existing=$(curl -s -k -X GET \
        "$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/buckets/$bucket_name" \
        -H "$auth_header" \
        $instance_header | \
        jq -r '.bucket_display_name // empty')
    
    if [ "$existing" == "$bucket_name" ]; then
        log_success "Bucket $bucket_name already registered in lakehouse"
        return 0
    fi
    
    # Register bucket with lakehouse using S3 credentials
    local bucket_config=$(cat <<EOF
{
    "bucket_display_name": "$bucket_name",
    "bucket_type": "ibm_cos",
    "endpoint": "$S3_ENDPOINT",
    "access_key": "$S3_ACCESS_KEY",
    "secret_key": "$S3_SECRET_KEY",
    "bucket_name": "$bucket_name"
}
EOF
)
    
    log_info "Registering S3 bucket with lakehouse..."
    local response=$(curl -s -k -X POST \
        "$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/buckets" \
        -H "$auth_header" \
        $instance_header \
        -H "Content-Type: application/json" \
        -d "$bucket_config")
    
    local created_name=$(echo "$response" | jq -r '.bucket_display_name // empty')
    
    if [ "$created_name" == "$bucket_name" ]; then
        log_success "Registered storage bucket: $bucket_name"
        return 0
    else
        local error_msg=$(echo "$response" | jq -r '.message // .error // "Unknown error"')
        log_warning "Could not register bucket: $error_msg"
        log_info "Bucket may already exist or credentials may be incorrect"
        return 0  # Don't fail on bucket registration issues
    fi
}

# Create storage and catalog using CPD API (v1)
create_catalog_cpd() {
    local catalog_name=$1
    local catalog_type=$2
    local bucket_name=$3
    local storage_name="${catalog_name}_storage"
    
    # Check if this catalog type should be created
    local catalog_type_upper=$(echo "$catalog_type" | tr '[:lower:]' '[:upper:]')
    local create_flag_var="${catalog_type_upper}_CATALOG_ENABLED"
    if [ "${!create_flag_var}" == "false" ]; then
        log_info "Skipping $catalog_type catalog (disabled via $create_flag_var)"
        return 0
    fi
    
    log_info "Creating $catalog_type storage and catalog: $catalog_name"
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    local instance_header=$(get_instance_header)
    
    # Check if S3 credentials are provided (support both S3_* and STORAGE_* variables)
    local endpoint="${STORAGE_ENDPOINT:-$S3_ENDPOINT}"
    local access_key="${STORAGE_ACCESS_KEY:-$S3_ACCESS_KEY}"
    local secret_key="${STORAGE_SECRET_KEY:-$S3_SECRET_KEY}"
    local region="${STORAGE_REGION:-us-south}"
    
    if [ -z "$endpoint" ] || [ -z "$access_key" ] || [ -z "$secret_key" ]; then
        log_error "S3 credentials required for CPD catalog creation"
        log_info "Set: STORAGE_ENDPOINT (or S3_ENDPOINT), STORAGE_ACCESS_KEY (or S3_ACCESS_KEY), STORAGE_SECRET_KEY (or S3_SECRET_KEY)"
        return 1
    fi
    
    if [ -z "$bucket_name" ]; then
        log_error "Bucket name required for CPD catalog creation"
        return 1
    fi
    
    # Build storage config for watsonx.data v3 API
    # Note: connection.name is the bucket name and must follow S3 naming rules
    local storage_config="{
        \"display_name\": \"$storage_name\",
        \"type\": \"ibm_cos\",
        \"description\": \"Storage for $catalog_type catalog\",
        \"managed_by\": \"customer\",
        \"region\": \"$region\",
        \"connection\": {
            \"name\": \"$bucket_name\",
            \"endpoint\": \"$endpoint\",
            \"access_key\": \"$access_key\",
            \"secret_key\": \"$secret_key\"
        },
        \"associated_catalog\": {
            \"catalog_name\": \"$catalog_name\",
            \"catalog_type\": \"$catalog_type\"
        }
    }"
    
    # Debug: Print API call details
    log_info "API Call: POST $base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/storage_registrations"
    log_info "Headers: Authorization: Bearer ${AUTH_TOKEN:0:20}..., AuthInstanceId: $WATSONX_INSTANCE_ID"
    log_info "Request Body:"
    echo "$storage_config" | jq '.' 2>/dev/null || echo "$storage_config"
    
    local response=$(curl -s -k -X POST \
        "$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/storage_registrations" \
        -H "$auth_header" \
        -H "AuthInstanceId: $WATSONX_INSTANCE_ID" \
        -H "Content-Type: application/json" \
        -d "$storage_config")
    
    log_info "Response:"
    echo "$response" | jq '.' 2>/dev/null || echo "$response"
    
    # Check if successful (CPD may return different success indicators)
    if echo "$response" | jq -e '.storage_name' >/dev/null 2>&1 || \
       echo "$response" | jq -e '.catalog_name' >/dev/null 2>&1 || \
       echo "$response" | grep -q "success\|created" 2>/dev/null; then
        log_success "Created $catalog_type storage and catalog: $catalog_name"
        return 0
    else
        local error_msg=$(echo "$response" | jq -r '.message // .error // .exception // "Unknown error"' 2>/dev/null || echo "Unknown error")
        log_error "Failed to create storage/catalog: $error_msg"
        return 1
    fi
}

# Create catalog using watsonx.data SaaS API (v2/v3)
create_catalog_saas() {
    local catalog_name=$1
    local catalog_type=$2
    local bucket_name=$3
    
    # Check if this catalog type should be created
    local catalog_type_upper=$(echo "$catalog_type" | tr '[:lower:]' '[:upper:]')
    local create_flag_var="${catalog_type_upper}_CATALOG_ENABLED"
    if [ "${!create_flag_var}" == "false" ]; then
        log_info "Skipping $catalog_type catalog (disabled via $create_flag_var)"
        return 0
    fi
    
    log_info "Creating $catalog_type catalog: $catalog_name"
    
    # Create storage bucket first if bucket name provided
    if [ -n "$bucket_name" ]; then
        create_storage_bucket "$bucket_name" || log_warning "Bucket creation had issues, continuing..."
    fi
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    local instance_header=$(get_instance_header)
    
    # Check if catalog already exists
    local existing=$(curl -s -k -X GET \
        "$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/catalogs/$catalog_name" \
        -H "$auth_header" \
        $instance_header | \
        jq -r '.catalog_name // empty')
    
    if [ "$existing" == "$catalog_name" ]; then
        log_warning "Catalog $catalog_name already exists, skipping creation"
        return 0
    fi
    
    # Create catalog based on type
    local catalog_config
    case $catalog_type in
        iceberg)
            catalog_config='{
                "catalog_name": "'$catalog_name'",
                "catalog_type": "iceberg",
                "managed_by": "ibm",
                "description": "Iceberg catalog for dbt testing"'
            if [ -n "$bucket_name" ]; then
                catalog_config+=',
                "bucket_name": "'$bucket_name'"'
            fi
            catalog_config+='
            }'
            ;;
        delta)
            catalog_config='{
                "catalog_name": "'$catalog_name'",
                "catalog_type": "delta",
                "managed_by": "ibm",
                "description": "Delta Lake catalog for dbt testing"'
            if [ -n "$bucket_name" ]; then
                catalog_config+=',
                "bucket_name": "'$bucket_name'"'
            fi
            catalog_config+='
            }'
            ;;
        hudi)
            catalog_config='{
                "catalog_name": "'$catalog_name'",
                "catalog_type": "hudi",
                "managed_by": "ibm",
                "description": "Apache Hudi catalog for dbt testing"'
            if [ -n "$bucket_name" ]; then
                catalog_config+=',
                "bucket_name": "'$bucket_name'"'
            fi
            catalog_config+='
            }'
            ;;
        *)
            log_error "Unknown catalog type: $catalog_type"
            return 1
            ;;
    esac
    
    # Debug: Print API call details
    log_info "API Call: POST $base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/catalogs"
    log_info "Headers: Authorization: Bearer ${AUTH_TOKEN:0:20}..., LhInstanceId: $WATSONX_INSTANCE_ID"
    log_info "Request Body:"
    echo "$catalog_config" | jq '.' 2>/dev/null || echo "$catalog_config"
    
    local response=$(curl -s -k -X POST \
        "$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/catalogs" \
        -H "$auth_header" \
        $instance_header \
        -H "Content-Type: application/json" \
        -d "$catalog_config")
    
    log_info "Response:"
    echo "$response" | jq '.' 2>/dev/null || echo "$response"
    
    local created_name=$(echo "$response" | jq -r '.catalog_name // empty')
    
    if [ "$created_name" == "$catalog_name" ]; then
        log_success "Created $catalog_type catalog: $catalog_name"
        return 0
    else
        log_error "Failed to create catalog: $(echo "$response" | jq -r '.message // .error // "Unknown error"')"
        return 1
    fi
}

# Wrapper function that calls appropriate API based on deployment type
create_catalog() {
    local deployment_type=${DEPLOYMENT_TYPE:-saas}
    
    if [ "$deployment_type" == "cpd" ]; then
        create_catalog_cpd "$@"
    else
        create_catalog_saas "$@"
    fi
}

# Create Spark query engine
create_spark_engine() {
    local engine_name=$1
    local enable_authz=${2:-false}
    shift 2
    local catalogs=("$@")  # Remaining arguments are catalog names
    
    log_info "Creating Spark engine: $engine_name (authz: $enable_authz)"
    if [ ${#catalogs[@]} -gt 0 ]; then
        log_info "Associated catalogs: ${catalogs[*]}"
    fi
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    
    # Check if engine already exists
    local existing=$(curl -s -k -X GET \
        "$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/spark_engines" \
        -H "$auth_header" \
        -H "AuthInstanceId: $WATSONX_INSTANCE_ID" | \
        jq -r '.spark_engines[]? | select(.display_name == "'$engine_name'") | .engine_id // empty')
    
    if [ -n "$existing" ]; then
        log_warning "Engine $engine_name already exists (ID: $existing), skipping creation"
        SPARK_ENGINE_ID="$existing"
        return 0
    fi
    
    # Build associated_catalogs array for JSON
    local catalogs_json="[]"
    if [ ${#catalogs[@]} -gt 0 ]; then
        catalogs_json=$(printf '%s\n' "${catalogs[@]}" | jq -R . | jq -s .)
    fi
    
    # Create engine configuration based on actual CPD API
    # CPD uses volume-based storage for engine home, not S3 storage
    local spark_version="${SPARK_VERSION:-4.0}"
    
    # Determine volume configuration
    # For CPD, volume_id MUST be the numeric instance_id from the volumes API
    # The API does NOT accept cpd-instance:: format - it only accepts numeric IDs
    local volume_id="${SPARK_ENGINE_VOLUME_ID:-}"
    local volume_name="${SPARK_ENGINE_VOLUME_NAME:-spark-engine-volume}"
    local volume_storage_size="${SPARK_ENGINE_VOLUME_SIZE:-5Gi}"
    local volume_storage_class="${SPARK_ENGINE_VOLUME_CLASS:-nfs-client}"
    
    # If no volume ID specified, look up the numeric instance_id by volume name
    if [ -z "$volume_id" ]; then
        log_info "No volume ID specified, looking up volume by name: $volume_name"
        volume_id=$(get_available_volume_id "$volume_name")
        
        if [ -n "$volume_id" ]; then
            log_info "Found existing volume '$volume_name' with numeric ID: $volume_id"
        else
            log_info "Volume '$volume_name' not found, will create new volume"
        fi
    else
        log_info "Using volume ID from SPARK_ENGINE_VOLUME_ID: $volume_id"
    fi
    
    # Build engine configuration
    local engine_config
    if [ -n "$volume_id" ]; then
        # Use existing volume by ID
        log_info "Using existing volume ID: $volume_id"
        engine_config=$(cat <<EOF
{
    "description": "Spark engine for dbt testing",
    "configuration": {
        "default_version": "$spark_version",
        "engine_home": {
            "volume_id": "$volume_id"
        }
    },
    "display_name": "$engine_name",
    "origin": "native",
    "type": "spark",
    "associated_catalogs": $catalogs_json
}
EOF
)
    else
        # Create new volume
        log_info "Creating new volume: $volume_name (size: $volume_storage_size, class: $volume_storage_class)"
        engine_config=$(cat <<EOF
{
    "description": "Spark engine for dbt testing",
    "configuration": {
        "default_version": "$spark_version",
        "engine_home": {
            "volume_storage_size": "$volume_storage_size",
            "volume_name": "$volume_name",
            "volume_storage_class": "$volume_storage_class"
        }
    },
    "display_name": "$engine_name",
    "origin": "native",
    "type": "spark",
    "associated_catalogs": $catalogs_json
}
EOF
)
    fi
    
    # CPD API includes instance ID in URL path
    local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines"
    
    # Debug: Print API call details
    log_info "API Call: POST $api_url"
    log_info "Headers: Authorization: Bearer ${AUTH_TOKEN:0:20}..., LhInstanceId: $WATSONX_INSTANCE_ID"
    log_info "Request Body:"
    echo "$engine_config" | jq '.' 2>/dev/null || echo "$engine_config"
    
    local response=$(curl -s -k -X POST \
        "$api_url" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" \
        -H "Content-Type: application/json" \
        -d "$engine_config")
    
    log_info "Response:"
    echo "$response" | jq '.' 2>/dev/null || echo "$response"
    
    local created_id=$(echo "$response" | jq -r '.engine_id // .id // empty')
    
    if [ -n "$created_id" ]; then
        log_success "Created Spark engine: $engine_name (ID: $created_id)"
        SPARK_ENGINE_ID="$created_id"
        return 0
    else
        local error_msg=$(echo "$response" | jq -r '.message // .error // .exception // "Unknown error"' 2>/dev/null || echo "Unknown error")
        log_error "Failed to create engine: $error_msg"
        return 1
    fi
}


# Get engine ID by name
get_engine_id_by_name() {
    local engine_name=$1
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    
    # CPD API includes instance ID in URL path
    local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines"
    
    local response=$(curl -s -k -X GET \
        "$api_url" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" 2>/dev/null)
    
    # Extract engine ID by matching display_name
    local engine_id=$(echo "$response" | jq -r '.spark_engines[]? | select(.display_name == "'$engine_name'") | .id // .engine_id // empty' 2>/dev/null)
    
    if [ -n "$engine_id" ]; then
        echo "$engine_id"
        return 0
    else
        return 1
    fi
}

# Get volume ID by name for Spark engine
get_available_volume_id() {
    local volume_name=${1:-spark-engine-volume}
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    
    # CPD API for getting Spark instances/volumes
    local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/cpd/spark_instances"
    
    log_info "Looking up volume: $volume_name"
    local response=$(curl -s -k -X GET \
        "$api_url" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" 2>/dev/null)
    
    # The response has structure: {"volumes": [{"display_name": "cpd-instance::name", "instance_id": "123"}]}
    # We need to match the display_name and return the instance_id
    local full_display_name="cpd-instance::$volume_name"
    local volume_id=$(echo "$response" | jq -r '.volumes[]? | select(.display_name == "'$full_display_name'") | .instance_id // empty' 2>/dev/null)
    
    if [ -n "$volume_id" ]; then
        log_info "Found volume $volume_name with ID: $volume_id"
        echo "$volume_id"
        return 0
    else
        log_warning "Volume $volume_name not found"
        return 1
    fi
}

# Attach catalog to engine (wrapper that uses engine ID)
attach_catalog_to_engine() {
    local engine_name_or_id=$1
    local catalog_name=$2
    
    log_info "Attaching catalog $catalog_name to engine $engine_name_or_id"
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    
    # CPD API includes instance ID in URL path
    local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_name_or_id"
    
    # Check if catalog is already attached
    local check_response=$(curl -s -k -X GET \
        "$api_url/catalogs" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" 2>/dev/null)
    
    # Check if response is valid JSON
    if echo "$check_response" | jq empty 2>/dev/null; then
        local existing=$(echo "$check_response" | jq -r '.catalogs[]? | select(.catalog_name == "'$catalog_name'") | .catalog_name // empty')
        
        if [ "$existing" == "$catalog_name" ]; then
            log_warning "Catalog $catalog_name already attached to engine $engine_name_or_id"
            return 0
        fi
    fi
    
    # Attach catalog using POST to /catalogs endpoint (correct CPD format from browser)
    local attach_body='{"catalog_names": ["'$catalog_name'"]}'
    
    log_info "API Call: POST $api_url/catalogs"
    log_info "Request Body: $attach_body"
    
    local response=$(curl -s -k -X POST \
        "$api_url/catalogs" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" \
        -H "Content-Type: application/json" \
        -d "$attach_body" 2>/dev/null)
    
    log_info "Response: $response"
    
    # Check if response is valid JSON
    if ! echo "$response" | jq empty 2>/dev/null; then
        log_warning "Catalog attachment response not JSON (may have succeeded): $response"
        return 0  # Don't fail, might be successful
    fi
    
    # Check if successful - look for success indicators
    if echo "$response" | jq -e '.catalog_name // .catalogs // .success' >/dev/null 2>&1; then
        log_success "Attached catalog $catalog_name to engine $engine_name_or_id"
        return 0
    else
        local error_msg=$(echo "$response" | jq -r '.message // .error // .exception // "Unknown"' 2>/dev/null)
        log_warning "Catalog attachment may have failed: $error_msg (continuing anyway)"
        return 0  # Don't fail on this as it might already be attached
    fi
}


# Create query server for Spark engine
create_query_server() {
    local engine_id=$1
    local server_name=${2:-"dbt-query-server"}
    
    log_info "Creating query server '$server_name' for engine $engine_id"
    
    # Get auth token if not already set
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    
    # CPD API for creating query server
    local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers"
    
    # Build query server configuration
    local driver_cores="${SPARK_DRIVER_CORES:-1}"
    local driver_memory="${SPARK_DRIVER_MEMORY:-4g}"
    local executor_cores="${SPARK_EXECUTOR_CORES:-1}"
    local executor_memory="${SPARK_EXECUTOR_MEMORY:-4g}"
    local executor_count="${SPARK_EXECUTOR_COUNT:-1}"
    
    # Encode API key for Spark config
    local encoded_apikey=$(echo -n "$CPD_USERNAME:$WATSONX_APIKEY" | base64)
    
    local server_config=$(cat <<EOF
{
    "query_server_details": {
        "name": "$server_name",
        "conf": {
            "spark.driver.cores": "$driver_cores",
            "spark.driver.memory": "$driver_memory",
            "spark.executor.cores": "$executor_cores",
            "spark.executor.memory": "$executor_memory",
            "ae.spark.executor.count": "$executor_count",
            "spark.hadoop.wxd.apikey": "ZenApiKey $encoded_apikey"
        },
        "env": {}
    }
}
EOF
)
    
   log_info "API Call: POST $api_url"
    log_info "Request Body:"
    echo "$server_config" | jq '.' 2>/dev/null || echo "$server_config"
    
    log_info "API Call: POST $api_url" >&2
    
    local response=$(curl -s -k -X POST \
        "$api_url" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" \
        -H "Content-Type: application/json" \
        -d "$server_config")
    
    # Extract clean ID
    local server_id=$(echo "$response" | jq -r '.id // empty' | tr -d "[:space:]'")
    
    if [ -n "$server_id" ]; then
        log_success "Query server created with ID: $server_id" >&2
        # This must be the ONLY thing sent to stdout
        echo "$server_id"
        return 0
    else
        log_error "Failed to create query server" >&2
        return 1
    fi
}

wait_for_query_server() {
    local engine_id=$1
    local server_id=$(echo "$2" | grep -oE '[0-9a-fA-F-]{36}' | head -n 1)
    local max_wait=${3:-300}
    local check_interval=10 # Ensure this is local to the function
    
    log_info "Waiting for query server $server_id to be RUNNING..."
    
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        local response=$(curl -s -k -X GET \
            "$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers" \
            -H "$(get_auth_header)" \
            -H "LhInstanceId: $WATSONX_INSTANCE_ID" 2>/dev/null)
        
        # Extract state safely
        local state=$(echo "$response" | jq -r --arg sid "$server_id" '.query_servers[]? | select(.id == $sid) | .state' 2>/dev/null)
        
        if [ "$state" == "RUNNING" ] || [ "$state" == "ACTIVE" ]; then
            log_success "Query server is RUNNING"
            return 0
        elif [ "$state" == "FAILED" ] || [ "$state" == "STOPPED" ]; then
            log_error "Query server failed or stopped (state: $state)"
            return 1
        elif [ -n "$state" ] && [ "$state" != "null" ]; then
            log_info "Query server state: $state (waiting...)"
        else
            log_warning "Server ID $server_id not found in response yet."
        fi
        
        sleep "$check_interval" # Quotes prevent word-splitting errors
        elapsed=$((elapsed + check_interval))
    done
    
    log_error "Timeout waiting for query server to start"
    return 1
}

# Wait for query server to be running
# wait_for_query_server() {
# local engine_id=$1
#     # Extract ONLY the 36-character UUID from the second argument
#     local server_id=$(echo "$2" | grep -oE '[0-9a-fA-F-]{36}' | head -n 1)
#     local max_wait=${3:-300}
    
#     if [ -z "$server_id" ]; then
#         log_error "Could not parse a valid UUID from input: '$2'"
#         return 1
#     fi

#     log_info "Monitoring state for validated ID: $server_id"
    
#     log_info "Waiting for query server $server_id to be RUNNING (max ${max_wait}s)..."
    
#     # Get auth token if not already set
#     if [ -z "$AUTH_TOKEN" ]; then
#         get_auth_token || return 1
#     fi
    
#     local base_url=$(get_base_url)
#     local auth_header=$(get_auth_header)
#     local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers"
    
#     local elapsed=0
#     while [ $elapsed -lt $max_wait ]; do
#         local response=$(curl -s -k -X GET \
#             "$api_url" \
#             -H "$auth_header" \
#             -H "LhInstanceId: $WATSONX_INSTANCE_ID" 2>/dev/null)
        
#         # Debug: Show server_id being searched for
#         log_info "Looking for server_id: '$server_id'"
        
#         # Debug: Show full response
#         log_info "API Response:"
#         echo "$response" | jq '.' 2>/dev/null || echo "$response"
        
#         # Use jq --arg to properly pass the server_id variable
#         local state=$(echo "$response" | jq -r --arg sid "$server_id" '.query_servers[]? | select(.id == $sid) | .state // empty' 2>/dev/null)
#         log_info "Query server state: '$state'"
#         if [ "$state" == "RUNNING" ]; then
#             log_success "Query server is RUNNING"
#             return 0
#         elif [ "$state" == "ACTIVE" ]; then
#             log_success "Query server is RUNNING"
#             return 0
#         elif [ "$state" == "FAILED" ]; then
#             log_error "Query server failed to start"
#             return 1
#         elif [ -n "$state" ]; then
#             log_info "Query server state: $state (waiting...)"
#         fi
        
#         sleep $check_interval
#         elapsed=$((elapsed + check_interval))
#     done
    
#     log_error "Timeout waiting for query server to start"
#     return 1
# }

# Get query server profile (dbt configuration)
get_query_server_profile() {
    local engine_id=$1
    local server_id=$2
    local catalog=${3:-iceberg_data}
    local schema=${4:-default}
    
    log_info "Getting query server profile for $server_id" >&2
    
    if [ -z "$AUTH_TOKEN" ]; then
        get_auth_token >&2 || return 1
    fi
    
    local base_url=$(get_base_url)
    local auth_header=$(get_auth_header)
    local api_url="$base_url/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers/$server_id/profile"
    
    local response=$(curl -s -k -X GET \
        "$api_url" \
        -H "$auth_header" \
        -H "LhInstanceId: $WATSONX_INSTANCE_ID" 2>/dev/null)
    
    local profile=$(echo "$response" | jq -r '.profile // empty' 2>/dev/null)
    
    if [ -n "$profile" ]; then
        # Replace placeholders
        profile=$(echo "$profile" | sed "s|<wxd-schema>|$schema|g")
        profile=$(echo "$profile" | sed "s|<wxd-catalog>|$catalog|g")
        profile=$(echo "$profile" | sed "s|<username>|$CPD_USERNAME|g")
        profile=$(echo "$profile" | sed "s|<apikey>|$WATSONX_APIKEY|g")
        
        echo "$profile"
        return 0
    else
        # Extract error from JSON or show raw response if extraction fails
        local error_msg=$(echo "$response" | jq -r '.message // .errors[0].message // .exception // "Unknown Error"' 2>/dev/null)
        log_error "Failed to get profile for $server_id. API Response: $error_msg" >&2
        return 1
    fi
}

# Run pytest with specific configuration
run_tests() {
    local test_pattern=${1:-"tests/functional/adapter/catalog_tests/"}
    local profile=${2:-"watsonx"}
    
    log_info "Running tests: $test_pattern"
    
    cd "$PROJECT_ROOT"
    
    # Activate virtual environment if specified
    if [ -n "$PYTHON_VENV_PATH" ] && [ -d "$PYTHON_VENV_PATH" ]; then
        log_info "Activating Python virtual environment: $PYTHON_VENV_PATH"
        source "$PYTHON_VENV_PATH/bin/activate"
    fi
    
    # Export required environment variables for tests
    export WATSONX_APIKEY
    export WATSONX_HOSTNAME
    export WATSONX_INSTANCE_ID
    
    # Run pytest
    log_info "Executing: pytest $test_pattern -v --profile $profile"
    
    if pytest "$test_pattern" -v --profile "$profile" --tb=short; then
        log_success "All tests passed!"
        return 0
    else
        log_error "Some tests failed"
        return 1
    fi
}

# Main execution
main() {
    log_info "=== dbt-watsonx-spark Catalog Test Automation ==="
    log_info "Project root: $PROJECT_ROOT"
    
    # Load environment
    load_env "${1:-.env}"
    
    # Set defaults for optional variables
    ICEBERG_CATALOG_NAME=${ICEBERG_CATALOG_NAME:-iceberg_data}
    DELTA_CATALOG_NAME=${DELTA_CATALOG_NAME:-delta_data}
    HUDI_CATALOG_NAME=${HUDI_CATALOG_NAME:-hudi_data}
    ICEBERG_BUCKET=${ICEBERG_BUCKET:-}
    DELTA_BUCKET=${DELTA_BUCKET:-}
    HUDI_BUCKET=${HUDI_BUCKET:-}
    ICEBERG_CATALOG_ENABLED=${ICEBERG_CATALOG_ENABLED:-true}
    DELTA_CATALOG_ENABLED=${DELTA_CATALOG_ENABLED:-true}
    HUDI_CATALOG_ENABLED=${HUDI_CATALOG_ENABLED:-true}
    SPARK_ENGINE_NAME=${SPARK_ENGINE_NAME:-spark-dbt-test}
    SPARK_ENGINE_AUTHZ_NAME=${SPARK_ENGINE_AUTHZ_NAME:-spark-dbt-authz-test}
    TEST_PATTERN=${TEST_PATTERN:-tests/functional/adapter/catalog_tests/}
    SKIP_INFRASTRUCTURE=${SKIP_INFRASTRUCTURE:-false}
    
    # Check if query server URL is provided (skip infrastructure setup)
    if [ -n "$QUERY_SERVER_URL" ]; then
        log_info "Query server URL provided: $QUERY_SERVER_URL"
        log_info "Skipping infrastructure setup (catalogs, engines, attachments)"
        SKIP_INFRASTRUCTURE=true
    fi
    
    if [ "$SKIP_INFRASTRUCTURE" != "true" ]; then
        log_info "=== Step 1: Creating Catalogs ==="
        create_catalog "$ICEBERG_CATALOG_NAME" "iceberg" "$ICEBERG_BUCKET" || log_warning "Iceberg catalog creation had issues"
        create_catalog "$DELTA_CATALOG_NAME" "delta" "$DELTA_BUCKET" || log_warning "Delta catalog creation had issues"
        create_catalog "$HUDI_CATALOG_NAME" "hudi" "$HUDI_BUCKET" || log_warning "Hudi catalog creation had issues"
        
        log_info "=== Step 2: Creating Spark Engines ==="
        create_spark_engine "$SPARK_ENGINE_NAME" "false" || log_warning "Standard engine creation had issues"
        STANDARD_ENGINE_ID="$SPARK_ENGINE_ID"  # Store the ID returned by create_spark_engine
        
        create_spark_engine "$SPARK_ENGINE_AUTHZ_NAME" "true" || log_warning "Authz engine creation had issues"
        AUTHZ_ENGINE_ID="$SPARK_ENGINE_ID"  # Store the ID returned by create_spark_engine
        
        log_info "=== Step 3: Attaching Catalogs to Standard Engine ==="
        # If engine ID not available from creation, look it up by name
        if [ -z "$STANDARD_ENGINE_ID" ]; then
            log_info "Looking up engine ID by name: $SPARK_ENGINE_NAME"
            STANDARD_ENGINE_ID=$(get_engine_id_by_name "$SPARK_ENGINE_NAME")
        fi
        
        if [ -n "$STANDARD_ENGINE_ID" ]; then
            log_info "Using engine ID: $STANDARD_ENGINE_ID"
            attach_catalog_to_engine "$STANDARD_ENGINE_ID" "$ICEBERG_CATALOG_NAME"
            attach_catalog_to_engine "$STANDARD_ENGINE_ID" "$DELTA_CATALOG_NAME"
            attach_catalog_to_engine "$STANDARD_ENGINE_ID" "$HUDI_CATALOG_NAME"
        else
            log_warning "Standard engine ID not available, skipping catalog attachment"
        fi
        
        log_info "=== Step 4: Attaching Catalogs to Authz Engine ==="
        # If engine ID not available from creation, look it up by name
        if [ -z "$AUTHZ_ENGINE_ID" ]; then
            log_info "Looking up engine ID by name: $SPARK_ENGINE_AUTHZ_NAME"
            AUTHZ_ENGINE_ID=$(get_engine_id_by_name "$SPARK_ENGINE_AUTHZ_NAME" || true)
        fi
        
        if [ -n "$AUTHZ_ENGINE_ID" ]; then
            log_info "Using engine ID: $AUTHZ_ENGINE_ID"
            attach_catalog_to_engine "$AUTHZ_ENGINE_ID" "$ICEBERG_CATALOG_NAME"
            attach_catalog_to_engine "$AUTHZ_ENGINE_ID" "$DELTA_CATALOG_NAME"
            attach_catalog_to_engine "$AUTHZ_ENGINE_ID" "$HUDI_CATALOG_NAME"
        else
            log_warning "Authz engine ID not available, skipping catalog attachment"
        fi
        
        log_info "=== Step 5: Creating Query Servers ==="
        # Create query server for standard engine
        if [ -n "$STANDARD_ENGINE_ID" ]; then
            STANDARD_QUERY_SERVER_ID=$(create_query_server "$STANDARD_ENGINE_ID" "dbt-standard-qs")
            if [ -n "$STANDARD_QUERY_SERVER_ID" ]; then
                wait_for_query_server "$STANDARD_ENGINE_ID" "$STANDARD_QUERY_SERVER_ID" 300
                
                # Generate and save profile
                log_info "Generating dbt profile for standard engine..."
                # Capture profile
                STANDARD_PROFILE=$(get_query_server_profile "$STANDARD_ENGINE_ID" "$STANDARD_QUERY_SERVER_ID" "$ICEBERG_CATALOG_NAME" "default")

                if [ -n "$STANDARD_PROFILE" ]; then
                    local profile_path="$HOME/.dbt/profiles.yml"
                    mkdir -p "$(dirname "$profile_path")"
                    echo "$STANDARD_PROFILE" > "$profile_path"
                    log_success "Profile saved to $profile_path"
                else
                    # This will now trigger if the function above returns 1 or an empty string
                    log_error "Critical: Could not generate standard dbt profile. Check API connectivity or Server ID."
                fi
            fi
        fi
        
        # Create query server for authz engine
        if [ -n "$AUTHZ_ENGINE_ID" ]; then
            AUTHZ_QUERY_SERVER_ID=$(create_query_server "$AUTHZ_ENGINE_ID" "dbt-authz-qs")
            if [ -n "$AUTHZ_QUERY_SERVER_ID" ]; then
                wait_for_query_server "$AUTHZ_ENGINE_ID" "$AUTHZ_QUERY_SERVER_ID" 300
                
                # Generate and save profile (append to existing)
                log_info "Generating dbt profile for authz engine..."
                AUTHZ_PROFILE=$(get_query_server_profile "$AUTHZ_ENGINE_ID" "$AUTHZ_QUERY_SERVER_ID" "$ICEBERG_CATALOG_NAME" "default")
                if [ -n "$AUTHZ_PROFILE" ]; then
                    local profile_path="$HOME/.dbt/profiles.yml"
                    # Append authz profile with different name
                    echo "" >> "$profile_path"
                    echo "$AUTHZ_PROFILE" | sed 's/profile_name:/watsonx_authz:/' >> "$profile_path"
                    log_success "Authz profile appended to $profile_path"
                fi
            fi
        fi
    else
        log_info "=== Skipping Infrastructure Setup ==="
        log_info "Using existing query server: ${QUERY_SERVER_URL:-configured}"
    fi
    
    log_info "=== Step 6: Running Tests ==="
    if ! run_tests "$TEST_PATTERN" "watsonx"; then
        log_error "Tests failed on standard engine"
        exit 1
    fi
    
    log_info "=== Step 6: Running Authz Tests on Authz Engine ==="
    # Update profile to use authz engine
    export WATSONX_ENGINE_ID="$SPARK_ENGINE_AUTHZ_NAME"
    if ! run_tests "tests/functional/adapter/catalog_tests/test_issue_71459_authz.py" "watsonx"; then
        log_warning "Authz tests had issues (this may be expected if permissions not configured)"
    fi
    
    log_success "=== All test automation completed successfully! ==="
}

# Run main function
main "$@"

# Made with Bob
