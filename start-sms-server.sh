#!/bin/bash

# SMS Server Start Script

echo "Starting SMS Server..."
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found"
    echo "Please copy .env.example to .env and configure your credentials:"
    echo "  cp .env.example .env"
    echo ""
    echo "Required configuration:"
    echo "  - TURSO_DATABASE_URL and TURSO_AUTH_TOKEN"
    echo "  - SIGNALWIRE_PROJECT_ID, SIGNALWIRE_AUTH_TOKEN, SIGNALWIRE_SPACE_URL, SIGNALWIRE_FROM_NUMBER"
    echo ""
    echo "See SMS_SETUP.md for detailed setup instructions."
    exit 1
fi

# Load .env file
echo "Loading environment variables from .env file..."
set -a  # Automatically export all variables
source .env
set +a  # Stop automatically exporting

# Verify Iggy variables are set
echo "Checking Iggy configuration..."
if [ -z "$IGGY_USERNAME" ] || [ -z "$IGGY_PASSWORD" ]; then
    echo "Warning: IGGY_USERNAME or IGGY_PASSWORD not set in .env"
    echo "Using defaults (username: iggy, password: iggy)"
    export IGGY_USERNAME=${IGGY_USERNAME:-iggy}
    export IGGY_PASSWORD=${IGGY_PASSWORD:-iggy}
fi

echo "Iggy Server: $IGGY_SERVER_ADDRESS"
echo "Iggy Username: $IGGY_USERNAME"
echo ""

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: Rust is not installed"
    echo "Install Rust from: https://rustup.rs/"
    exit 1
fi

# Build and run
echo "Building SMS server..."
cargo build --bin sms-server --release

if [ $? -eq 0 ]; then
    echo ""
    echo "Build successful!"
    echo "Starting server..."
    echo ""
    cargo run --bin sms-server --release
else
    echo "Build failed"
    exit 1
fi