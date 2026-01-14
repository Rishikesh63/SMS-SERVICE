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
