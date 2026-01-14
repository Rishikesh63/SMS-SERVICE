@echo off
REM SMS Server Start Script for Windows

echo Starting SMS Server...
echo.

REM Check if .env file exists
if not exist .env (
    echo Error: .env file not found
    echo Please copy .env.example to .env and configure your credentials:
    echo   copy .env.example .env
    echo.
    echo Required configuration:
    echo   - TURSO_DATABASE_URL and TURSO_AUTH_TOKEN
    echo   - SIGNALWIRE_PROJECT_ID, SIGNALWIRE_AUTH_TOKEN, SIGNALWIRE_SPACE_URL, SIGNALWIRE_FROM_NUMBER
    echo.
    echo See SMS_SETUP.md for detailed setup instructions.
    exit /b 1
)

REM Check if Rust is installed
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: Rust is not installed
    echo Install Rust from: https://rustup.rs/
    exit /b 1
)

REM Build and run
echo Building SMS server...
cargo build --bin sms-server --release

if %errorlevel% equ 0 (
    echo.
    echo Build successful!
    echo Starting server...
    echo.
    cargo run --bin sms-server --release
) else (
    echo Build failed
    exit /b 1
)
