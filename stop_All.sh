#!/bin/bash
# Stop all Traders-View services and tunnel

# Stop Cloudflare tunnel
pkill -f "cloudflared tunnel"

# Stop main dashboard app
pkill -f "traders-view.py"

# Stop topic services
pkill -f "run_all_services.py"

# Optionally stop individual topic services
pkill -f "crypto_service.py"
pkill -f "financial_service.py"
pkill -f "geopolitical_service.py"
pkill -f "metals_service.py"

echo "All Traders-View services and tunnel stopped."
