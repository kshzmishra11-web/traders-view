#!/bin/bash

# Traders-View EC2 startup script

# Load and export all environment variables from .bashrc
if [ -f .bashrc ]; then
    set -a
    source .bashrc
    set +a
fi

# Activate Python virtual environment if exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Install dependencies
pip3 install -r requirements.txt

# Start all topic services
nohup python3 run_all_services.py > run_all_services.log 2>&1 &

# Start main dashboard app
nohup python3 traders-view.py > app.log 2>&1 &

# Wait for the app to be up before starting cloudflared
echo "Waiting for dashboard app to be available on http://127.0.0.1:8787 ..."
for i in {1..30}; do
    sleep 2
    if curl -s http://127.0.0.1:8787 > /dev/null; then
        echo "Dashboard app is up."
        break
    fi
done

# Start Cloudflare named tunnel (background)
nohup cloudflared tunnel run tradersview-uk-tunnel > cloudflared.log 2>&1 &
sleep 2
if pgrep -f "cloudflared tunnel run tradersview-uk-tunnel" > /dev/null; then
    echo "Cloudflare named tunnel started for https://tradersview.uk"
else
    echo "Failed to start Cloudflare named tunnel. Check cloudflared.log for details."
    exit 1
fi
