#!/bin/bash

# Wahaku Deployment Script
# Run this script on the server as root or with sudo

set -e

echo "=== Wahaku Multi-Tenancy Deployment ==="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
   echo "WARNING: This script should be run as root for proper permissions"
   read -p "Continue anyway? (y/n): " -n 1 -r
   echo
   if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      exit 1
   fi
fi

# Configuration
APP_DIR="/www/wwwroot/chat.yaakhi.id"
SERVICE_FILE="wahaku.service"
ENV_EXAMPLE="wahaku.env.example"
ENV_FILE="/etc/systemd/system/wahaku.env"

# Verify app directory exists
if [ ! -d "$APP_DIR" ]; then
    echo "ERROR: App directory not found: $APP_DIR"
    exit 1
fi

# Copy service file
echo "1. Installing systemd service..."
cp "$APP_DIR/$SERVICE_FILE" /etc/systemd/system/
systemctl daemon-reload

# Setup environment file
echo ""
echo "2. Setting up environment configuration..."
if [ ! -f "$ENV_FILE" ]; then
    cp "$APP_DIR/$ENV_EXAMPLE" "$ENV_FILE"
    echo "   Created $ENV_FILE"
    echo ""
    echo "!!! IMPORTANT: Edit $ENV_FILE and set your API keys and passwords !!!"
    echo ""
    read -p "Press Enter after editing the file..."
else
    echo "   Environment file already exists at $ENV_FILE"
fi

# Set permissions
echo ""
echo "3. Setting permissions..."
chown -R www-data:www-data "$APP_DIR"
chmod +x "$APP_DIR/wahaku"
chmod 600 "$ENV_FILE"

# Enable and start service
echo ""
echo "4. Enabling and starting service..."
systemctl enable wahaku
systemctl restart wahaku
sleep 2

# Check status
echo ""
echo "5. Service status:"
systemctl status wahaku --no-pager -l

# Show logs
echo ""
echo "6. Recent logs:"
journalctl -u wahaku -n 20 --no-pager

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Next steps:"
echo "1. Verify the app is running: systemctl status wahaku"
echo "2. Check logs: journalctl -u wahaku -f"
echo "3. Access dashboard at: http://localhost:4500/dashboard"
echo "4. Default admin credentials from config.json"
echo ""
echo "To stop: systemctl stop wahaku"
echo "To restart: systemctl restart wahaku"
