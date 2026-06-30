#!/usr/bin/env bash

# Exit immediately if any command fails
set -e

echo "=================================================="
# Check if tmux is installed
if ! command -v tmux &> /dev/null; then
    echo "❌ Error: 'tmux' is not installed."
    echo "To install it, run:"
    echo "   sudo apt install tmux  (on Debian/Ubuntu)"
    echo "   brew install tmux      (on macOS)"
    exit 1
fi

echo "🐳 Starting local Kafka broker..."
docker compose up -d

echo "⚙️ Compiling Java project..."
mvn clean compile

# Define session name
SESSION="collatz-demo"

# Kill any existing session with this name
tmux kill-session -t "$SESSION" 2>/dev/null || true

echo "🚀 Launching split-pane tmux session: $SESSION..."
echo "Press Ctrl+B then D to detach, or Ctrl+C in any pane to stop that process."
sleep 2

# Create new detached session with Aggregator in Pane 0
tmux new-session -d -s "$SESSION" -n "Collatz" 'mvn exec:java -Dexec.mainClass="com.collatz.Aggregator"'

# Split horizontally (Creates Pane 1 on the right)
tmux split-window -h -t "$SESSION" 'mvn exec:java -Dexec.mainClass="com.collatz.Worker"'

# Select left pane (Pane 0) and split vertically (Creates Pane 2 bottom-left)
tmux select-pane -t "$SESSION:Collatz.0"
tmux split-window -v -t "$SESSION" 'sleep 2 && mvn exec:java -Dexec.mainClass="com.collatz.Worker"'

# Select right pane (Pane 1, now Pane 3 in the index) and split vertically (Creates Pane 4 bottom-right)
tmux select-pane -t "$SESSION:Collatz.1"
tmux split-window -v -t "$SESSION" 'sleep 5 && mvn exec:java -Dexec.mainClass="com.collatz.Coordinator"'

# Enable mouse mode so you can resize/click panes easily
tmux set-option -t "$SESSION" mouse on

# Attach to the session
tmux attach-session -t "$SESSION"
