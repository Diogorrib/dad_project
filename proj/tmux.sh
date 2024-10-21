#!/bin/bash

# Start a new tmux session named 'server_client_session'
tmux new-session -d -s server_client_session

# Create the first server pane (iteration 0, port 8080)
tmux send-keys "cd server/ && mvn exec:java -Dexec.args='8080 0'" C-m

# Split the first window vertically and go to iteration 1, port 8080
tmux split-window -v
tmux send-keys "cd server/ && mvn exec:java -Dexec.args='8080 1'" C-m

# Split again and run iteration 2, port 8080
tmux split-window -v
tmux send-keys "cd server/ && mvn exec:java -Dexec.args='8080 2'" C-m

# Split again and run iteration 3, port 8080
tmux split-window -v
tmux send-keys "cd server/ && mvn exec:java -Dexec.args='8080 3'" C-m

# Split again and run iteration 4, port 8080
tmux split-window -v
tmux send-keys "cd server/ && mvn exec:java -Dexec.args='8080 4'" C-m

# Select layout to arrange panes nicely
tmux select-layout tiled

# Create a new window for the client and execute the command
tmux new-window -t server_client_session
tmux send-keys "cd client/ && mvn exec:java" C-m

tmux split-window -v
tmux send-keys "cd client/ && mvn exec:java -Dexec.args='2 -i'" C-m

tmux select-layout tiled

# Create another window for the console client and execute the command
tmux new-window -t server_client_session
tmux send-keys "cd consoleclient/ && mvn exec:java" C-m

# Attach to the session
tmux attach -t server_client_session

