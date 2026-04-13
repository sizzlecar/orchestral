#!/bin/sh
# Minimal MCP server mock that responds to initialize + tools/list + tools/call.
# Supports both NDJSON (one JSON object per line) and Content-Length framing.

send_response() {
    local body="$1"
    printf "%s\n" "$body"
}

# Read and respond to requests in a loop
while IFS= read -r line; do
    # Skip empty lines and Content-Length headers
    case "$line" in
        "") continue ;;
        Content-Length:*) continue ;;
    esac

    # Strip trailing \r if present
    line=$(printf '%s' "$line" | tr -d '\r')

    # Skip non-JSON lines
    case "$line" in
        \{*) ;;
        *) continue ;;
    esac

    body="$line"

    # Extract method and id with sed
    method=$(printf '%s' "$body" | sed -n 's/.*"method" *: *"\([^"]*\)".*/\1/p')
    id=$(printf '%s' "$body" | sed -n 's/.*"id" *: *\([0-9]*\).*/\1/p')

    case "$method" in
        initialize)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"serverInfo\":{\"name\":\"mock\",\"version\":\"1.0\"}}}"
            ;;
        notifications/initialized)
            # No response needed for notifications
            ;;
        tools/list)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"tools\":[{\"name\":\"greet\",\"description\":\"Generate a greeting message for a person\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\",\"description\":\"Person name\"},\"language\":{\"type\":\"string\",\"enum\":[\"en\",\"zh\"],\"description\":\"Greeting language\"}},\"required\":[\"name\"]}}]}}"
            ;;
        tools/call)
            person_name=$(printf '%s' "$body" | sed -n 's/.*"name" *: *"\([^"]*\)".*/\1/p' | tail -1)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"Hello, ${person_name:-World}!\"}]}}"
            ;;
        *)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"error\":{\"code\":-32601,\"message\":\"unknown method\"}}"
            ;;
    esac
done
