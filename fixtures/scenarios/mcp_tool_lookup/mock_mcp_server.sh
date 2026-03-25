#!/bin/sh
# Minimal MCP server mock that responds to initialize + tools/list + tools/call.
# Speaks JSONRPC 2.0 over stdio with Content-Length framing.

send_response() {
    local body="$1"
    local len=${#body}
    printf "Content-Length: %d\r\n\r\n%s" "$len" "$body"
}

# Read and respond to requests in a loop
request_id=0
while true; do
    # Read Content-Length header
    read -r header_line
    case "$header_line" in
        Content-Length:*) ;;
        *) continue ;;
    esac
    content_length=$(echo "$header_line" | sed 's/Content-Length: *//;s/\r//')

    # Read empty line
    read -r _blank

    # Read body
    body=$(dd bs=1 count="$content_length" 2>/dev/null)

    # Extract method
    method=$(echo "$body" | sed -n 's/.*"method" *: *"\([^"]*\)".*/\1/p')
    id=$(echo "$body" | sed -n 's/.*"id" *: *\([0-9]*\).*/\1/p')

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
            # Extract the name argument from the call
            person_name=$(echo "$body" | sed -n 's/.*"name" *: *"\([^"]*\)".*/\1/p' | tail -1)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"Hello, ${person_name:-World}!\"}]}}"
            ;;
        *)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"error\":{\"code\":-32601,\"message\":\"unknown method\"}}"
            ;;
    esac
done
