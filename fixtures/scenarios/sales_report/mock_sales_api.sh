#!/bin/sh
# Mock MCP server: exposes query_sales_data + query_budget_variance tools
# Returns Q4 sales actuals and budget comparison data
# Supports both NDJSON (one JSON object per line) and Content-Length framing.

send_response() {
    local body="$1"
    printf "%s\n" "$body"
}

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

    method=$(printf '%s' "$body" | sed -n 's/.*"method" *: *"\([^"]*\)".*/\1/p')
    id=$(printf '%s' "$body" | sed -n 's/.*"id" *: *\([0-9]*\).*/\1/p')

    case "$method" in
        initialize)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"serverInfo\":{\"name\":\"sales-api\",\"version\":\"1.0\"}}}"
            ;;
        notifications/initialized)
            ;;
        tools/list)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"tools\":[{\"name\":\"query_sales_data\",\"description\":\"Query Q4 actual sales data by region\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"quarter\":{\"type\":\"string\",\"enum\":[\"Q1\",\"Q2\",\"Q3\",\"Q4\"]}},\"required\":[\"quarter\"]}},{\"name\":\"query_budget_variance\",\"description\":\"Compare actual vs budget and compute variance for each region\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"quarter\":{\"type\":\"string\"}},\"required\":[\"quarter\"]}}]}}"
            ;;
        tools/call)
            tool_name=$(printf '%s' "$body" | sed -n 's/.*"name" *: *"\([^"]*\)".*/\1/p' | tail -1)
            case "$tool_name" in
                query_sales_data)
                    send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"{\\\"quarter\\\":\\\"Q4\\\",\\\"data\\\":[{\\\"region\\\":\\\"North America\\\",\\\"actual\\\":1380000,\\\"deals_closed\\\":47},{\\\"region\\\":\\\"Europe\\\",\\\"actual\\\":790000,\\\"deals_closed\\\":31},{\\\"region\\\":\\\"Asia Pacific\\\",\\\"actual\\\":1050000,\\\"deals_closed\\\":38},{\\\"region\\\":\\\"Latin America\\\",\\\"actual\\\":385000,\\\"deals_closed\\\":15}]}\"}]}}"
                    ;;
                query_budget_variance)
                    send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"{\\\"quarter\\\":\\\"Q4\\\",\\\"variance\\\":[{\\\"region\\\":\\\"North America\\\",\\\"target\\\":1200000,\\\"actual\\\":1380000,\\\"variance_pct\\\":15.0,\\\"status\\\":\\\"Exceeded\\\"},{\\\"region\\\":\\\"Europe\\\",\\\"target\\\":850000,\\\"actual\\\":790000,\\\"variance_pct\\\":-7.1,\\\"status\\\":\\\"Below Target\\\"},{\\\"region\\\":\\\"Asia Pacific\\\",\\\"target\\\":960000,\\\"actual\\\":1050000,\\\"variance_pct\\\":9.4,\\\"status\\\":\\\"Exceeded\\\"},{\\\"region\\\":\\\"Latin America\\\",\\\"target\\\":420000,\\\"actual\\\":385000,\\\"variance_pct\\\":-8.3,\\\"status\\\":\\\"Below Target\\\"}]}\"}]}}"
                    ;;
                *)
                    send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"error\":{\"code\":-32601,\"message\":\"unknown tool\"}}"
                    ;;
            esac
            ;;
        *)
            send_response "{\"jsonrpc\":\"2.0\",\"id\":$id,\"error\":{\"code\":-32601,\"message\":\"unknown method\"}}"
            ;;
    esac
done
