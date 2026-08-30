function createMessage() {
    return {
        event: "message",
        data: [],
        id: undefined,
        retry: undefined,
    };
}

/** A small incremental parser for the WHATWG server-sent event wire format. */
export function createSseParser(onEvent) {
    if (typeof onEvent !== "function") {
        throw new TypeError("onEvent must be a function");
    }

    let buffer = "";
    let message = createMessage();

    const dispatch = () => {
        if (message.data.length === 0) {
            message = createMessage();
            return;
        }
        onEvent({
            type: message.event || "message",
            data: message.data.join("\n"),
            id: message.id,
            retry: message.retry,
        });
        message = createMessage();
    };

    const processLine = (line) => {
        if (line === "") {
            dispatch();
            return;
        }
        if (line.startsWith(":")) {
            return;
        }

        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        let value = colon < 0 ? "" : line.slice(colon + 1);
        if (value.startsWith(" ")) {
            value = value.slice(1);
        }

        switch (field) {
            case "event":
                message.event = value;
                break;
            case "data":
                message.data.push(value);
                break;
            case "id":
                if (!value.includes("\0")) {
                    message.id = value;
                }
                break;
            case "retry":
                if (/^\d+$/.test(value)) {
                    message.retry = Number(value);
                }
                break;
            default:
                break;
        }
    };

    const drain = (final = false) => {
        while (buffer.length > 0) {
            let lineEnd = -1;
            for (let index = 0; index < buffer.length; index += 1) {
                if (buffer[index] === "\n" || buffer[index] === "\r") {
                    lineEnd = index;
                    break;
                }
            }

            if (lineEnd < 0) {
                if (final) {
                    processLine(buffer);
                    buffer = "";
                }
                return;
            }

            if (buffer[lineEnd] === "\r" && lineEnd === buffer.length - 1 && !final) {
                return;
            }

            const separatorLength = buffer[lineEnd] === "\r" && buffer[lineEnd + 1] === "\n" ? 2 : 1;
            const line = buffer.slice(0, lineEnd);
            buffer = buffer.slice(lineEnd + separatorLength);
            processLine(line);
        }
    };

    return {
        push(chunk) {
            buffer += chunk;
            drain(false);
        },
        finish() {
            drain(true);
            dispatch();
        },
    };
}

export async function consumeSseStream(stream, { onEvent, signal } = {}) {
    if (!stream?.getReader) {
        throw new TypeError("The response does not expose a readable byte stream");
    }

    const parser = createSseParser(onEvent);
    const decoder = new TextDecoder();
    const reader = stream.getReader();

    const abort = () => reader.cancel(signal?.reason).catch(() => {});
    signal?.addEventListener("abort", abort, { once: true });
    try {
        while (true) {
            if (signal?.aborted) {
                throw signal.reason ?? new DOMException("Aborted", "AbortError");
            }
            const { done, value } = await reader.read();
            if (done) {
                parser.push(decoder.decode());
                parser.finish();
                return;
            }
            parser.push(decoder.decode(value, { stream: true }));
        }
    } finally {
        signal?.removeEventListener("abort", abort);
        reader.releaseLock();
    }
}
