export function decodeSerializedCommand(command) {
    if (typeof command !== "string") {
        return null;
    }

    const normalized = command.trim();

    if (!normalized || normalized.includes("CallQueuedClosure")) {
        return null;
    }

    if (normalized.startsWith("{") || normalized.startsWith("[")) {
        try {
            return JSON.parse(normalized);
        } catch (error) {
            return null;
        }
    }

    return null;
}

export function prettyPrintSerializedJobData(data) {
    const decoded = decodeSerializedCommand(data?.command);

    return decoded ?? data;
}
