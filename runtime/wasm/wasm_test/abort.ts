import "allocator/arena";

export { allocate_memory };

declare namespace env {
    function abort(
        message?: string | null,
        fileName?: string | null,
        lineNumber?: u32,
        columnNumber?: u32
        ): void;
}

export function abort(): void {
    env.abort("aborted", "abort.ts", 1, 1);
}
