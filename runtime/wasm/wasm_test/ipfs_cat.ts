import "allocator/arena";

export { memory };

declare namespace typeConversion {
    function bytesToString(bytes: Uint8Array): string
}

declare namespace ipfs {
    function cat(hash: String): Uint8Array
}

export function ipfsCat(hash: string): string {
    return typeConversion.bytesToString(ipfs.cat(hash))
}
