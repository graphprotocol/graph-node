import "allocator/arena";

export { memory };

declare namespace dataSource {
    function create(name: string, params: Array<string>): boolean
}

export function dataSourceCreate(name: string, params: Array<string>): boolean {
    return dataSource.create(name, params)
}
