
export function makeRoute(path) {
    if (process.env.NODE_ENV == 'production') {
        return `/${path}`;
    } else {
        return `http://localhost:8000/${path}`;
    }
}
