
export function makeRoute(path) {
    if (window.location.host.startsWith('localhost')) {
        return `http://localhost:8000/${path}`;
    } else {
        return `/${path}`;
    }
}
