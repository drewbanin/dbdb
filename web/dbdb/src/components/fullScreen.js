
function FullScreenIcon({ isFullscreen }) {
    return (
        <div style={{ width: 12, height: 12, paddingBottom: 4}}>
            <svg height="100%" version="1.1" viewBox="10 10 16 16" width="100%">
                <path d="m 10,16 2,0 0,-4 4,0 0,-2 L 10,10 l 0,6 0,0 z"></path>
                <path d="m 20,10 0,2 4,0 0,4 2,0 L 26,10 l -6,0 0,0 z"></path>
                <path d="m 24,24 -4,0 0,2 L 26,26 l 0,-6 -2,0 0,4 0,0 z"></path>
                <path d="M 12,20 10,20 10,26 l 6,0 0,-2 -4,0 0,-4 0,0 z"></path>
            </svg>
        </div>
    )
}

export default FullScreenIcon;
