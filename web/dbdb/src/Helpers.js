
const formatBytes = (bytes, decimals = 2) => {
    if (!+bytes) return '0 Bytes'

    const k = 1024
    const dm = decimals < 0 ? 0 : decimals
    const sizes = ['bytes', 'kb', 'mb', 'gb', 'tb']

    const i = Math.floor(Math.log(bytes) / Math.log(k))

    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}

const formatNumber = (number, decimals = 2) => {
    if (!number) {
        return null;
    }

    return number.toLocaleString(
      undefined, // leave undefined to use the visitor's browser 
                 // locale or a string like 'en-US' to override it.
      { minimumFractionDigits: decimals }
    );
}

export { formatBytes, formatNumber };
