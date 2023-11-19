
function postRequest(path, body, callback) {
    fetch(
        `http://localhost:8000/${path}`,
        {
           method: 'POST',
           body: JSON.stringify(body),
           headers: {
               "content-type": "application/json"
            }
        }
    )
    .then(response => response.json())
    .then(json => callback(json))
    .catch(error => console.error(error));
}



export { postRequest };
