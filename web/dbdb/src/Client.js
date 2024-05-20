
import { makeRoute } from './routes.js';

function postRequest(path, body, callback) {
    fetch(
        makeRoute(path),
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
    .catch(error => callback(error));
}



export { postRequest };
