
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
    .then(response => {
        // big hack! sorry!
        const machineId = response.headers.get('X-FLY-MACHINE-ID');
        window.FLY_MACHINE_ID = machineId;
        return response.json()
    })
    .then(json => callback(json))
    .catch(error => callback(error));
}



export { postRequest };
