import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';

import { PostHogProvider} from 'posthog-js/react'

document.title = 'dbdb';

const posthogKey = 'phc_PiRdvWyeWLYovNfwEh4mhDgzzNu0VBVgcVTl6ux9Ti0';
const posthogOptions = {
  api_host: "https://us.i.posthog.com",
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <PostHogProvider
      apiKey={posthogKey}
      options={posthogOptions}
    >
      <App />
    </PostHogProvider>
  </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();



