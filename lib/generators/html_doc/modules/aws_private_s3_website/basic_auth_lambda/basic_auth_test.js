const assert = require('assert');
const basic_auth = require('./basic_auth.js');

// Not using a 3rd-party framework like Mocha because this is an
//  embedded, standalone Lambda function in an otherwise non-JS project.

function test (test_msg, f) {
    console.info(test_msg);
    try {
        f();
        console.info("... success");
    } catch (error) {
        console.error("... failure");
        console.error(error);
    }
}

const viewer_request = require('./test_viewer_request.json');
const http_401 = {
    'status': 401,
    'statusDescription': 'Unauthorized',
    'body': 'Unauthorized',
    'headers': {
        'www-authenticate': [
            { 'key': 'WWW-Authenticate', 'value': 'Basic' }
        ]
    }
}


test("Unauthorized requests should generate an HTTP 401 response", () => {
    basic_auth.handler(viewer_request, null, (err, ret) => {
        assert.deepEqual(ret, http_401);
    });
})

// Authorized requests should return the request unaltered
const authorized_viewer_request = require('./test_authorized_viewer_request.json')
const passed_request = authorized_viewer_request.Records[0].cf.request;

test("Authorized requests should return the wrapped request unaltered", () => {
    basic_auth.handler(authorized_viewer_request, null, (err, ret) => {
        assert.deepEqual(ret, passed_request);
    });
});