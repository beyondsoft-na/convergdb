'use strict';

// From https://hackernoon.com/serverless-password-protecting-a-static-website-in-an-aws-s3-bucket-bfaaa01b8666

exports.handler = (event, context, callback) => {

    // Get request and request headers
    const request = event.Records[0].cf.request;
    const headers = request.headers;

    // Configure authentication
    const authUser = "user";
    const authPass = "pass";

    // Construct the Basic Auth string
    const authString = 'Basic ' + new Buffer(authUser + ':' + authPass).toString('base64');

    // Require Basic authentication
    if (typeof headers.authorization == 'undefined' || headers.authorization[0].value != authString) {
        const response = {
            'status': '401',
            'statusDescription': 'Unauthorized',
            'body': 'Unauthorized',
            'headers': {
                'www-authenticate': [{ 'key': 'WWW-Authenticate', 'value': 'Basic' }]
            }
        };
        callback(null, response);
    } else {
        // Continue request processing if authentication passed
        callback(null, request);
    }
};
