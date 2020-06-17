To run server:

1. Create a virtual environment:
`python3 -mvenv env`

2. Install dependencies:
`pip3 install -r requirements.txt`

3. Set the REQUEST_URL env variable in your terminal:
`export REQUEST_URL="https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures"`

4. Run script to test with given test cases (should print 3 response objects with status code 201):
`python3 submit_performance_data.py`

5. To run with other test files of the same format, add them to the 'data' and 'schemas' folders and run script again (should print a response object for each row in each data file): 
`python3 submit_performance_data.py`
