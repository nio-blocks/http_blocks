RESTPolling
===========

Used as a base class for any block that needs to do polling of a URL. *Not a stand alone block.*

Properties
--------------

-   **polling_interval**: How often url is polled. A new request will be made for every member of `queries` at the `polling_interval`.
-   **retry_interval**: When a request fails, wait this many seconds before attempting a retry.
-   **retry_limit**: The maximum number of retries to attempt for each request.
-   **queries**: List of endpoints to poll.
-   **include_query**: If not `None`, the endpoint for this request, from `queries`, will be stored in this attribute of the outgoing signal.


Dependencies
----------------

-   [requests](https://pypi.python.org/pypi/requests/)

Commands
----------------
None
