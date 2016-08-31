HTTP_Blocks
=======

Blocks that perform various actions regarding http communication.

-   [RESTPolling](https://github.com/nio-blocks/http_blocks#RESTPolling)

***

RESTPolling
===========

Used as a base class for any block that needs to do polling of a URL. *Not a stand alone block.*

Properties
--------------

-   **queries**: List of queries.
-   **polling_interval**: How often url is polled. When using more than one query. Each query will be polled at a period equal to the `polling_interval` times the number of queries.
-   **retry_interval**: When a url request fails, how long to wait before attempting to try again.
-   **retry_limit**: When a url request fails, number of times to attempt a retry before giving up.


Dependencies
----------------

-   [requests](https://pypi.python.org/pypi/requests/)

Commands
----------------
None

Input
-------
None

Output
---------
Varies depending on implementation of the base class. `_process_response(self, resp)` is to be implemented to return the signals that will be notified.
