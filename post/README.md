PostSignal
=======

Opens up a url endpoint to which you can POST json objects. These will then be output as signals.

Properties
--------------

-   **host**: Local IP. Defaults to 127.0.0.1.
-   **port**: Port to accept signals on. Defaults to 8182.
-   **endpoint**: Endpoint at which to accept signals.

Dependencies
----------------

-   Web Module

Commands
----------------
None

Input
-------
None

Output
---------
Creates a new signal for each successful POST to this block.
