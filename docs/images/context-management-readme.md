#### The following FHIRcast features are not supported by Cast:
##### Context Management
Cast hub does not support context management.  Context is to be retrieved from the relevant applications directly through the request mechanism.   In cast, the hub is a routing appliance only.  It does not look at event data; it has no storage or database;  only distribution logic.   The context management paradigm was tried 30 years ago with CCOW ( <https://en.wikipedia.org/wiki/CCOW> ).  We have to acknowledge that today all advanced radiology integrations function without a context management server. They manage with events obtained through a combination of file drops, postMessages, URL with parameters, EXEs with command-line parameters, localhost web service and socket to to socket.  


##### OAuth 2.0 Authorization Scopes

FHIRcast defines OAuth 2.0 access scopes that correspond directly to FHIRcast events. These scopes associate read or write permissions to an event. Applications that need to receive workflow related events SHOULD ask for read scopes. Applications that request context changes SHOULD ask for write scopes.


This is related to the context management feature.  
It is not supported in the hub OAuth handling but the client can send them.

The authorization scoping project and desktop integration projects do not have to intefere.


Cast is focused on desktop integration and does not require changing how  application implements resource access control.  If the customer has an existing OAuth server that manages user access to event names, the  subscribing applications can send their events to the auth server.  But requiring that this exists before the project start is not in scope of Cast.  
