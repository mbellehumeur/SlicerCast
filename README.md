# 3D Slicer Cast Interface Extension

<p align="center">
  <img src="docs/images/banner.png" alt="Cast Interface Banner" width="100%">
</p>


---

## Overview

Cast Interface is a 3D Slicer extension focused on desktop integration workflows for healthcare applications.


---

## Extension Features
Resource servers: Resource servers subscribe to all user topics for dicom/nifti events and send back results to the user throuh the hub. Each server has its own onMessage script. The script handles producing the results from the DICOM files received and publishes a dicom-send event back to the user topic.

Image Display Client: The image display client provide a PACS client type interface to the 3D slicer viewer. Supported events should be ImagingStudy-open, Imaging-Study-close, dicom-send and request for sceneview.

Hub: The hub is the server that distributes the messages and handles the data transfer requests over the websocket connection to each client.

---
## Background

Cast is an offshoot of FHIRcast (<https://fhircast.hl7.org/>). FHIRcast is the standard replacing Epic’s file drop interface for integration with PACS and reporting systems. It provides a secure messaging infrastructure using a hub with websocket subscriptions.

Cast differs to FHIRcast in it's goals and features but you can use the python client in this extension to interface to FHIRcast hubs, for example, NodeOnFHIR, Medplum and Agfa (at connectathon).

### Goal 

Cast is focused on desktop integration of healthcare applications. It is not restricted to a specific data format.  Cast is also not restricted to a specific authentication and authorization mechanism; it expects that apps will authenticate with the customer's system.  


### Description 

#### In addition to distributing FHIRcast events, the cast hub allows the following:

 - Request data from applications such as worklist context, report content, DICOM instance, DICOM tag, JPEG/PNG screenshots, scene views, etc.

 - Support for binary files transfer; therefore payloads other than FHIR/JSON, such as DICOM, PNG, NIFTi, openEHR, OpenIGTLink.

 - Group topics for multi-user workflows, such as tumor boards or interventional procedures.

 - Support for IHE roles.

 - Support three additional subscription data: 
     - subscriber.product.name, 
     - subscriber.product.version,
     - subscriber.actors
 
 - Support four additional event data: 
     - subscriber.name
     - subscriber.actor
     - target.actor
     - target.product.name


The hub provides a test mock auth endpoint that assigns a user  when none is provided. For public web applications that do not need user authentication but want to use the resource servers, the mock endpoints provide the required functionality.  

The hub also supports a “single-user” mode  for stand-alone applications.

#### The following FHIRcast features are not supported by Cast:
##### Context Management
Cast hub does not support context management.  Context is to be retrieved from the relevant applications directly through the request mechanism.   In cast, the hub is a routing appliance only.  It does not look at event data; it has no storage or database;  only distribution logic.   The context management paradigm was tried 30 years ago with CCOW ( <https://en.wikipedia.org/wiki/CCOW> ).  We have to acknowledge that today all advanced radiology integrations function without a context management server. They manage with events obtained through a combination of file drops, postMessages, URL with parameters, EXEs with command-line parameters, localhost web service and socket to to socket.  


##### OAuth 2.0 Authorization Scopes

FHIRcast defines OAuth 2.0 access scopes that correspond directly to FHIRcast events. These scopes associate read or write permissions to an event. Applications that need to receive workflow related events SHOULD ask for read scopes. Applications that request context changes SHOULD ask for write scopes.


This is related to the context management feature.  
It is not supported in the hub OAuth handling but the client can send them.


### How does the cast request work?
There is value to being able to obtain real-time information from other applications in the workfow.  For example, knowing the "sceneview" status of an Image Display application or the current content of the report editor.  This  is different than what a FHIRcast hub would know since it is relies on getting events to maintain it's context which are not generated for each user action. 

<p align="center">
  <img src="docs/images/request-event-flow.svg" alt="Request event flow" width="100%">
</p>





### Security Benefits for Resource Servers

This architecture protects resource servers by eliminating direct inbound internet exposure entirely.

Each resource server establishes only **outbound encrypted connections** to the Cast Hub, which functions exclusively as a  **routing  appliance**. Because no inbound ports need to be opened on hospital or enterprise networks, the resource servers remain protected behind existing firewalls and are never directly reachable from the public internet. 
It also simplifies providing reources since the IT department does not need to open ports on their router.  Since the resource servers are not on the internet, you will get shared keys for the auth server.  The hub can use domain name certificates.


For the hub, when it is on the internet, it provides a significantly reduced attack surface and minimizes operational security risk since it maintains no storage or database. 
<p align="center">
  <img src="docs/images/deployment.png" alt="Cast Interface Banner" width="100%">
</p>


### How does the binary file transfer work?

<p align="center">
  <img src="docs/images/binary-file-transfer.svg" alt="Binary file transfer" width="100%">
</p>


### Cloud deployment

In theory, the hub can be cloud deployed as a serverless application.  In practice, many of those low cost offerings do not support websocket services and a docker based offering is necessary like  Azure WebApps or AWS elastic beanstalk.  

For high availibity deployment a  hot stand-by configuration can be used.  The "reset server" button in the hub admin portal allows testing workflow behavior during failover.



## Installation

### Install from the 3D Slicer Extension Manager

1. Open **3D Slicer**
2. Open the **Extension Manager**
3. Search for **Cast Interface**
4. Click **Install**
5. Restart 3D Slicer

---

## License

MIT License

---

## Acknowledgements

* 3D Slicer community
* Open-source healthcare ecosystem
* Medical imaging interoperability initiatives
