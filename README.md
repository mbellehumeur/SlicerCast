# 3D Slicer Cast Interface Extension

<p align="center">
  <img src="docs/images/banner.png" alt="Cast Interface Banner" width="70%">
</p>


---

## Overview

Cast Interface is a 3D Slicer extension focused on desktop integration workflows for healthcare applications.


---


---
## Background

Cast is an offshoot of FHIRcast (<https://fhircast.hl7.org/>). FHIRcast is the standard replacing Epic’s file drop interface for integration with PACS and reporting systems. It provides a secure messaging infrastructure using a hub with websocket subscriptions.  The following animations hightlights the low-latency and benefit of central message routing. 
<figure>
  <p align="center">
    <img src="docs/images/imagingstudy-open-flow.svg"
         alt="ImagingStudy-open event flow: user selects an exam on the worklist, worklist publishes imagingstudy-open to the hub over HTTP POST, hub fans the event over WebSocket to Image Display, Reporting, and EHR, and each app updates its UI."
         width="100%">
  </p>
  <figcaption>The worklist publishes <code>imagingstudy-open</code> on the user topic; the hub fans the same event to every subscribed app (Image Display, Reporting, EHR, and others).</figcaption>
</figure>


Cast is focused on desktop integration of all healthcare applications. It is not restricted to a specific data format or authentication/authorization mechanism.  Cast also has a context sharing strategy and architecture that diverges from FHIRcast (described here).



## Extension Features
Resource servers: Resource servers subscribe to all user topics for dicom/nifti events and send back results to the user throuh the hub. Each server has its own onMessage script. The script handles producing the results from the DICOM files received and publishes a dicom-send event back to the user topic.

Image Display Client: The image display client provide a PACS client type interface to the 3D slicer viewer. Supported events should be ImagingStudy-open, Imaging-Study-close, dicom-send and request for sceneview.

Hub: The hub is the server that distributes the messages and handles the data transfer requests over the websocket connection to each client.


### Description 

In addition to distributing FHIRcast events, Cast allows the following:

 - Request data from applications such as worklist context, report content, DICOM instance, DICOM metadata, JPEG/PNG screenshots, scene views, etc.

 - Support for binary files transfer; therefore payloads other than FHIR/JSON, such as DICOM, PNG, NIFTi,ect. 

 - Support for resource servers.

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



### How does the cast request work?
There is value to being able to obtain real-time information from other applications in the workfow.  For example, knowing the "sceneview" status of an Image Display application or the current content of the report editor.  This  is different than what a FHIRcast hub would know since it is relies on getting events to maintain it's context which are not generated for each user action. 

The following animation shows the added resiliency and data exchange that this feature provides.

*Animation description:  The image display application has crashed and the user restarts the app without context.  It finds which study to load from the worklist client and then queries the reporting client to get the measurements in the template.  The measurements are used to populate annotation labeling drop-down in the image display tools.*

<p align="center">
  <img src="docs/images/request-event-flow.svg" alt="Request event flow" width="100%">
</p>



### How does binary file transfer work?

Cast uses a **notify then download** model: the WebSocket carries JSON and a
`payloadId` per file; file bytes live in the hub’s short-lived HTTP store until a
subscriber calls `GET /api/hub/payloads/{payloadId}`.

All binary uploads use **one STOW batch** per publish — `multipart/related` with
a JSON manifest (`event.context.files[]`) plus one HTTP part per file. That
covers DICOM slices, NIfTI volumes, and other binary-family events. There is no
separate single-file `multipart/form-data` path.

Before forwarding the JSON file metadata to the recipients over websocket, the hub adds the short-live payloadId to each file metadata so that they can be downloaded.
For DICOM files, the DICOM metadata of each file is therefore available before the download.  REcipients can therefore select which file they actually need and download those in the order that they want.  This provides something similar to DICOM association but at file level and with all info available ; not just SOP class and transfer syntaxes.  For example, if a complete study is sent to  Total Segmentator, its script can choose to only down one series of thin slices; saving time and bandwidth.


Resource servers (e.g. TotalSegmentator) receive metadata on the socket, then
`fetch_all_payloads` fills `files[].data` before your `onMessage` script runs.

Full description: [docs/binary-file-transfer.md](docs/binary-file-transfer.md).

<p align="center">
  <img src="docs/images/binary-file-transfer-animated.svg" alt="Binary file transfer (animated)" width="100%">
</p>

Static overview: [docs/images/binary-file-transfer.svg](docs/images/binary-file-transfer.svg).



### Security Benefits for cloud deployment of Resource Servers

This architecture protects resource servers by eliminating direct inbound internet exposure entirely.

Each resource server establishes only **outbound encrypted connections** to the Cast Hub, which functions exclusively as a  **routing  appliance**. Because no inbound ports need to be opened on hospital or enterprise networks, the resource servers remain protected behind existing firewalls and are never directly reachable from the public internet. 
It also simplifies providing reources since the IT department does not need to open ports on their router.  Since the resource servers are not on the internet, you will get shared keys for the auth server.  The hub can use domain name certificates.


For the hub, when it is on the internet, it provides a significantly reduced attack surface and minimizes operational security risk since it maintains no storage or database. 
<p align="center">
  <img src="docs/images/deployment.png" alt="Cast Interface Banner" width="100%">
</p>




In theory, the hub can be cloud deployed as a serverless application.  In practice, many of those low cost offerings do not support websocket services and a docker based offering is necessary like  Azure WebApps or AWS elastic beanstalk.  

For high availibity deployment a  hot stand-by configuration can be used.  The "reset server" button in the hub admin portal allows testing workflow behavior during failover.

The hub provides a test mock auth endpoint that assigns a user  when none is provided. For public web applications that do not need user authentication but want to use the resource servers, the mock endpoints provide the required functionality.  

The hub also supports a “single-user” mode  for stand-alone applications.

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
