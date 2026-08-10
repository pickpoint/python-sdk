"""Generated tracking.v2 protobuf stubs."""

from . import messages_pb2 as messages
from . import service_pb2 as service
from . import service_pb2_grpc as service_grpc

LatLng = messages.LatLng
ClientMsg = messages.ClientMsg
ServerMsg = messages.ServerMsg
Resume = messages.Resume
TrackStart = messages.TrackStart
TrackStop = messages.TrackStop
LocationAdd = messages.LocationAdd
LocationBatch = messages.LocationBatch
Subscribe = messages.Subscribe
Event = messages.Event
CommandAck = messages.CommandAck
CommandAckStatus = messages.CommandAckStatus
Hello = messages.Hello
Relocate = messages.Relocate
ResumeOk = messages.ResumeOk
TrackStarted = messages.TrackStarted
TrackStopped = messages.TrackStopped
LocationAdded = messages.LocationAdded
Subscribed = messages.Subscribed
Error = messages.Error
ErrorCode = messages.ErrorCode
Command = messages.Command
Pong = messages.Pong
EventAdded = messages.EventAdded
Ping = messages.Ping

TrackingStub = service_grpc.TrackingStub
TrackingServicer = service_grpc.TrackingServicer
add_TrackingServicer_to_server = service_grpc.add_TrackingServicer_to_server

__all__ = [
    "LatLng",
    "ClientMsg",
    "ServerMsg",
    "Resume",
    "TrackStart",
    "TrackStop",
    "LocationAdd",
    "LocationBatch",
    "Subscribe",
    "Event",
    "CommandAck",
    "CommandAckStatus",
    "Hello",
    "Relocate",
    "ResumeOk",
    "TrackStarted",
    "TrackStopped",
    "LocationAdded",
    "Subscribed",
    "Error",
    "ErrorCode",
    "Command",
    "Pong",
    "EventAdded",
    "Ping",
    "TrackingStub",
    "TrackingServicer",
    "add_TrackingServicer_to_server",
    "messages",
    "service",
    "service_grpc",
]
