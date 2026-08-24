export class PureReactiveProtocolError extends Error {
  constructor(message: string, readonly code: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "PureReactiveProtocolError";
  }
}

export class ProtocolViolationError extends PureReactiveProtocolError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "PROTOCOL_VIOLATION", options);
    this.name = "ProtocolViolationError";
  }
}

export class CapabilityMismatchError extends PureReactiveProtocolError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "CAPABILITY_MISMATCH", options);
    this.name = "CapabilityMismatchError";
  }
}

export class ConnectionLostError extends PureReactiveProtocolError {
  constructor(message = "The physical connection was lost.", options?: ErrorOptions) {
    super(message, "CONNECTION_LOST", options);
    this.name = "ConnectionLostError";
  }
}


export class LivenessTimeoutError extends PureReactiveProtocolError {
  constructor(message = "The peer did not respond within the negotiated liveness timeout.") {
    super(message, "LIVENESS_TIMEOUT");
    this.name = "LivenessTimeoutError";
  }
}
export class StreamClosedError extends PureReactiveProtocolError {
  constructor(message = "The reactive stream is closed.") {
    super(message, "STREAM_CLOSED");
    this.name = "StreamClosedError";
  }
}

export class TransportUnavailableError extends PureReactiveProtocolError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "TRANSPORT_UNAVAILABLE", options);
    this.name = "TransportUnavailableError";
  }
}
