/** Base class for all Baran SDK errors. */
export class BaranError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    // Maintain proper prototype chain in transpiled JS
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Raised when the sidecar cannot be reached. */
export class BaranConnectionError extends BaranError {}

/** Raised when the PSK is invalid or missing. */
export class BaranAuthError extends BaranError {}

/** Raised when agent registration fails (e.g. conflict, limit reached). */
export class BaranRegistrationError extends BaranError {}

/** Raised when an event publish request fails. */
export class BaranPublishError extends BaranError {}
