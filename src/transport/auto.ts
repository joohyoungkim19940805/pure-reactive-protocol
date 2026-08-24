import { TransportUnavailableError } from "../core/errors";
import type { ReactiveTransport, ReactiveTransportFactory, TransportConnection } from "./types";

const abortReason = (signal: AbortSignal): unknown => signal.reason ?? Object.assign(new Error("Aborted"), { name: "AbortError" });

export class AutoTransport implements ReactiveTransport {
  readonly id = "auto";
  constructor(private readonly factories: readonly ReactiveTransportFactory[]) {}

  async connect(signal?: AbortSignal): Promise<TransportConnection> {
    if (signal?.aborted) throw abortReason(signal);
    const errors: unknown[] = [];
    for (const factory of [...this.factories].sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0))) {
      let supported = false;
      try { supported = factory.isSupported(); }
      catch (error) { errors.push(error); continue; }
      if (!supported) continue;
      try { return await factory.create().connect(signal); }
      catch (error) {
        if (signal?.aborted) throw abortReason(signal);
        errors.push(error);
      }
    }
    throw new TransportUnavailableError(
      `No transport could establish a connection.${errors.length ? ` ${errors.map(String).join(" | ")}` : ""}`
    );
  }
}
