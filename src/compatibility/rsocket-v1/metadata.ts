const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export const RSOCKET_COMPOSITE_METADATA_MIME = "message/x.rsocket.composite-metadata.v0";
export const RSOCKET_ROUTING_MIME = "message/x.rsocket.routing.v0";
export const RSOCKET_JSON_MIME = "application/json";
export const RSOCKET_BINARY_MIME = "application/octet-stream";

export const WELL_KNOWN_MIME_ROUTING = 0x7e;
export const WELL_KNOWN_MIME_COMPOSITE = 0x7f;

const writeU24 = (target: Uint8Array, offset: number, value: number): void => {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffff) throw new RangeError("Value must fit unsigned 24 bits.");
  target[offset] = (value >>> 16) & 0xff;
  target[offset + 1] = (value >>> 8) & 0xff;
  target[offset + 2] = value & 0xff;
};

const readU24 = (value: Uint8Array, offset: number): number =>
  ((value[offset] ?? 0) << 16) | ((value[offset + 1] ?? 0) << 8) | (value[offset + 2] ?? 0);

export interface CompositeMetadataEntry {
  readonly mimeType: string;
  readonly content: Uint8Array;
}

const wellKnownMimeId = (mimeType: string): number | undefined => {
  if (mimeType === RSOCKET_ROUTING_MIME) return WELL_KNOWN_MIME_ROUTING;
  if (mimeType === RSOCKET_COMPOSITE_METADATA_MIME) return WELL_KNOWN_MIME_COMPOSITE;
  return undefined;
};

export const encodeRoute = (route: string): Uint8Array => {
  const encoded = encoder.encode(route);
  if (encoded.byteLength === 0 || encoded.byteLength > 255) throw new RangeError("RSocket route must encode to 1..255 UTF-8 bytes.");
  const output = new Uint8Array(1 + encoded.byteLength);
  output[0] = encoded.byteLength;
  output.set(encoded, 1);
  return output;
};

export const decodeRoutes = (metadata: Uint8Array): readonly string[] => {
  const routes: string[] = [];
  let offset = 0;
  while (offset < metadata.byteLength) {
    const length = metadata[offset++]!;
    if (length === 0 || offset + length > metadata.byteLength) throw new RangeError("Malformed RSocket routing metadata.");
    routes.push(decoder.decode(metadata.subarray(offset, offset + length)));
    offset += length;
  }
  return routes;
};

export const encodeCompositeMetadata = (entries: readonly CompositeMetadataEntry[]): Uint8Array => {
  const encoded = entries.map((entry) => {
    const known = wellKnownMimeId(entry.mimeType);
    let header: Uint8Array;
    if (known !== undefined) {
      header = Uint8Array.of(0x80 | known);
    } else {
      const mime = encoder.encode(entry.mimeType);
      if (mime.byteLength === 0 || mime.byteLength > 128) throw new RangeError("Composite metadata MIME type must encode to 1..128 bytes.");
      header = new Uint8Array(1 + mime.byteLength);
      // The 7-bit explicit MIME length stores length-1 so 128 bytes are representable.
      header[0] = mime.byteLength - 1;
      header.set(mime, 1);
    }
    if (entry.content.byteLength > 0xffffff) throw new RangeError("Composite metadata entry exceeds 24-bit length.");
    const output = new Uint8Array(header.byteLength + 3 + entry.content.byteLength);
    output.set(header, 0);
    writeU24(output, header.byteLength, entry.content.byteLength);
    output.set(entry.content, header.byteLength + 3);
    return output;
  });
  const length = encoded.reduce((total, value) => total + value.byteLength, 0);
  const output = new Uint8Array(length);
  let offset = 0;
  for (const value of encoded) { output.set(value, offset); offset += value.byteLength; }
  return output;
};

export const decodeCompositeMetadata = (metadata: Uint8Array): readonly CompositeMetadataEntry[] => {
  const entries: CompositeMetadataEntry[] = [];
  let offset = 0;
  while (offset < metadata.byteLength) {
    const first = metadata[offset++]!;
    let mimeType: string;
    if ((first & 0x80) !== 0) {
      const id = first & 0x7f;
      if (id === WELL_KNOWN_MIME_ROUTING) mimeType = RSOCKET_ROUTING_MIME;
      else if (id === WELL_KNOWN_MIME_COMPOSITE) mimeType = RSOCKET_COMPOSITE_METADATA_MIME;
      else mimeType = `x.rsocket.well-known.${id}`;
    } else {
      const length = (first & 0x7f) + 1;
      if (offset + length > metadata.byteLength) throw new RangeError("Malformed composite metadata MIME type.");
      mimeType = decoder.decode(metadata.subarray(offset, offset + length));
      offset += length;
    }
    if (offset + 3 > metadata.byteLength) throw new RangeError("Malformed composite metadata length.");
    const contentLength = readU24(metadata, offset);
    offset += 3;
    if (offset + contentLength > metadata.byteLength) throw new RangeError("Truncated composite metadata payload.");
    entries.push({ mimeType, content: metadata.slice(offset, offset + contentLength) });
    offset += contentLength;
  }
  return entries;
};

export const encodeRoutingCompositeMetadata = (route: string): Uint8Array =>
  encodeCompositeMetadata([{ mimeType: RSOCKET_ROUTING_MIME, content: encodeRoute(route) }]);

export const firstRouteFromCompositeMetadata = (metadata?: Uint8Array): string | undefined => {
  if (!metadata || metadata.byteLength === 0) return undefined;
  for (const entry of decodeCompositeMetadata(metadata)) {
    if (entry.mimeType !== RSOCKET_ROUTING_MIME) continue;
    return decodeRoutes(entry.content)[0];
  }
  return undefined;
};
