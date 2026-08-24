const encoder = new TextEncoder();
const decoder = new TextDecoder();
const strictDecoder = new TextDecoder("utf-8", { fatal: true });

export interface ProtocolAttribute {
  readonly id: string;
  readonly value: Uint8Array;
  readonly required?: boolean;
}

const assertWellFormedUnicode = (value: string): void => {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError("String contains an unpaired UTF-16 surrogate.");
      index += 1;
      continue;
    }
    if (code >= 0xdc00 && code <= 0xdfff) throw new TypeError("String contains an unpaired UTF-16 surrogate.");
  }
};

export const bytes = (value: string): Uint8Array => {
  assertWellFormedUnicode(value);
  return encoder.encode(value);
};
export const text = (value?: Uint8Array): string => value ? decoder.decode(value) : "";
export const strictText = (value?: Uint8Array): string => value ? strictDecoder.decode(value) : "";

export const attribute = (
  id: string,
  value: string | Uint8Array,
  options: { required?: boolean } = {}
): ProtocolAttribute => {
  if (!id) throw new TypeError("Attribute id must not be empty.");
  assertWellFormedUnicode(id);
  return {
    id,
    value: typeof value === "string" ? bytes(value) : value,
    ...(options.required === undefined ? {} : { required: options.required })
  };
};

export const firstAttribute = (
  attributes: readonly ProtocolAttribute[],
  id: string
): ProtocolAttribute | undefined => attributes.find((item) => item.id === id);

export const attributeText = (
  attributes: readonly ProtocolAttribute[],
  id: string
): string | undefined => {
  const found = firstAttribute(attributes, id);
  return found ? text(found.value) : undefined;
};

export const attributesById = (
  attributes: readonly ProtocolAttribute[],
  id: string
): readonly ProtocolAttribute[] => attributes.filter((item) => item.id === id);

export const attributeTextStrict = (
  attributes: readonly ProtocolAttribute[],
  id: string
): string | undefined => {
  const found = firstAttribute(attributes, id);
  return found ? strictText(found.value) : undefined;
};
