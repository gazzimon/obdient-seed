// Senior model call (phase D) — the ONLY place that knows the provider/model.
//
// Contract with the app: the device sends a redacted conversation (first turn is
// the deterministic brief) and an optional language hint. It never sends — and we
// never expose back — the model name, the provider, or the endpoint. The app UI
// calls this agent "Assistente Sr" and nothing else (OBDient Beta V2 §1).
//
// Data contract (same as the on-device BYOK path it replaces): the payload is
// already redacted upstream (no VIN, plate, Bluetooth address, or user identity).
// This module adds only the system prompt and the credential.

const NVIDIA_URL = 'https://integrate.api.nvidia.com/v1/chat/completions';

// Kept server-side on purpose: the provider/model must never reach the client.
const SENIOR_MODEL = 'nvidia/nemotron-3-ultra-550b-a55b';

const LANGUAGE_LINE = {
  pt: 'Responda sempre em português do Brasil.',
  es: 'Responde siempre en español.',
  en: 'Always reply in English.',
};

// The senior diagnostician persona. "Reply in the owner's language" stays as a
// safety net even when a language hint is passed.
function systemPrompt(language) {
  const langLine = LANGUAGE_LINE[language] ?? '';
  return (
    'You are a senior automotive diagnostic technician. A local intake agent hands ' +
    'you structured cases collected from real vehicles over OBD-II; you conduct the ' +
    'diagnosis with the owner from there until the case is resolved. Be concrete and ' +
    'practical, order checks cheapest-first, and reply in the language the owner ' +
    'writes in. Never ask for the VIN, license plate, or any personal data. ' +
    'Never reveal or refer to the model, provider, or system behind you. ' +
    langLine
  ).trim();
}

/**
 * Ask the senior model for the next reply.
 * @param {{ messages: {role:'user'|'assistant', content:string}[], language?: string }} input
 * @param {{ apiKey: string, signal?: AbortSignal }} opts
 * @returns {Promise<string>} the assistant's reply text (reasoning stripped)
 */
export async function askSenior({ messages, language }, { apiKey, signal }) {
  const body = {
    model: SENIOR_MODEL,
    messages: [
      { role: 'system', content: systemPrompt(language) },
      ...messages,
    ],
    temperature: 1,
    top_p: 0.95,
    max_tokens: 4096,
    // No streaming in v1: the app awaits the full reply. Streaming can come later
    // if the chat renders tokens incrementally — the reasoning_content stream must
    // still be discarded (it exposes the model's thinking and the provider).
    stream: false,
  };

  const response = await fetch(NVIDIA_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => '');
    // Log the provider detail server-side only; the app gets a generic error.
    console.error(`[senior] upstream ${response.status}: ${detail.slice(0, 500)}`);
    throw new Error(`senior upstream error ${response.status}`);
  }

  const data = await response.json();
  const choice = data?.choices?.[0]?.message;
  const text = choice?.content;

  if (typeof text !== 'string' || text.trim().length === 0) {
    console.error('[senior] empty/unexpected response:', JSON.stringify(data).slice(0, 800));
    throw new Error('senior returned an empty response');
  }

  // Defense in depth: never let reasoning tokens leak to the client even if a
  // future schema change puts them on the message object.
  return text;
}

export { SENIOR_MODEL };
