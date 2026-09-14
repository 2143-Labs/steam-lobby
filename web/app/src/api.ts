// Fetch helpers for the website API.
import type {
  AuthConfig,
  LeaderboardRow,
  LinkIntentResponse,
  ModeInfo,
  PlayerProfile,
  SessionInfo,
} from "./types";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function responseError(resp: Response): Promise<ApiError> {
  let detail = "HTTP " + resp.status;
  try {
    const body = await resp.json();
    if (typeof body?.error === "string") detail = body.error;
  } catch {
    /* non-JSON error body */
  }
  return new ApiError(resp.status, detail);
}

async function getJson<T>(url: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(url, init);
  if (!resp.ok) throw await responseError(resp);
  return (await resp.json()) as T;
}

export function fetchLeaderboard(game: string): Promise<LeaderboardRow[]> {
  return getJson<LeaderboardRow[]>("/api/leaderboard/" + encodeURIComponent(game));
}

export function fetchPlayer(id: string): Promise<PlayerProfile> {
  return getJson<PlayerProfile>("/api/player/" + encodeURIComponent(id));
}

export async function fetchModes(base = ""): Promise<ModeInfo[]> {
  const body = await getJson<{ modes: ModeInfo[] }>(base + "/modes");
  return body.modes;
}

/** Auth config; null when unreachable/404 (offline, file://, prod w/o dev). */
export async function fetchAuthConfig(): Promise<AuthConfig | null> {
  try {
    const resp = await fetch("/auth/config");
    return resp.ok ? ((await resp.json()) as AuthConfig) : null;
  } catch {
    return null;
  }
}

/** Return the live cookie session, or null when there is none. */
export async function fetchSession(base = ""): Promise<SessionInfo | null> {
  const resp = await fetch(base + "/api/session", {
    credentials: "include",
    cache: "no-store",
  });
  if (resp.status === 401) return null;
  if (!resp.ok) throw await responseError(resp);
  return (await resp.json()) as SessionInfo;
}

export async function createLinkIntent(
  csrfToken: string,
  base = "",
): Promise<LinkIntentResponse> {
  return getJson<LinkIntentResponse>(base + "/api/link/intent", {
    method: "POST",
    credentials: "include",
    headers: { "X-CSRF-Token": csrfToken },
  });
}

export async function confirmLinkIntent(
  intentId: string,
  csrfToken: string,
  base = "",
): Promise<void> {
  const resp = await fetch(base + "/api/link/confirm", {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
    body: JSON.stringify({ intent_id: intentId }),
  });
  if (!resp.ok) throw await responseError(resp);
}
