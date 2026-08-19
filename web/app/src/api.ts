// Fetch helpers for the read-only stats API (no auth on GETs).
import type { AuthConfig, LeaderboardRow, ModeInfo, PlayerProfile } from "./types";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function getJson<T>(url: string): Promise<T> {
  const resp = await fetch(url);
  if (!resp.ok) {
    let detail = "HTTP " + resp.status;
    try {
      const body = await resp.json();
      if (typeof body?.error === "string") detail = body.error;
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(resp.status, detail);
  }
  return (await resp.json()) as T;
}

export function fetchLeaderboard(game: string): Promise<LeaderboardRow[]> {
  return getJson<LeaderboardRow[]>("/api/leaderboard/" + encodeURIComponent(game));
}

export function fetchPlayer(id: string): Promise<PlayerProfile> {
  return getJson<PlayerProfile>("/api/player/" + encodeURIComponent(id));
}

export function fetchModes(): Promise<ModeInfo[]> {
  return getJson<ModeInfo[]>("/modes");
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
