// API wire types — hand-maintained mirrors of the Rust response structs in
// lobby-server/src/routes.rs (snake_case, matching the WS protocol style).
// Keep in sync when the server API changes.

export interface LeaderboardRow {
  player_id: string;
  display_name: string;
  mu: number;
  sigma: number;
  rating: number;
}

export interface PlayerIdentity {
  provider: string;
  last_login_at: string;
}

export interface PlayerRating {
  game_mode: string;
  mu: number;
  sigma: number;
  rating: number;
  last_updated: string;
}

export interface RecentMatch {
  match_token: string;
  game_mode: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  opponent_id: string;
  opponent_name: string;
  /** null when the match has no stored result yet (LEFT JOIN). */
  outcome: string | null;
  mu_change: number | null;
}

export interface PlayerProfile {
  player_id: string;
  display_name: string;
  primary_provider: string;
  created_at: string;
  identities: PlayerIdentity[];
  ratings: PlayerRating[];
  recent_matches: RecentMatch[];
}

/** GET /auth/config */
export interface AuthConfig {
  providers: string[];
  dev_mode: boolean;
  guest_login: boolean;
}

/** GET /modes */
export interface ModeInfo {
  name: string;
  game_type: string;
}
