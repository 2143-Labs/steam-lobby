// Wire protocol mirrors: lobby-server/src/ws.rs ClientMessage/ServerMessage
// (tag=type, snake_case). Hand-maintained — keep in sync with the Rust enums.
// `target`/`checksum` travel as decimal strings for bit-exactness (serde_json
// f64 parsing is not correctly rounded); parse with parseFloat (exact).

export type PlayerStateValue = "InMenus" | "Queueing" | "MatchAccepted" | "InMatch" | "Reporting";
export type GameTypeValue = "p2p" | "server";

export interface LeaderboardEntry {
  player_id: string;
  mu: number;
  sigma: number;
  rating: number;
}

export interface OpponentInfo {
  player_id: string;
  display_name: string;
}

/** MatchOutcome serialized (externally tagged enum): { Win|Loss|Draw|Forfeit: { mu_change } } | "Disputed" | "UnreviewableDispute" */
export type MatchOutcome =
  | { Win: { mu_change?: number } }
  | { Loss: { mu_change?: number } }
  | { Draw: { mu_change?: number } }
  | { Forfeit: { mu_change?: number } }
  | "Disputed"
  | "UnreviewableDispute"
  | null;
export type ServerMessage =
  | { type: "auth_ok"; player_id: string; display_name: string; state: PlayerStateValue }
  | {
      type: "queue_status";
      elapsed_ms: number;
      band_lo: number;
      band_hi: number;
      candidates: number;
      queue_size: number;
      my_mu: number;
      my_sigma: number;
      my_rating: number;
      leaderboard: LeaderboardEntry[];
    }
  | { type: "opponent_connected"; match_token: string }
  | {
      type: "report_received";
      match_token: string;
      reporting_player: string;
      winner: string | null;
      demo_hash: string | null;
    }
  | {
      type: "match_found";
      match_token: string;
      opponent: OpponentInfo;
      timeout_ms: number;
      game_type: GameTypeValue;
      game_mode: string;
    }
  | { type: "match_started"; match_token: string; start_timeout_secs: number }
  | { type: "game_server_ready"; match_token: string; address: string; join_token: string | null }
  | { type: "game_server_error"; match_token: string; message: string }
  | {
      type: "game_state";
      match_token: string;
      frame: number;
      player_a: string;
      player_b: string;
      left_y: number;
      right_y: number;
      ball_x: number;
      ball_y: number;
      left_score: number;
      right_score: number;
      speed: number;
      checksum: string;
    }
  | { type: "input_ack"; match_token: string; frame: number }
  | { type: "round_start"; match_token: string; frame: number; round: number; countdown_ticks: number }
  | { type: "rps_begin"; match_token: string; round: number; timeout_ms: number; player_a: string; player_b: string }
  | {
      type: "rps_round";
      match_token: string;
      round: number;
      a_choice: number;
      b_choice: number;
      winner: string | null;
      a_score: number;
      b_score: number;
    }
  | { type: "peer_input"; match_token: string; from: string; frame: number; target: string }
  | { type: "rollback_resync"; match_token: string; frame: number; state: string }
  | { type: "webrtc_offer"; match_token: string; from: string; sdp: string }
  | { type: "webrtc_answer"; match_token: string; from: string; sdp: string }
  | { type: "webrtc_ice"; match_token: string; from: string; candidate: string }
  | { type: "game_over"; match_token: string; winner: string }
  | { type: "match_result"; match_token: string; outcome: MatchOutcome }
  | { type: "match_declined"; match_token: string }
  | { type: "match_expired"; match_token: string }
  | { type: "queue_expired" }
  | { type: "error"; code: string; message: string };

export function parseServerMessage(raw: string): ServerMessage | null {
  try {
    const msg = JSON.parse(raw) as ServerMessage;
    return typeof msg?.type === "string" ? msg : null;
  } catch {
    return null;
  }
}

// ── Client messages (tag=type, snake_case) ────────────────────────────────

export type ClientMessage =
  | { type: "auth"; session_token: string }
  | { type: "begin_matchmaking"; mode: string; difficulty: string }
  | { type: "cancel_matchmaking" }
  | { type: "accept_match"; match_token: string }
  | { type: "decline_match"; match_token: string }
  | { type: "start_match"; match_token: string }
  | { type: "game_input"; match_token: string; frame: number; target: string }
  | { type: "rps_choice"; match_token: string; choice: number }
  | { type: "webrtc_offer"; match_token: string; sdp: string }
  | { type: "webrtc_answer"; match_token: string; sdp: string }
  | { type: "webrtc_ice"; match_token: string; candidate: string }
  | { type: "rollback_health"; match_token: string; frame: number; checksum: string }
  | { type: "match_report"; match_token: string; winner: string | null; demo_hash: string | null }
  | { type: "heartbeat" };
