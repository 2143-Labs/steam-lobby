// /player/:playerId — profile: linked accounts, per-game MMR, match history.
// Identities show provider + last login ONLY (never the provider_uid — the
// server already omits it). Self-view ("this is you") when the decoded
// #token sub matches; the maintenance hook is UI-only for now.
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { fetchPlayer } from "../api";
import { jwtSub, shortId, state } from "../lobby/store";

function fmtDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toISOString().slice(0, 10);
}

export default function PlayerPage() {
  const { playerId = "" } = useParams<{ playerId: string }>();
  const me = jwtSub(state.token) ?? null;

  const q = useQuery({
    queryKey: ["player", playerId],
    queryFn: () => fetchPlayer(playerId),
    retry: 1,
  });

  if (q.isLoading) return <p>Loading player…</p>;

  if (q.error) {
    const status = q.error instanceof Error && "status" in q.error
      ? (q.error as { status?: number }).status
      : undefined;
    return (
      <>
        <h2>Player</h2>
        <p className="err">
          {status === 404 ? "Unknown player: " + shortId(playerId) : "Failed to load player: " + (q.error as Error).message}
        </p>
        <p>
          <Link to="/">← Lobby</Link>
        </p>
      </>
    );
  }

  const p = q.data!;
  const isMe = me !== null && me === p.player_id;
  return (
    <>
      <h2>
        {p.display_name && p.display_name !== "Unknown" ? p.display_name : shortId(p.player_id)}
      </h2>
      {isMe && <p className="ok">This is you.</p>}
      <p className="sys">
        Player ID: {p.player_id} · Primary provider: {p.primary_provider} · Joined:{" "}
        {fmtDate(p.created_at)}
      </p>
      <h3>Linked accounts</h3>
      {p.identities.length === 0 ? (
        <p className="sys">No linked accounts (guest).</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Provider</th>
              <th>Last login</th>
            </tr>
          </thead>
          <tbody>
            {p.identities.map((id) => (
              <tr key={id.provider}>
                <td>{id.provider}</td>
                <td>{fmtDate(id.last_login_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <h3>Ratings</h3>
      {p.ratings.length === 0 ? (
        <p className="sys">No rated games yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Game</th>
              <th>μ</th>
              <th>σ</th>
              <th>Rating</th>
            </tr>
          </thead>
          <tbody>
            {p.ratings.map((r) => (
              <tr key={r.game_mode}>
                <td>
                  <Link to={"/leaderboard/" + r.game_mode}>{r.game_mode}</Link>
                </td>
                <td>{r.mu.toFixed(1)}</td>
                <td>{r.sigma.toFixed(1)}</td>
                <td>{r.rating.toFixed(1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <h3>Recent matches</h3>
      {p.recent_matches.length === 0 ? (
        <p className="sys">No matches yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Game</th>
              <th>Opponent</th>
              <th>Outcome</th>
              <th>μ change</th>
              <th>Ended</th>
            </tr>
          </thead>
          <tbody>
            {p.recent_matches.map((m) => (
              <tr key={m.match_token}>
                <td>{m.game_mode}</td>
                <td>
                  <Link to={"/player/" + m.opponent_id}>
                    {m.opponent_name && m.opponent_name !== "Unknown"
                      ? m.opponent_name
                      : shortId(m.opponent_id)}
                  </Link>
                </td>
                <td>{m.outcome ?? m.status}</td>
                <td>{m.mu_change !== null ? (m.mu_change >= 0 ? "+" : "") + m.mu_change.toFixed(2) : "—"}</td>
                <td>{fmtDate(m.ended_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {isMe && <p className="sys">Account maintenance (rename, link management) coming later.</p>}
      <p>
        <Link to="/">← Lobby</Link>
      </p>
    </>
  );
}
