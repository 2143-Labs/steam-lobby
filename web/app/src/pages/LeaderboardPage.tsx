// /leaderboard/:game — all players by rating (mu - 3*sigma), via the read-only
// API. TanStack Query handles loading/error; unknown games get a 404 state.
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { fetchLeaderboard, fetchModes } from "../api";
import { shortId } from "../lobby/store";

function rating(mu: number, sigma: number): number {
  return mu - 3 * sigma;
}

export default function LeaderboardPage() {
  const { game = "ranked_1v1" } = useParams<{ game: string }>();
  const navigate = useNavigate();

  const modes = useQuery({
    queryKey: ["modes"],
    queryFn: fetchModes,
    staleTime: 60_000,
  });

  const board = useQuery({
    queryKey: ["leaderboard", game],
    queryFn: () => fetchLeaderboard(game),
    retry: 1,
  });

  if (board.isLoading) return <p>Loading leaderboard…</p>;

  if (board.error) {
    const status = board.error instanceof Error && "status" in board.error
      ? (board.error as { status?: number }).status
      : undefined;
    return (
      <>
        <h2>Leaderboard</h2>
        {status === 404 || modes.isError ? (
          <p className="err">Unknown game: {game}</p>
        ) : (
          <p className="err">Failed to load leaderboard: {(board.error as Error).message}</p>
        )}
        {modes.data && modes.data.length > 0 && (
          <select
            value={game}
            onChange={(e) => navigate("/leaderboard/" + e.target.value)}
          >
            {modes.data.map((m) => (
              <option key={m.name} value={m.name}>
                {m.name} ({m.game_type})
              </option>
            ))}
          </select>
        )}
        <p>
          <Link to="/">← Lobby</Link>
        </p>
      </>
    );
  }

  const rows = board.data ?? [];
  return (
    <>
      <h2>Leaderboard — {game}</h2>
      {modes.data && modes.data.length > 0 && (
        <label>
          Game{" "}
          <select value={game} onChange={(e) => navigate("/leaderboard/" + e.target.value)}>
            {modes.data.map((m) => (
              <option key={m.name} value={m.name}>
                {m.name} ({m.game_type})
              </option>
            ))}
          </select>
        </label>
      )}
      {rows.length === 0 ? (
        <p className="sys">No rated players yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Player</th>
              <th>μ</th>
              <th>σ</th>
              <th>Rating</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={r.player_id}>
                <td>{i + 1}</td>
                <td>
                  <Link to={"/player/" + r.player_id}>
                    {r.display_name && r.display_name !== "Unknown"
                      ? r.display_name
                      : shortId(r.player_id)}
                  </Link>
                </td>
                <td>{r.mu.toFixed(1)}</td>
                <td>{r.sigma.toFixed(1)}</td>
                <td>{rating(r.mu, r.sigma).toFixed(1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p>
        <Link to="/">← Lobby</Link>
      </p>
    </>
  );
}
