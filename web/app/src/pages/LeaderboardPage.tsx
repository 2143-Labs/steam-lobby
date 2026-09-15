// /leaderboard/:game — all players by rating (mu - 3*sigma), via the read-only
// API. TanStack Query handles loading/error; unknown games get a 404 state.
import { useQuery } from "@tanstack/react-query";
import { Link, Navigate, useNavigate, useParams } from "react-router-dom";
import { ApiError, fetchLeaderboard, fetchModes } from "../api";
import { shortId } from "../lobby/store";

function rating(mu: number, sigma: number): number {
  return mu - 3 * sigma;
}

export default function LeaderboardPage() {
  const { game: routeGame } = useParams<{ game: string }>();
  const navigate = useNavigate();

  const modes = useQuery({
    queryKey: ["modes"],
    queryFn: () => fetchModes(),
    staleTime: 60_000,
  });

  // The bare /leaderboard route carries no :game, so the host's first
  // advertised mode resolves it. `enabled` keeps the board query from firing
  // while that is still unknown.
  const game = routeGame ?? modes.data?.[0]?.name;

  const board = useQuery({
    queryKey: ["leaderboard", game],
    queryFn: () => {
      if (!game) throw new Error("no game resolved");
      return fetchLeaderboard(game);
    },
    enabled: !!game,
    retry: 1,
  });

  // Order matters: the advertised modes resolve the game — and whether there is
  // one at all — before the board is rendered.
  if (modes.isPending || modes.isError) return <p>Loading…</p>;

  const modesList = modes.data ?? [];
  if (modesList.length === 0) {
    return (
      <>
        <h2>Leaderboard</h2>
        <p className="sys">This server advertises no game modes.</p>
        <p>
          <Link to="/">← Lobby</Link>
        </p>
      </>
    );
  }

  // Bare /leaderboard: redirect to the host's first advertised mode, so no
  // game name is chosen by this component.
  if (routeGame === undefined) {
    return <Navigate replace to={"/leaderboard/" + modesList[0].name} />;
  }

  if (board.isLoading) return <p>Loading leaderboard…</p>;

  if (board.error) {
    const status = board.error instanceof ApiError ? board.error.status : undefined;
    const detail = board.error instanceof Error ? board.error.message : String(board.error);
    return (
      <>
        <h2>Leaderboard</h2>
        {status === 404 ? (
          <p className="err">Unknown game: {game}</p>
        ) : (
          <p className="err">Failed to load leaderboard: {detail}</p>
        )}
        {modesList.length > 1 && (
          <select
            value={game}
            onChange={(e) => navigate("/leaderboard/" + e.target.value)}
          >
            {modesList.map((m) => (
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
      {modesList.length > 1 && (
        <label>
          Game{" "}
          <select value={game} onChange={(e) => navigate("/leaderboard/" + e.target.value)}>
            {modesList.map((m) => (
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
