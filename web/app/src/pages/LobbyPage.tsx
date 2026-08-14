// The lobby page — composes the demo's sections into one page: header + nav,
// connect panel, status, controls (queue / match), game area, log.
import { Link } from "react-router-dom";
import ConnectPanel from "../components/ConnectPanel";
import GameArea from "../components/GameArea";
import MatchPanel from "../components/MatchPanel";
import QueuePanel from "../components/QueuePanel";
import StatusBar from "../components/StatusBar";
import { useLobby } from "../hooks/useLobby";

export default function LobbyPage() {
  const st = useLobby();
  const inMatch = st.controls === "match" || st.controls === "inmatch";

  return (
    <>
      <h1>Steam Lobby — protocol demo</h1>
      <p className="sys">
        Uses native <code>fetch</code> + <code>WebSocket</code>. Sign in through Steam for a
        genuine login (needs the server reachable via its public URL), or open two tabs with
        distinct dev steam IDs (server must run with <code>AUTH_DEV_MODE=true</code>), connect
        both, then start matchmaking in each.
      </p>
      <nav>
        <Link className="primary" to="/leaderboard/ranked_1v1">
          Leaderboard
        </Link>
      </nav>
      <ConnectPanel />
      <StatusBar />
      <div id="controls">
        {inMatch ? <MatchPanel /> : <QueuePanel />}
        <GameArea />
      </div>
    </>
  );
}
