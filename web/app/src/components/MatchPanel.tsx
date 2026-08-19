// Match panel — port of the demo's ctrl-match / ctrl-inmatch sections
// (web/index.html:99-113, 1021-1064). The START button is revealed ONLY by
// match_started (both players accepted) and counts down the server's window.
// Report buttons are never rendered — the server-authoritative pong game
import { clearStartCountdown, send, setStatus, showControls, state } from "../lobby/store";
import { useLobby } from "../hooks/useLobby";

export default function MatchPanel() {
  const st = useLobby();

  function accept() {
    if (!state.matchToken) return;
    send({ type: "accept_match", match_token: state.matchToken });
    // Reset the in-match panel so a previous game's state never leaks in.
    state.startBtn.visible = false;
    state.startBtn.disabled = true;
    state.serverInfo = null;
    state.oppP2p = "Waiting for opponent to start…";
    state.reportStatus = "—";
    showControls("inmatch");
  }

  function decline() {
    if (!state.matchToken) return;
    send({ type: "decline_match", match_token: state.matchToken });
    setStatus("Match declined", "");
    state.matchToken = null;
    state.gameType = null;
    state.serverInfo = null;
    showControls("connected");
  }

  function start() {
    if (!state.matchToken) return;
    clearStartCountdown();
    send({ type: "start_match", match_token: state.matchToken });
    state.startBtn.visible = false;
    state.oppP2p = "Waiting for opponent to start…";
  }

  if (st.controls === "match") {
    return (
      <div id="ctrl-match">
        <p>
          Opponent: <span id="opponent">{st.opponentName || st.opponentId || "—"}</span>
          <br />
          Match token: <span className="sys">{st.matchToken || "—"}</span>
        </p>
        <button className="primary" onClick={accept}>
          Accept Match
        </button>
        <button onClick={decline}>Decline Match</button>
      </div>
    );
  }

  if (st.controls === "inmatch") {
    return (
      <div id="ctrl-inmatch">
        {st.serverInfo !== null && <p id="server-info">{st.serverInfo}</p>}
        <p>
          P2P: <span id="opp-p2p">{st.oppP2p}</span>
        </p>
        <p>
          Result: <span id="report-status">{st.reportStatus}</span>
        </p>
        {st.startBtn.visible && (
          <button
            id="btn-start"
            className="primary"
            disabled={st.startBtn.disabled}
            onClick={start}
          >
            {st.startBtn.label}
          </button>
        )}
      </div>
    );
  }

  return null;
}
