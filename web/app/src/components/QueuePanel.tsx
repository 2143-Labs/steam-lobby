// Matchmaking panel — port of the demo's queue section (web/index.html:80-98,
// 1002-1020). Fix (2): the queue preview is mode-aware — queueing rps_1v1
// shows the RPS preview panel (disabled buttons, "Waiting for an opponent…"),
// any other mode shows the pong practice canvas.
import { useState } from "react";
import { Link } from "react-router-dom";
import { getAvailableModes } from "../lobby/client";
import { notify, send, setStatus, showControls, shortId, state } from "../lobby/store";
import { startQueuePreview } from "../game/pong";
import { useLobby } from "../hooks/useLobby";

const DIFFICULTIES = ["easy", "normal", "hard"];

export default function QueuePanel() {
  const st = useLobby();
  const modes = getAvailableModes();
  const [difficulty, setDifficulty] = useState("normal");

  function startQueue() {
    const mode = st.selectedMode;
    send({ type: "begin_matchmaking", mode, difficulty });
    setStatus("Queueing…", "");
    showControls("queueing");
    // Mode-aware preview: rps_1v1 → RPS preview panel; else pong practice.
    startQueuePreview(mode);
    const q = state.queue;
    q.wait = "waiting…";
    q.band = "—";
    q.candidates = "—";
    q.size = "—";
    q.myMu = "—";
    q.mySigma = "—";
    q.myRating = "—";
    q.leaderboard = [];
    notify();
  }

  function cancelQueue() {
    send({ type: "cancel_matchmaking" });
    setStatus("Connected", "ok");
    showControls("connected");
  }

  if (st.controls === "queueing") {
    return (
      <section>
        <div id="ctrl-queueing">
          <button onClick={cancelQueue}>Cancel Matchmaking</button>
          <div id="queue-stats">
            <p>
              Waiting: <span className="in">{st.queue.wait}</span> — MMR band{" "}
              <span className="in">{st.queue.band}</span> — opponents in band:{" "}
              <span className="in">{st.queue.candidates}</span> (queue size{" "}
              <span className="in">{st.queue.size}</span>)
            </p>
            <p>
              My MMR: μ <span className="in">{st.queue.myMu}</span> σ{" "}
              <span className="in">{st.queue.mySigma}</span> rating{" "}
              <span className="in">{st.queue.myRating}</span>
            </p>
          </div>
          <table id="leaderboard">
            <thead>
              <tr>
                <th>Player</th>
                <th>μ</th>
                <th>σ</th>
                <th>Rating</th>
              </tr>
            </thead>
            <tbody>
              {st.queue.leaderboard.map((e) => (
                <tr key={e.player_id}>
                  <td>
                    <Link to={"/player/" + e.player_id}>{shortId(e.player_id)}</Link>
                  </td>
                  <td>{e.mu.toFixed(1)}</td>
                  <td>{e.sigma.toFixed(1)}</td>
                  <td>{e.rating.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    );
  }

  return (
    <section>
      <div id="ctrl-connected">
        <label>Mode</label>
        <select
          value={st.selectedMode}
          onChange={(e) => {
            state.selectedMode = e.target.value;
            notify();
          }}
        >
          {modes.map((m) => (
            <option key={m.name} value={m.name}>
              {m.name} ({m.game_type})
            </option>
          ))}
        </select>
        <label>Difficulty</label>
        <select value={difficulty} onChange={(e) => setDifficulty(e.target.value)}>
          {DIFFICULTIES.map((d) => (
            <option key={d}>{d}</option>
          ))}
        </select>
        <button className="primary" onClick={startQueue}>
          Start Matchmaking
        </button>
      </div>
    </section>
  );
}
