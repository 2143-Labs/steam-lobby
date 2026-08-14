// The game area: pong canvas or RPS panel per the current game mode, plus the
// game-status line and the connection-metrics line (the latter is written
// imperatively by the metrics updater into the element it finds by id).
import PongCanvas from "./PongCanvas";
import RpsPanel from "./RpsPanel";
import { useLobby } from "../hooks/useLobby";

export default function GameArea() {
  const st = useLobby();
  const visible = st.controls === "queueing" || st.controls === "inmatch";
  if (!visible) return null;

  return (
    <div id="game-area">
      <PongCanvas />
      <RpsPanel />
      <p id="game-status">{st.gameStatus}</p>
      <p id="conn-metrics">Server: — · Opponent: —</p>
    </div>
  );
}
