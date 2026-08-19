// Rock Paper Scissors panel — live during rps_1v1 matches, and as a disabled
// preview while queueing for rps_1v1 (fix 2).
import { chooseRps } from "../game/rps";
import { useLobby } from "../hooks/useLobby";

const CHOICES = [
  { choice: 0, title: "Rock", glyph: "✊" },
  { choice: 1, title: "Paper", glyph: "✋" },
  { choice: 2, title: "Scissors", glyph: "✌" },
];

export default function RpsPanel() {
  const st = useLobby();
  const active = st.gameMode === "rps";
  const preview = st.gameMode === "rps_preview";
  if (!active && !preview) return null;

  return (
    <div id="rps-panel">
      <p id="rps-status">{st.rpsStatus}</p>
      <div id="rps-buttons">
        {CHOICES.map((c) => (
          <button
            key={c.choice}
            className="rps-btn"
            data-choice={c.choice}
            title={c.title}
            disabled={!st.rpsButtonsEnabled}
            onClick={() => chooseRps(c.choice)}
          >
            {c.glyph}
          </button>
        ))}
      </div>
      <p id="rps-score">{st.rpsScore}</p>
    </div>
  );
}
