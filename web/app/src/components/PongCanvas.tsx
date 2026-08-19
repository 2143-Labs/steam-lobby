// The pong canvas — registers itself with the game module so the imperative
// loops can draw (the game keeps the last frame after stopGame).
// The canvas is CONDITIONALLY rendered (only while a pong/practice game is
// active), so registration uses a callback ref that fires on every attach —
// a `useEffect` with [] deps would run once at app mount when no canvas
// exists and never re-run, leaving the game module's context null forever.
import { useCallback } from "react";
import { computeTargetFromOffset, setCanvas, PONG_H } from "../game/pong";
import { useLobby } from "../hooks/useLobby";

export default function PongCanvas() {
  const st = useLobby();

  const ref = useCallback((el: HTMLCanvasElement | null) => {
    setCanvas(el);
  }, []);

  const showCanvas = st.gameMode === "practice" || st.gameMode === "pong";
  if (!showCanvas) return null;

  return (
    <canvas
      id="pong"
      ref={ref}
      width={640}
      height={PONG_H}
      onMouseMove={(e) => computeTargetFromOffset(e.nativeEvent.offsetY)}
    />
  );
}
