// The pong canvas — registers itself with the game module so the imperative
// loops can draw (the game keeps the last frame after stopGame).
import { useEffect, useRef } from "react";
import { computeTargetFromOffset, setCanvas, PONG_H } from "../game/pong";
import { useLobby } from "../hooks/useLobby";

export default function PongCanvas() {
  const st = useLobby();
  const ref = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    setCanvas(ref.current);
    return () => setCanvas(null);
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
