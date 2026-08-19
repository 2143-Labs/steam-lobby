// Status line + protocol event log (auto-scrolls like the demo).
import { useEffect, useRef } from "react";
import { useLobby } from "../hooks/useLobby";

export default function StatusBar() {
  const st = useLobby();
  const logRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [st.logLines.length]);

  return (
    <>
      <section>
        <label>
          Status:{" "}
          <span id="status" className={st.statusCls}>
            {st.statusText}
          </span>
        </label>
      </section>
      <section>
        <label>Event log</label>
        <div id="log" ref={logRef}>
          {st.logLines.map((line, i) => (
            <div key={i} className={line.kind}>
              {line.text}
            </div>
          ))}
        </div>
      </section>
    </>
  );
}
