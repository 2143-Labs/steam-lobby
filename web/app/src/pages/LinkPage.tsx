import { useEffect, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ApiError, confirmLinkIntent, createLinkIntent, fetchSession } from "../api";
import type { LinkIntentResponse, SessionInfo } from "../types";

type LinkStep = "loading" | "confirm" | "saving" | "done" | "error";

export default function LinkPage({ nativeComplete = false }: { nativeComplete?: boolean }) {
  const [params] = useSearchParams();
  const [step, setStep] = useState<LinkStep>(nativeComplete ? "done" : "loading");
  const [session, setSession] = useState<SessionInfo | null>(null);
  const [intent, setIntent] = useState<LinkIntentResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const started = useRef(false);
  const nativeIntentId = nativeComplete ? params.get("intent_id") : null;

  useEffect(() => {
    if (nativeComplete || started.current) return;
    started.current = true;
    void fetchSession().then(async (live) => {
      if (!live) throw new ApiError(401, "Sign in with Steam before linking Discord");
      setSession(live);
      const created = await createLinkIntent(live.csrf_token);
      setIntent(created);
      setStep("confirm");
    }).catch((reason: unknown) => {
      setError(reason instanceof Error ? reason.message : "Unable to prepare Discord link");
      setStep("error");
    });
  }, [nativeComplete]);

  async function confirm() {
    if (!session || !intent) return;
    setStep("saving");
    try {
      await confirmLinkIntent(intent.intent_id, session.csrf_token);
      setStep("done");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to link Discord");
      setStep("error");
    }
  }

  if (nativeComplete) {
    return (
      <section>
        <h2>Discord link ready</h2>
        {nativeIntentId ? (
          <p>Return to the game client to confirm this Discord link.</p>
        ) : (
          <p className="err">The link callback did not include an intent.</p>
        )}
        <p><Link to="/">← Lobby</Link></p>
      </section>
    );
  }

  return (
    <section>
      <h2>Link Discord</h2>
      {step === "loading" && <p>Preparing link…</p>}
      {(step === "confirm" || step === "saving") && intent && (
        <>
          <p>
            Link Discord account <strong>{intent.discord_display_name}</strong> to your Steam-backed
            ranked account?
          </p>
          <button className="primary" disabled={step === "saving"} onClick={() => void confirm()}>
            {step === "saving" ? "Linking…" : "Confirm link"}
          </button>
          <Link to="/">Cancel</Link>
        </>
      )}
      {step === "done" && (
        <>
          <p className="ok">Discord linked.</p>
          <p><Link to="/">Return to lobby</Link></p>
        </>
      )}
      {step === "error" && (
        <>
          <p className="err">{error}</p>
          <p><Link to="/">Return to lobby</Link></p>
        </>
      )}
    </section>
  );
}
