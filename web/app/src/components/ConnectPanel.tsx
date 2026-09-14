// Login and session controls. Provider logins return through an HttpOnly
// browser cookie; only dev/guest/native flows ever expose a token to JS.
import { useEffect, useState } from "react";
import { fetchAuthConfig, fetchSession } from "../api";
import {
  connectWithSession,
  connectWithToken,
  disconnect,
  reconnect,
  signout,
} from "../lobby/client";
import { jwtSub, log, setStatus, state } from "../lobby/store";
import type { AuthConfig } from "../types";
import { useLobby } from "../hooks/useLobby";

const PROVIDER_LABELS: Record<string, string> = { discord: "Discord", au2143: "au.2143.me" };


export default function ConnectPanel() {
  const st = useLobby();
  const [serverBase, setServerBase] = useState<string>(() =>
    location.protocol.startsWith("http") ? location.origin : "http://localhost:8080"
  );
  const [cfg, setCfg] = useState<AuthConfig | null>(null);
  const [steamId, setSteamId] = useState<string>("76561198000000001");
  const [guestBusy, setGuestBusy] = useState(false);

  useEffect(() => {
    const initialBase = serverBase.trim().replace(/\/+$/, "");
    void Promise.all([fetchAuthConfig(), fetchSession(initialBase)]).then(([config, session]) => {
      setCfg(config);
      if (config) {
        state.rankedQueueEnabled = config.ranked_queue_enabled;
      }
      if (session) {
        void connectWithSession(initialBase, session);
      }
    }).catch((e) => {
      log("sys", "session startup failed: " + (e as Error).message);
    });
    // Startup is intentionally tied to the initial base only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const base = serverBase.trim().replace(/\/+$/, "");

  const showSteam = !!cfg && cfg.providers.includes("steam");
  const showDevPanel = cfg ? cfg.dev_mode : true;
  const showGuest = !!cfg && cfg.guest_login;
  const showConnect = !!cfg && cfg.dev_mode;
  const extraProviders = cfg ? cfg.providers.filter((p) => p !== "steam") : [];
  const signedIn = !!st.connected;
  const showSignout = !!signedIn || !!st.playerId || !!st.lastAuthMode;
  const canReconnect = !!st.lastAuthMode && (st.lastAuthMode === "cookie" || !!st.lastToken) && !st.connected;
  const canLinkDiscord =
    signedIn && st.authProvider === "steam" && !!cfg?.providers.includes("discord");

  async function doConnect() {
    if (!base) {
      setStatus("Error: server URL is required", "err");
      return;
    }
    let token: string | null = null;
    if (!steamId.trim()) {
      setStatus("Error: server URL and steam ID are required", "err");
      return;
    }
    setStatus("Fetching test token…");
    log("sys", "POST " + base + "/auth/test-token");
    try {
      const resp = await fetch(base + "/auth/test-token", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ steam_id: Number(steamId.trim()) }),
      });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const body = await resp.json();
      token = body.token as string;
      log("sys", "got test session token");
    } catch (e) {
      setStatus("Error: test-token request failed — " + (e as Error).message, "err");
      log("sys", "test-token failed: " + (e as Error).message);
      return;
    }
    await connectWithToken(base, token);
  }

  async function doGuest() {
    if (!base) {
      setStatus("Error: server URL is required", "err");
      return;
    }
    setGuestBusy(true);
    setStatus("Minting guest account…");
    log("sys", "POST " + base + "/auth/guest");
    try {
      const resp = await fetch(base + "/auth/guest", { method: "POST" });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const body = await resp.json();
      const token: string | undefined = body.token;
      if (!token) throw new Error("no token in response");
      log("sys", "got guest session token");
      await connectWithToken(base, token);
    } catch (e) {
      setStatus("Error: guest account failed — " + (e as Error).message, "err");
      log("sys", "guest account failed: " + (e as Error).message);
    } finally {
      setGuestBusy(false);
    }
  }

  function steamLogin() {
    location.href = base + "/auth/steam/login?return_to=/";
  }

  function extraLogin(provider: string) {
    location.href = base + "/auth/" + encodeURIComponent(provider) + "/login?return_to=/";
  }

  function linkDiscord() {
    location.href = base + "/auth/discord/login?return_to=/link";
  }

  const sub = st.playerId || jwtSub(st.token);

  return (
    <section>
      <label>Server URL</label>
      <input
        value={serverBase}
        size={30}
        onChange={(e) => setServerBase(e.target.value)}
        disabled={signedIn}
      />
      {showSignout && (
        <p className="sys">
          Player ID: <span className="in">{sub ?? "—"}</span>{" "}
          {st.displayName && st.displayName !== "Unknown" ? `(${st.displayName})` : ""}{" "}
          <button onClick={() => void signout()}>Sign out</button>
        </p>
      )}
      {showSteam && !signedIn && (
        <div>
          <button className="primary" onClick={steamLogin}>
            Sign in with Steam
          </button>
        </div>
      )}
      {extraProviders.length > 0 && !signedIn && (
        <div>
          {extraProviders.map((p) => (
            <button key={p} className="primary" onClick={() => extraLogin(p)}>
              Sign in with {PROVIDER_LABELS[p] || p}
            </button>
          ))}
        </div>
      )}
      {canLinkDiscord && (
        <div>
          <button className="primary" onClick={linkDiscord}>
            Link Discord
          </button>
        </div>
      )}
      {showDevPanel && !signedIn && (
        <div>
          <label>Steam ID (dev test-token; server must run with AUTH_DEV_MODE=true)</label>
          <input value={steamId} size={20} onChange={(e) => setSteamId(e.target.value)} />
        </div>
      )}
      {showConnect && !signedIn && (
        <button className="primary" onClick={() => void doConnect()}>
          Connect
        </button>
      )}
      {showGuest && !signedIn && (
        <button className="primary" disabled={guestBusy} onClick={() => void doGuest()}>
          No account
        </button>
      )}
      {canReconnect && (
        <button className="primary" onClick={reconnect}>
          Reconnect
        </button>
      )}
      {signedIn && <button onClick={disconnect}>Disconnect</button>}
    </section>
  );
}
