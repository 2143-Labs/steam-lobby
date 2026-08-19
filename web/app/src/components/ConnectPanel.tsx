// Login panel — port of the demo's connect section (web/index.html:55-74,
// 185-219, 309-347, 411-468, 1070-1072). The login options are gated by
// GET /auth/config: the Steam button only when a public origin is configured,
// the dev test-token field only when AUTH_DEV_MODE is on. If the config is
// unreachable (offline / file://), show only the dev panel.
// Fix (1): a Reconnect button appears after a Disconnect (the session token
// stays valid server-side, JWT TTL 86400s) and reuses the last base+token.
import { useEffect, useState } from "react";
import { fetchAuthConfig } from "../api";
import { connectWithToken, disconnect, reconnect, signout } from "../lobby/client";
import { jwtSub, log, setStatus } from "../lobby/store";
import type { AuthConfig } from "../types";
import { useLobby } from "../hooks/useLobby";

const PROVIDER_LABELS: Record<string, string> = { discord: "Discord", au2143: "au.2143.me" };

// Consume an OpenID-issued fragment token (#token=…) if present, then strip it
// from the URL so it isn't re-sent or stored in history.
function consumeFragmentToken(): string | null {
  if (location.hash.startsWith("#token=")) {
    const token = location.hash.slice(7);
    history.replaceState(null, "", location.pathname + location.search);
    return token;
  }
  return null;
}

export default function ConnectPanel() {
  const st = useLobby();
  const [serverBase, setServerBase] = useState<string>(() =>
    location.protocol.startsWith("http") ? location.origin : "http://localhost:8080"
  );
  const [cfg, setCfg] = useState<AuthConfig | null>(null);
  const [steamId, setSteamId] = useState<string>("76561198000000001");
  const [fragmentToken] = useState<string | null>(consumeFragmentToken);
  const [guestBusy, setGuestBusy] = useState(false);

  useEffect(() => {
    const initialBase = serverBase.trim().replace(/\/+$/, "");
    void fetchAuthConfig().then((c) => {
      setCfg(c);
      // A Steam login redirected back with a #token= fragment — complete the
      // sign-in automatically (works even when the config is unreachable).
      if (fragmentToken) {
        void connectWithToken(initialBase, fragmentToken);
      }
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const base = serverBase.trim().replace(/\/+$/, "");

  const showSteam = !!cfg && cfg.providers.includes("steam");
  const showDevPanel = cfg ? cfg.dev_mode : true; // config unreachable → dev panel only
  const showGuest = !!cfg && cfg.guest_login;
  const showConnect = !!cfg && (cfg.dev_mode || !!fragmentToken);
  const extraProviders = cfg ? cfg.providers.filter((p) => p !== "steam") : [];
  const signedIn = !!st.connected;
  const showSignout = !!signedIn || !!st.playerId;
  const canReconnect = !!st.lastToken && !st.connected;

  async function doConnect() {
    if (!base) {
      setStatus("Error: server URL is required", "err");
      return;
    }
    let token: string | null = fragmentToken;
    if (token) {
      setStatus("Using Steam login token");
      log("sys", "using session token from Steam OpenID login (fragment)");
    } else {
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
          body: JSON.stringify({ steam_id: Number(steamId.trim()) }), // dev-only: test-token body is a plain u64
        });
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        const body = await resp.json();
        token = body.token as string;
        log("sys", "got token (first 12 chars): " + token.slice(0, 12) + "…");
      } catch (e) {
        setStatus("Error: test-token request failed — " + (e as Error).message, "err");
        log("sys", "test-token failed: " + (e as Error).message);
        return;
      }
    }
    await connectWithToken(base, token as string);
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
      log("sys", "got guest token (first 12 chars): " + token.slice(0, 12) + "…");
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
      {showSignout && sub && (
        <p className="sys">
          Player ID: <span className="in">{sub}</span>{" "}
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
