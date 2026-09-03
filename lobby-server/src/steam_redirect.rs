//! Steam protocol hand-off: GET /steam/{action}/{params} serves an interstitial
//! that launches `steam://{action}/{params}`. Only allowlisted (action, app id)
//! combos resolve — everything else 404s; extend ALLOWLIST to add more.
use axum::extract::Path;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};

/// Allowlisted (steam action, permitted app ids). The hand-off target is
/// `steam://{action}/{app}/{…params}`. Extend this list; nothing else changes.
const ALLOWLIST: &[(&str, &[u32])] = &[("joinlobby", &[357190])];

/// Validate (action, params) against the allowlist and return the steam URI.
/// `None` = reject with 404. Rules: action must be allowlisted; params[0] must
/// parse as u32 and be in that action's app list; every further param must
/// parse as u64. No param-count rule — Steam rejects malformed URIs itself.
fn resolve_redirect(action: &str, params: &[&str]) -> Option<String> {
    let allowed_apps = ALLOWLIST.iter().find(|(a, _)| *a == action)?.1;
    let app: u32 = params.first()?.parse().ok()?;
    if !allowed_apps.contains(&app) {
        return None;
    }
    for p in &params[1..] {
        p.parse::<u64>().ok()?;
    }
    Some(format!("steam://{action}/{}", params.join("/")))
}

/// GET /steam/{action}/{params…} → interstitial page that launches
/// `steam://{action}/{params…}`. Anything not on the allowlist 404s.
pub async fn steam_redirect(Path(rest): Path<String>) -> Response {
    let rest = rest.trim_matches('/');
    let Some((action, params)) = rest.split_once('/') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let params: Vec<&str> = params.split('/').collect();
    let Some(uri) = resolve_redirect(action, &params) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    debug_assert!(uri
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == ':'));
    let html = INTERSTITIAL.replace("__STEAM_URI__", &uri);
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Html(html),
    )
        .into_response()
}

/// Interstitial shown before handing off to the Steam client. The
/// `__STEAM_URI__` placeholder appears exactly three times (button href,
/// visible `<code>`, auto-launch script) and `.replace` swaps all three. The
/// relative "Back to home" link keeps this page working on pvp.john2143.com
/// and the pvp-{N}.john2143.com PR previews alike.
const INTERSTITIAL: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Opening Steam…</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; min-height: 100vh; display: grid; place-items: center;
         background: #0f1115; color: #e6e8ee;
         font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
  .card { max-width: 32rem; text-align: center; padding: 2rem; }
  h1 { font-size: 1.4rem; margin: 0 0 1rem; }
  p { color: #9aa1ad; line-height: 1.5; }
  code { color: #c9d1de; word-break: break-all; }
  a.btn { display: inline-block; margin: 1rem 0; padding: 0.7rem 1.6rem;
          background: #1b6ac9; color: #fff; border-radius: 8px;
          text-decoration: none; font-weight: 600; }
  a.btn:hover { background: #2a7de0; }
  a.home { color: #9aa1ad; font-size: 0.85rem; }
</style>
</head>
<body>
  <main class="card">
    <h1>Opening Steam…</h1>
    <p>If Steam does not open, click the button — most browsers only allow a
       click to hand off to another application.</p>
    <a class="btn" href="__STEAM_URI__">Open in Steam</a>
    <p><code>__STEAM_URI__</code></p>
    <p><a class="home" href="/">Back to home</a></p>
  </main>
  <script>
    // Auto-attempt the hand-off shortly after paint; the button above is the
    // reliable user-gesture fallback if the browser blocks this navigation.
    window.setTimeout(function () { window.location.href = "__STEAM_URI__"; }, 500);
  </script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::resolve_redirect;

    #[test]
    fn resolve_redirect_table() {
        let cases: &[(&str, &[&str], Option<&str>)] = &[
            // canonical joinlobby URL from the ask
            (
                "joinlobby",
                &["357190", "109775244080946091", "0"],
                Some("steam://joinlobby/357190/109775244080946091/0"),
            ),
            // 2-segment form (lobby id without "steamid optional")
            (
                "joinlobby",
                &["357190", "109775244080946091"],
                Some("steam://joinlobby/357190/109775244080946091"),
            ),
            // app only — no param-count rule
            (
                "joinlobby",
                &["357190"],
                Some("steam://joinlobby/357190"),
            ),
            // action not yet allowlisted
            ("run", &["357190"], None),
            // app not in the action's list
            ("joinlobby", &["480", "1"], None),
            // non-numeric lobby param
            ("joinlobby", &["357190", "abc"], None),
            // u64 overflow on lobby param
            ("joinlobby", &["357190", "18446744073709551616"], None),
            // u32 overflow on app
            ("joinlobby", &["4294967296"], None),
            // missing app
            ("joinlobby", &[], None),
            // empty action
            ("", &["357190"], None),
        ];
        for (action, params, expected) in cases {
            assert_eq!(
                resolve_redirect(action, params),
                expected.map(str::to_owned),
                "action={action:?}, params={params:?}"
            );
        }
    }
}
