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

/// Interstitial shown before handing off to the Steam client: a Steam-styled
/// page with a single Join button. The `__STEAM_URI__` placeholder appears
/// once (the button href) and `.replace` swaps it.
const INTERSTITIAL: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Join the Steam lobby</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; min-height: 100vh; display: grid; place-items: center;
         background: #171a21; color: #c7d5e0;
         font-family: "Motiva Sans", Arial, Helvetica, sans-serif; }
  .card { text-align: center; padding: 2rem; }
  h1 { color: #ffffff; font-size: 1.4rem; font-weight: 400;
       letter-spacing: 0.03em; margin: 0 0 2rem; }
  a.btn { display: inline-block; padding: 0.8rem 3.5rem; border-radius: 2px;
          background: linear-gradient(to bottom, #75b022, #588a1b);
          color: #ffffff; font-size: 1.1rem; text-decoration: none;
          text-shadow: 0 1px 0 rgba(0, 0, 0, 0.4); }
  a.btn:hover { filter: brightness(1.1); }
</style>
</head>
<body>
  <main class="card">
    <h1>Join the Steam lobby</h1>
    <a class="btn" href="__STEAM_URI__">Join</a>
  </main>
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
