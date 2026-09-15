//! HTTP surface: health + embedded single-file app, the read-only stats API
//! (leaderboard + player profile), Steam OpenID login/callback, ticket auth,
//! logout, the internal gameserver result webhook, and the dev-only
//! test-token + mock creator endpoints.
use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::state::AppState;
use crate::steam_auth::ValidatedSession;
use lobby_core::traits::{MatchStore, PlayerStore};

// ── stats API response types (snake_case on the wire, mirror of web/app/src/types.ts) ──

#[derive(Serialize)]
pub struct LeaderboardRow {
    pub player_id: String,
    pub display_name: String,
    pub mu: f64,
    pub sigma: f64,
    pub rating: f64, // mu - 3*sigma, the display/matchmaking value
}

#[derive(Serialize)]
pub struct PlayerIdentity {
    pub provider: String,
    pub last_login_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
pub struct PlayerRating {
    pub game_mode: String,
    pub mu: f64,
    pub sigma: f64,
    pub rating: f64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
pub struct RecentMatch {
    pub match_token: String,
    pub game_mode: String,
    pub status: String,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    pub opponent_id: String,
    pub opponent_name: String,
    /// The viewer's perspective (flipped from the stored player_a perspective).
    pub outcome: Option<String>,
    pub mu_change: Option<f64>,
}

#[derive(Serialize)]
pub struct PlayerProfile {
    pub player_id: String,
    pub display_name: String,
    pub primary_provider: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub identities: Vec<PlayerIdentity>,
    pub ratings: Vec<PlayerRating>,
    pub recent_matches: Vec<RecentMatch>,
}

pub async fn health() -> &'static str {
    "ok"
}

/// The built single-file app (`web/app/dist/index.html`), embedded at build
/// time so it works from any CWD and inside the Docker image.
pub async fn index() -> Html<&'static str> {
    Html(include_str!("../../web/app/dist/index.html"))
}



#[derive(Deserialize)]
pub struct LoginQuery {
    return_to: Option<String>,
    native_handoff: Option<String>,
}

fn validate_return_to(return_to: &str, public_url: Option<&str>) -> bool {
    if return_to.starts_with('/') && !return_to.starts_with("//") && !return_to.starts_with("/\\") {
        return !return_to.contains('?') && !return_to.contains('#');
    }
    let Some(public_url)=public_url else { return false };
    if return_to.contains('#') { return false }
    match (url::Url::parse(return_to),url::Url::parse(public_url)) {
        (Ok(a),Ok(b)) => a.origin()==b.origin(), _ => false,
    }
}

fn cookie(headers:&HeaderMap,name:&str)->Option<String>{
    headers.get(header::COOKIE)?.to_str().ok()?.split(';').find_map(|part|{
        let (key,value)=part.trim().split_once('=')?;
        (key==name).then(||value.to_string())
    })
}

fn secure_cookie(state:&AppState)->bool {
    let Some(url)=state.config.public_url.as_deref().and_then(|u|url::Url::parse(u).ok()) else{return true};
    !(url.scheme()=="http" && url.host_str().is_some_and(|host| {
        host=="localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip|ip.is_loopback())
    }))
}

fn set_cookie(response:&mut Response,name:&str,value:&str,path:&str,max_age:u64,http_only:bool,state:&AppState){
    let mut raw=format!("{name}={value}; Path={path}; Max-Age={max_age}; SameSite=Lax");
    if http_only { raw.push_str("; HttpOnly"); }
    if secure_cookie(state){raw.push_str("; Secure");}
    if let Ok(value)=HeaderValue::from_str(&raw){response.headers_mut().append(header::SET_COOKIE,value);}
}

fn clear_cookie(response:&mut Response,name:&str,path:&str,state:&AppState){set_cookie(response,name,"",path,0,true,state)}

fn exact_public_origin(state:&AppState,headers:&HeaderMap)->bool{
    let Some(expected)=state.config.public_url.as_deref().and_then(|u|url::Url::parse(u).ok()).map(|u|u.origin().ascii_serialization()) else{return false};
    headers.get(header::ORIGIN).and_then(|v|v.to_str().ok())==Some(expected.as_str())
}

pub(crate) async fn revalidate_session(state:&AppState, session:&ValidatedSession)->Option<uuid::Uuid>{
    let claims=match session{ValidatedSession::Browser(c)|ValidatedSession::Native(c)=>c};
    let user_id=uuid::Uuid::parse_str(&claims.sub).ok()?;
    if state.store.get_token_version(user_id).await.ok()?!=claims.token_version{return None}
    match session{
        ValidatedSession::Browser(c)=>{
            let row=state.store.live_browser_session(c.sid,user_id).await.ok()??;
            let csrf=c.csrf.as_deref()?;
            (c.provider.as_deref()==Some(row.auth_provider.as_str())
                && row.csrf_hash==Sha256::digest(csrf.as_bytes()).to_vec()).then_some(user_id)
        }
        ValidatedSession::Native(c)=>state.store.live_native_session(c.sid,user_id).await.ok()?.is_some().then_some(user_id),
    }
}

pub(crate) async fn authenticate_token(state:&AppState, token:&str)->Option<ValidatedSession>{
    let session=state.steam_auth.validate_session_token(token).ok()?;
    revalidate_session(state,&session).await?;
    Some(session)
}

pub(crate) async fn authenticate_cookie_ws(state:&AppState,headers:&HeaderMap)->Option<ValidatedSession>{
    if !exact_public_origin(state,headers){return None}
    let token=cookie(headers,"lobby_session")?;
    let session=authenticate_token(state,&token).await?;
    matches!(session,ValidatedSession::Browser(_)).then_some(session)
}

async fn validate_browser_csrf(
    state: &AppState,
    headers: &HeaderMap,
    claims: &crate::steam_auth::SessionClaims,
) -> Option<()> {
    let csrf=headers.get("x-csrf-token").and_then(|v|v.to_str().ok())?;
    if claims.csrf.as_deref()!=Some(csrf){return None}
    let user_id=uuid::Uuid::parse_str(&claims.sub).ok()?;
    let row=state.store.live_browser_session(claims.sid,user_id).await.ok()??;
    if row.auth_provider!=claims.provider.as_deref()? || row.csrf_hash!=Sha256::digest(csrf.as_bytes()).to_vec(){return None}
    Some(())
}

pub(crate) async fn authenticate_headers(state:&AppState,headers:&HeaderMap,mutation:bool)->Option<ValidatedSession>{
    if let Some(token)=headers.get(header::AUTHORIZATION).and_then(|v|v.to_str().ok()).and_then(|v|v.strip_prefix("Bearer ")){
        // A bearer token is never ambient — the browser keeps it only inside
        // the HttpOnly cookie, so it cannot be replayed by a cross-site form.
        // CSRF therefore applies to cookie authentication alone; requiring it
        // here would break the documented bearer logout contract.
        return authenticate_token(state,token).await;
    }
    let token=cookie(headers,"lobby_session")?;
    if mutation && !exact_public_origin(state,headers){return None}
    let session=authenticate_token(state,&token).await?;
    let ValidatedSession::Browser(claims)=&session else{return None};
    if mutation {validate_browser_csrf(state,headers,claims).await?;}
    Some(session)
}

async fn authenticate_cookie(state:&AppState,headers:&HeaderMap)->Option<crate::steam_auth::SessionClaims>{
    let token=cookie(headers,"lobby_session")?;
    let ValidatedSession::Browser(claims)=authenticate_token(state,&token).await? else{return None};
    Some(claims)
}

async fn issue_browser(state:&AppState,user_id:uuid::Uuid,provider:&str)->Result<(String,String),()> {
    let csrf=crate::db::opaque_token();
    let sid=state.store.create_browser_session(user_id,provider,&crate::db::token_hash(&csrf),state.config.jwt_ttl_secs).await.map_err(|_|())?;
    let version=state.store.get_token_version(user_id).await.map_err(|_|())?;
    let token=state.steam_auth.generate_browser_token(user_id,sid,provider,&csrf,version,state.config.jwt_ttl_secs).map_err(|_|())?;
    Ok((token,csrf))
}

async fn issue_native(state:&AppState,user_id:uuid::Uuid)->Result<String,()> {
    let sid=state.store.create_native_session(user_id).await.map_err(|_|())?;
    let version=state.store.get_token_version(user_id).await.map_err(|_|())?;
    state.steam_auth.generate_native_token(user_id,sid,version).map_err(|_|())
}

async fn begin_login(state:&AppState,provider:&str,return_to:String,verifier:Option<String>)->Result<(String,String),Response>{
    if !validate_return_to(&return_to,state.config.public_url.as_deref()) {return Err((StatusCode::BAD_REQUEST,Json(serde_json::json!({"error":"invalid_return_to"}))).into_response())}
    if state.config.public_url.is_none(){return Err((StatusCode::BAD_REQUEST,Json(serde_json::json!({"error":"public_url_required"}))).into_response())}
    let login_state=crate::db::opaque_token();
    let nonce=crate::db::opaque_token();
    state.store.create_oauth_login_state(&login_state,&nonce,provider,&return_to,verifier.as_deref()).await.map_err(|_|StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    Ok((login_state,nonce))
}

pub async fn steam_login(State(state):State<Arc<AppState>>,Query(query):Query<LoginQuery>)->Response{
    let return_to=query.return_to.unwrap_or_else(||"/".into());
    let (login_state,nonce)=match begin_login(&state,"steam",return_to,None).await{Ok(v)=>v,Err(r)=>return r};
    let url=state.steam_auth.openid_redirect_url(state.config.public_url.as_deref().unwrap(),&login_state,"/");
    let mut response=Redirect::temporary(&url).into_response();
    set_cookie(&mut response,"oauth_login_nonce",&nonce,"/auth",600,true,&state); response
}

pub async fn steam_callback(State(state):State<Arc<AppState>>,headers:HeaderMap,Query(query):Query<HashMap<String,String>>)->Response{
    let state_param=query.get("state").cloned().unwrap_or_default();
    let nonce=cookie(&headers,"oauth_login_nonce").unwrap_or_default();
    let mut failure=StatusCode::UNAUTHORIZED.into_response();
    clear_cookie(&mut failure,"oauth_login_nonce","/auth",&state);
    let Some(stored)=state.store.consume_oauth_login_state(&state_param,&nonce,"steam").await.ok().flatten() else{return failure};
    let steam_id=match state.steam_auth.verify_openid(&query).await{Ok(v)=>v,Err(_)=>return failure};
    let display_name=state.steam_auth.get_player_summary(steam_id).await.unwrap_or_else(|_|"Unknown".into());
    let user_id=match state.store.find_or_create_user("steam",&steam_id.to_string(),&display_name,true).await{Ok(v)=>v,Err(_)=>return failure};
    let (token,_csrf)=match issue_browser(&state,user_id,"steam").await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    let mut response=Redirect::temporary(&stored.return_to).into_response();
    clear_cookie(&mut response,"oauth_login_nonce","/auth",&state);
    set_cookie(&mut response,"lobby_session",&token,"/",state.config.jwt_ttl_secs,true,&state); response
}

pub async fn auth_login(State(state):State<Arc<AppState>>,Path(provider):Path<String>,Query(query):Query<LoginQuery>)->Response{
    let Some(cfg)=state.auth_providers.get(&provider) else{return StatusCode::NOT_FOUND.into_response()};
    if state.config.steam_backed_accounts_only && provider!="discord" {return StatusCode::NOT_FOUND.into_response()}
    if state.config.steam_backed_accounts_only && provider=="discord" && let Some(handoff)=query.native_handoff {
        if state.store.peek_oauth_handoff(&handoff).await.ok().flatten().is_none(){return StatusCode::UNAUTHORIZED.into_response()}
        let Some(public)=state.config.public_url.as_deref() else{return StatusCode::SERVICE_UNAVAILABLE.into_response()};
        let callback=format!("{}/auth/{provider}/callback",public.trim_end_matches('/'));
        let url=crate::auth_providers::authorization_url(cfg,&callback,&handoff,None,"");
        return Redirect::temporary(&url).into_response();
    }
    let (verifier,challenge)=if cfg.use_pkce{let(v,c)=crate::auth_providers::pkce_pair();(Some(v),c)}else{(None,String::new())};
    let return_to=query.return_to.unwrap_or_else(||"/".into());
    let (login_state,nonce)=match begin_login(&state,&provider,return_to,verifier.clone()).await{Ok(v)=>v,Err(r)=>return r};
    let callback=format!("{}/auth/{provider}/callback",state.config.public_url.as_deref().unwrap().trim_end_matches('/'));
    let url=crate::auth_providers::authorization_url(cfg,&callback,&login_state,verifier.as_deref(),&challenge);
    let mut response=Redirect::temporary(&url).into_response(); set_cookie(&mut response,"oauth_login_nonce",&nonce,"/auth",600,true,&state); response
}

async fn provider_identity(state:&AppState,provider:&str,code:&str,verifier:Option<&str>)->Option<(String,String,serde_json::Value)>{
    let cfg=state.auth_providers.get(provider)?;
    let public=state.config.public_url.as_deref()?;
    let callback=format!("{}/auth/{provider}/callback",public.trim_end_matches('/'));
    let mut form=vec![("grant_type","authorization_code"),("code",code),("redirect_uri",callback.as_str()),("client_id",cfg.client_id.as_str()),("client_secret",cfg.client_secret.as_str())];
    if let Some(v)=verifier{form.push(("code_verifier",v));}
    let token:serde_json::Value=state.http.post(&cfg.token_endpoint).form(&form).send().await.ok()?.error_for_status().ok()?.json().await.ok()?;
    let access=token["access_token"].as_str()?;
    let info:serde_json::Value=state.http.get(&cfg.userinfo_endpoint).bearer_auth(access).send().await.ok()?.error_for_status().ok()?.json().await.ok()?;
    Some((info[&cfg.id_field].as_str()?.to_string(),crate::auth_providers::userinfo_name(&info,cfg),info))
}

pub async fn auth_callback(State(state):State<Arc<AppState>>,Path(provider):Path<String>,headers:HeaderMap,Query(query):Query<HashMap<String,String>>)->Response{
    if state.auth_providers.get(&provider).is_none(){return StatusCode::NOT_FOUND.into_response()}
    let state_param=query.get("state").cloned().unwrap_or_default();
    let code=query.get("code").cloned().unwrap_or_default();
    if state.config.steam_backed_accounts_only && provider=="discord"
        && state.store.peek_oauth_handoff(&state_param).await.ok().flatten().is_some()
    {
        let Some((uid,name,_))=provider_identity(&state,&provider,&code,None).await else{return StatusCode::UNAUTHORIZED.into_response()};
        let intent=match state.store.complete_native_handoff(&state_param,&uid,&name).await{
            Ok(Some(v))=>v,
            Ok(None)=>return StatusCode::UNAUTHORIZED.into_response(),
            Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        return Redirect::temporary(&format!("/link/native-complete?intent_id={intent}")).into_response();
    }
    let nonce=cookie(&headers,"oauth_login_nonce").unwrap_or_default();
    let mut failure=StatusCode::UNAUTHORIZED.into_response();
    clear_cookie(&mut failure,"oauth_login_nonce","/auth",&state);
    let Some(stored)=state.store.consume_oauth_login_state(&state_param,&nonce,&provider).await.ok().flatten() else{return failure};
    let Some((provider_uid,display_name,info))=provider_identity(&state,&provider,&code,stored.code_verifier.as_deref()).await else{return failure};
    if state.config.steam_backed_accounts_only {
        let mut response=match state.store.login_linked_account("discord",&provider_uid,&display_name).await {
            Ok(Some(owner))=>match issue_browser(&state,owner,"discord").await {
                Ok((token,_))=>{let mut r=Redirect::temporary(&stored.return_to).into_response();set_cookie(&mut r,"lobby_session",&token,"/",state.config.jwt_ttl_secs,true,&state);r},
                Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            },
            Ok(None)=>match state.store.create_provider_proof(&provider_uid,&display_name).await {
                Ok(proof)=>{let mut r=Redirect::temporary("/link").into_response();set_cookie(&mut r,"discord_link_proof",&proof,"/api/link",300,true,&state);r},
                Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            },
            Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        clear_cookie(&mut response,"oauth_login_nonce","/auth",&state); return response
    }
    let user_id=match state.store.find_or_create_user(&provider,&provider_uid,&display_name,true).await{Ok(v)=>v,Err(_)=>return failure};
    if provider=="au2143" {let admin=info["groups"].as_array().is_some_and(|g|g.iter().any(|v|v.as_str()==Some("pvp_admin")));let _=state.store.set_admin_flag(user_id,admin).await;}
    let (token,_)=match issue_browser(&state,user_id,&provider).await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    let mut response=Redirect::temporary(&stored.return_to).into_response();clear_cookie(&mut response,"oauth_login_nonce","/auth",&state);set_cookie(&mut response,"lobby_session",&token,"/",state.config.jwt_ttl_secs,true,&state);response
}

#[derive(Deserialize)] pub struct TicketAuthBody{ticket:String}

async fn ticket_token(state:&AppState,ip:std::net::IpAddr,ticket:&str)->Result<String,Response>{
    if !state.ticket_limiter.check(ip){return Err((StatusCode::TOO_MANY_REQUESTS,Json(serde_json::json!({"error":"rate_limited"}))).into_response())}
    if ticket.is_empty()||ticket.len()>8192||ticket.len()%2!=0||!ticket.bytes().all(|b|b.is_ascii_hexdigit()){return Err((StatusCode::BAD_REQUEST,Json(serde_json::json!({"error":"malformed_ticket"}))).into_response())}
    let steam_id=match state.steam_auth.verify_ticket(ticket,"matchmaking").await{Ok(v)=>v,Err(lobby_core::error::LobbyError::SteamAuthFailed(e)) if e=="provider_unavailable"=>return Err((StatusCode::SERVICE_UNAVAILABLE,Json(serde_json::json!({"error":"provider_unavailable"}))).into_response()),Err(_)=>return Err(StatusCode::UNAUTHORIZED.into_response())};
    let user_id=state.store.find_or_create_user("steam",&steam_id.to_string(),"",true).await.map_err(|_|StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    issue_native(state,user_id).await.map_err(|_|StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub async fn ticket_auth(State(state):State<Arc<AppState>>,ConnectInfo(ip):ConnectInfo<std::net::SocketAddr>,Json(body):Json<TicketAuthBody>)->Response{
    match ticket_token(&state,ip.ip(),&body.ticket).await{Ok(token)=>(StatusCode::OK,Json(serde_json::json!({"token":token}))).into_response(),Err(r)=>r}
}
pub async fn api_ticket(State(state):State<Arc<AppState>>,ConnectInfo(ip):ConnectInfo<std::net::SocketAddr>,Json(body):Json<TicketAuthBody>)->Response{
    match ticket_token(&state,ip.ip(),&body.ticket).await{Ok(token)=>(StatusCode::OK,Json(serde_json::json!({"access_token":token,"expires_in":3600}))).into_response(),Err(r)=>r}
}

#[derive(Deserialize)] pub struct TestTokenBody{steam_id:u64}
pub async fn test_token(State(state):State<Arc<AppState>>,ConnectInfo(ip):ConnectInfo<std::net::SocketAddr>,Json(body):Json<TestTokenBody>)->Response{
    if !state.config.auth_dev_mode||state.config.steam_backed_accounts_only{return StatusCode::NOT_FOUND.into_response()}
    if !state.test_token_limiter.check(ip.ip()){return StatusCode::TOO_MANY_REQUESTS.into_response()}
    let user=match state.store.find_or_create_user("steam",&body.steam_id.to_string(),"",false).await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    let (token,_)=match issue_browser(&state,user,"test").await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    (StatusCode::OK,Json(serde_json::json!({"token":token}))).into_response()
}

pub async fn guest_token(State(state):State<Arc<AppState>>,ConnectInfo(ip):ConnectInfo<std::net::SocketAddr>)->Response{
    if state.config.steam_backed_accounts_only{return StatusCode::NOT_FOUND.into_response()}
    if !state.guest_token_limiter.check(ip.ip()){return StatusCode::TOO_MANY_REQUESTS.into_response()}
    let name=format!("Guest-{:06x}",(uuid::Uuid::new_v4().as_u128()&0xffffff)as u32);
    let user=match state.store.create_guest_user(&name).await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    let (token,_)=match issue_browser(&state,user,"guest").await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    (StatusCode::OK,Json(serde_json::json!({"token":token}))).into_response()
}

pub async fn api_session(State(state):State<Arc<AppState>>,headers:HeaderMap)->Response{
    let Some(claims)=authenticate_cookie(&state,&headers).await else{return no_store(StatusCode::UNAUTHORIZED.into_response())};
    let user=uuid::Uuid::parse_str(&claims.sub).unwrap();
    let name=state.store.get_display_name(user).await.ok().flatten().unwrap_or_else(||"Unknown".into());
    let body=Json(serde_json::json!({"user_id":user,"display_name":name,"auth_provider":claims.provider.unwrap_or_default(),"csrf_token":claims.csrf.unwrap_or_default()})).into_response(); no_store(body)
}
fn no_store(mut response:Response)->Response{response.headers_mut().insert(header::CACHE_CONTROL,HeaderValue::from_static("no-store"));response}

pub async fn logout(State(state):State<Arc<AppState>>,headers:HeaderMap)->Response{
    let Some(session)=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let (browser,result)=match session{
        ValidatedSession::Browser(c)=>{let user=uuid::Uuid::parse_str(&c.sub).unwrap();(true,state.store.revoke_browser_session(c.sid,user).await)},
        ValidatedSession::Native(c)=>{let user=uuid::Uuid::parse_str(&c.sub).unwrap();(false,state.store.revoke_native_session(c.sid,user).await)},
    };
    if result.is_err(){return StatusCode::INTERNAL_SERVER_ERROR.into_response()}
    let mut response=StatusCode::NO_CONTENT.into_response();if browser{clear_cookie(&mut response,"lobby_session","/",&state)}response
}
pub async fn logout_all(State(state):State<Arc<AppState>>,headers:HeaderMap)->Response{
    let Some(session)=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let claims=match session{ValidatedSession::Browser(c)|ValidatedSession::Native(c)=>c};let user=uuid::Uuid::parse_str(&claims.sub).unwrap();
    if state.store.revoke_all_sessions(user).await.is_err(){return StatusCode::INTERNAL_SERVER_ERROR.into_response()}
    let mut response=StatusCode::NO_CONTENT.into_response();clear_cookie(&mut response,"lobby_session","/",&state);response
}

#[derive(Deserialize)] pub struct IntentBody{intent_id:uuid::Uuid}
pub async fn browser_link_intent(State(state):State<Arc<AppState>>,headers:HeaderMap)->Response{
    let Some(ValidatedSession::Browser(c))=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let user=uuid::Uuid::parse_str(&c.sub).unwrap();
    let Some(proof)=cookie(&headers,"discord_link_proof") else{return StatusCode::UNAUTHORIZED.into_response()};
    let result=state.store.create_browser_link_intent(user,c.sid,&proof).await;
    let mut response=match result{
        Ok(Ok((id,name)))=>(StatusCode::CREATED,Json(serde_json::json!({"intent_id":id,"discord_display_name":name}))).into_response(),
        Ok(Err(crate::db::LinkIntentError::Expired))=>StatusCode::GONE.into_response(),
        Ok(Err(crate::db::LinkIntentError::Replayed))=>StatusCode::CONFLICT.into_response(),
        Ok(Err(crate::db::LinkIntentError::SessionMismatch))=>StatusCode::UNAUTHORIZED.into_response(),
        Ok(Err(crate::db::LinkIntentError::SteamRequired))=>StatusCode::FORBIDDEN.into_response(),
        Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    clear_cookie(&mut response,"discord_link_proof","/api/link",&state);
    response
}
pub async fn browser_link_confirm(State(state):State<Arc<AppState>>,headers:HeaderMap,Json(body):Json<IntentBody>)->Response{
    let Some(ValidatedSession::Browser(c))=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let user=uuid::Uuid::parse_str(&c.sub).unwrap();
    match state.store.confirm_discord_link(body.intent_id,user,"browser",c.sid).await{Ok(Ok(()))=>StatusCode::NO_CONTENT.into_response(),Ok(Err("expired"))=>StatusCode::GONE.into_response(),Ok(Err(_))=>StatusCode::CONFLICT.into_response(),Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response()}
}
pub async fn native_link_start(State(state):State<Arc<AppState>>,headers:HeaderMap)->Response{
    let Some(ValidatedSession::Native(c))=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let Some(_cfg)=state.auth_providers.get("discord") else{return StatusCode::NOT_FOUND.into_response()};
    let Some(public)=state.config.public_url.as_deref() else{return StatusCode::SERVICE_UNAVAILABLE.into_response()};
    let handoff=match state.store.create_oauth_handoff(c.sid).await{Ok(v)=>v,Err(_)=>return StatusCode::INTERNAL_SERVER_ERROR.into_response()};
    let url=format!("{}/auth/discord/login?native_handoff={}",public.trim_end_matches('/'),url::form_urlencoded::byte_serialize(handoff.as_bytes()).collect::<String>());
    (StatusCode::CREATED,Json(serde_json::json!({"authorization_url":url}))).into_response()
}
pub async fn native_link_confirm(State(state):State<Arc<AppState>>,headers:HeaderMap,Json(body):Json<IntentBody>)->Response{
    let Some(ValidatedSession::Native(c))=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let user=uuid::Uuid::parse_str(&c.sub).unwrap();
    match state.store.confirm_discord_link(body.intent_id,user,"native",c.sid).await{Ok(Ok(()))=>StatusCode::NO_CONTENT.into_response(),Ok(Err("expired"))=>StatusCode::GONE.into_response(),Ok(Err(_))=>StatusCode::CONFLICT.into_response(),Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response()}
}
pub async fn revoke_link(State(state):State<Arc<AppState>>,headers:HeaderMap)->Response{
    let Some(ValidatedSession::Browser(c))=authenticate_headers(&state,&headers,true).await else{return StatusCode::UNAUTHORIZED.into_response()};
    let user=uuid::Uuid::parse_str(&c.sub).unwrap();
    if !state.store.has_account(user,"steam").await.unwrap_or(false){return StatusCode::FORBIDDEN.into_response()}
    match state.store.revoke_discord_link(user).await{Ok(())=>StatusCode::NO_CONTENT.into_response(),Err(_)=>StatusCode::INTERNAL_SERVER_ERROR.into_response()}
}

#[cfg(test)]
mod tests {
    use super::validate_return_to;

    #[test]
    fn validate_return_to_table() {
        let cases: &[(&str, Option<&str>, bool)] = &[
            ("/dashboard", Some("https://lobby.example.com"), true),
            ("/dashboard", None, true),
            ("//evil.com/x", None, false),
            ("/\\evil.com", None, false),
            ("", None, false),
            ("javascript:alert(1)", None, false),
            ("/x?a=b", None, false),
            ("/x#frag", None, false),
            ("https://evil.com/x", Some("https://lobby.example.com"), false),
            ("https://lobby.example.com/cb", Some("https://lobby.example.com"), true),
            ("https://lobby.example.com/cb", None, false),
            ("https://lobby.example.com/cb?x=1", Some("https://lobby.example.com"), true),
            ("https://lobby.example.com/cb#frag", Some("https://lobby.example.com"), false),
        ];
        for (return_to, public_url, expected) in cases {
            assert_eq!(validate_return_to(return_to, *public_url), *expected);
        }
    }
}

#[derive(Deserialize)]
pub struct GameResultBody {
    #[serde(default)]
    pub winner: Option<uuid::Uuid>, // None = draw
}

/// The gameserver reports the match outcome. The URL itself is the
/// authentication: {token}/{secret} — 401 unless the secret matches.
pub async fn game_result(
    State(state): State<Arc<AppState>>,
    Path((token, secret)): Path<(String, String)>,
    Json(body): Json<GameResultBody>,
) -> impl IntoResponse {
    let m = match state.store.get_match(&token).await {
        Ok(Some(m)) => m,
        Ok(None) => return StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::warn!("game_result db error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    if m.result_secret.as_deref() != Some(secret.as_str()) {
        return StatusCode::UNAUTHORIZED;
    }
    match state
        .match_manager
        .resolve_from_gameserver(
            &token,
            body.winner,
            &state.store,
            &state.store,
            &state.store,
        )
        .await
    {
        Ok(outcome) => {
            tracing::info!("match {token} gameserver result resolved: {outcome:?}");
            crate::ws::notify_match_players(
                &state,
                &token,
                crate::ws::ServerMessage::MatchResult {
                    match_token: token.clone(),
                    outcome: serde_json::to_value(&outcome).unwrap(),
                },
            )
            .await;
            StatusCode::OK
        }
        Err(e) => {
            // wrong status (already resolved/disputed) or invalid winner
            tracing::warn!("game_result rejected for match {token}: {e}");
            StatusCode::CONFLICT
        }
    }
}

#[derive(Serialize)]
pub struct ModeInfo {
    pub name: String,
    pub game_type: lobby_core::types::ConnectionStrategy,
    /// False when this mode's queue is gated off — a NativeReport mode while
    /// RANKED_QUEUE_ENABLED=false. The web UI disables its queue button from
    /// this field instead of matching on a game name.
    pub queue_enabled: bool,
}

/// The modes the server actually runs — the demo populates its dropdown from this.
pub async fn modes(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({ "modes": state.game_modes.iter().map(|spec| ModeInfo {
            name: spec.id.to_owned(),
            game_type: spec.connection,
            queue_enabled: spec.authority != lobby_core::types::ResultAuthority::NativeReport
                || state.config.ranked_queue_enabled,
        }).collect::<Vec<_>>() }),
    )
}

/// GET /api/leaderboard/{game_mode} — all rated players for a mode, ordered
/// by rating (mu - 3*sigma). 404 for an unknown game_mode.
pub async fn api_leaderboard(
    State(state): State<Arc<AppState>>,
    Path(game_mode): Path<String>,
) -> Response {
    let known = state.game_modes.iter().any(|spec| spec.id == game_mode);
    if !known {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("unknown game_mode: {game_mode}") })),
        )
            .into_response();
    }
    match state.store.leaderboard_with_names(&game_mode).await {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|(player_id, display_name, mu, sigma)| LeaderboardRow {
                    player_id: player_id.to_string(),
                    display_name,
                    mu,
                    sigma,
                    rating: mu - 3.0 * sigma,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => {
            tracing::error!("leaderboard query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal" })),
            )
                .into_response()
        }
    }
}

/// GET /api/player/{player_id} — profile: linked accounts (provider + last
/// login only, never the provider_uid), per-game ratings, and recent match
/// history with outcomes in the viewer's perspective. 404 for an unknown id.
pub async fn api_player(
    State(state): State<Arc<AppState>>,
    Path(player_id): Path<String>,
) -> Response {
    let Ok(user_id) = player_id.parse::<uuid::Uuid>() else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown player" })),
        )
            .into_response();
    };
    let Ok(Some((display_name, primary_provider, created_at))) =
        state.store.player_profile(user_id).await
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown player" })),
        )
            .into_response();
    };

    let identities = match state.store.accounts_for_user(user_id).await {
        Ok(rows) => rows
            .into_iter()
            .map(|(provider, last_login_at)| PlayerIdentity {
                provider,
                last_login_at,
            })
            .collect(),
        Err(e) => {
            tracing::error!("identities query failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal" })),
            )
                .into_response();
        }
    };
    let ratings = match state.store.all_ratings_for_user(user_id).await {
        Ok(rows) => rows
            .into_iter()
            .map(|(game_mode, mu, sigma, last_updated)| PlayerRating {
                game_mode,
                mu,
                sigma,
                rating: mu - 3.0 * sigma,
                last_updated,
            })
            .collect(),
        Err(e) => {
            tracing::error!("ratings query failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal" })),
            )
                .into_response();
        }
    };
    let recent_matches = match state.store.recent_matches_for_user(user_id, 20).await {
        Ok(rows) => rows
            .into_iter()
            .map(|m| {
                // The stored outcome is player_a-perspective; flip when the
                // viewer was player_b (Win↔Loss), and use their mu change.
                let (outcome, mu_change) = if m.player_b == user_id {
                    let flipped = match m.outcome.as_deref() {
                        Some("Win") => Some("Loss".to_string()),
                        Some("Loss") => Some("Win".to_string()),
                        other => other.map(str::to_string),
                    };
                    (flipped, m.mu_change_b)
                } else {
                    (m.outcome, m.mu_change_a)
                };
                RecentMatch {
                    match_token: m.match_token,
                    game_mode: m.game_mode,
                    status: m.status,
                    started_at: m.started_at,
                    ended_at: m.ended_at,
                    opponent_id: m.opponent_id.to_string(),
                    opponent_name: m.opponent_name,
                    outcome,
                    mu_change,
                }
            })
            .collect(),
        Err(e) => {
            tracing::error!("recent matches query failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal" })),
            )
                .into_response();
        }
    };

    Json(PlayerProfile {
        player_id: player_id.clone(),
        display_name,
        primary_provider,
        created_at,
        identities,
        ratings,
        recent_matches,
    })
    .into_response()
}

/// Auth surface capabilities the demo uses to gate its login UI:
/// `providers` lists the registered login providers ("steam" when a public
/// origin is configured — OpenID needs an absolute callback — plus the
/// registry's provider ids), `dev_mode` when the test-token endpoint is
/// exposed.
pub async fn auth_config(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut providers = Vec::new();
    if state.config.public_url.is_some() {
        providers.push("steam");
    }
    if state.config.steam_backed_accounts_only {
        if state.auth_providers.get("discord").is_some() {
            providers.push("discord");
        }
    } else {
        providers.extend(state.auth_providers.ids());
    }
    Json(serde_json::json!({
        "ranked_queue_enabled": state.config.ranked_queue_enabled,
        "providers": providers,
        "dev_mode": state.config.auth_dev_mode && !state.config.steam_backed_accounts_only,
        "guest_login": !state.config.steam_backed_accounts_only,
    }))
}

/// Return TURN REST-auth credentials for a WebRTC peer connection.
/// 503 when LOBBY_TURN_SECRET is unset (host candidates only).
pub async fn turn_credentials(State(state): State<Arc<AppState>>) -> Response {
    let Some(secret) = &state.config.turn_secret else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "turn not configured"})),
        )
            .into_response();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Json(crate::turn::mint_turn_credentials(
        secret,
        3600,
        now,
        &state.config.turn_uris,
    ))
    .into_response()
}

/// Dev-only fake creator: returns a (simulated) server address and auto-reports
/// player_a's win 3s after allocation, exercising the full webhook path.
pub async fn mock_allocate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let callback = body["result_callback_url"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let winner = body["player_a"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_default();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let _ = state
            .http
            .post(&callback)
            .json(&serde_json::json!({ "winner": winner }))
            .send()
            .await;
    });
    Json(serde_json::json!({ "server_address": "127.0.0.1:25565", "join_token": "mock-join" }))
}
