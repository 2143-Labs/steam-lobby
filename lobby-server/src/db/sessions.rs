use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct LiveBrowserSession {
    pub session_id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub auth_provider: String,
    pub csrf_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct LiveNativeSession {
    pub session_id: uuid::Uuid,
    pub user_id: uuid::Uuid,
}

#[derive(Debug, Clone)]
pub struct OAuthLoginState {
    pub provider: String,
    pub return_to: String,
    pub code_verifier: Option<String>,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkIntentError {
    Expired,
    Replayed,
    SessionMismatch,
    SteamRequired,
}

pub fn opaque_token() -> String {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn token_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

impl PostgresStore {
    pub async fn create_browser_session(
        &self,
        user_id: uuid::Uuid,
        provider: &str,
        csrf_hash: &[u8],
        ttl_secs: u64,
    ) -> Result<uuid::Uuid> {
        let (id,) = sqlx::query_as::<_, (uuid::Uuid,)>(
            "INSERT INTO browser_sessions(user_id,auth_provider,csrf_hash,expires_at) VALUES($1,$2,$3,NOW()+($4::bigint * INTERVAL '1 second')) RETURNING session_id",
        ).bind(user_id).bind(provider).bind(csrf_hash).bind(ttl_secs as i64)
            .fetch_one(&self.pool).await.map_err(map_db_error)?;
        Ok(id)
    }
    pub async fn live_native_session_by_id(&self, session_id: uuid::Uuid) -> Result<Option<LiveNativeSession>> {
        sqlx::query_as::<_, (uuid::Uuid,uuid::Uuid)>(
            "SELECT session_id,user_id FROM native_sessions WHERE session_id=$1 AND revoked_at IS NULL AND expires_at>NOW()",
        ).bind(session_id).fetch_optional(&self.pool).await.map_err(map_db_error)
         .map(|r| r.map(|r| LiveNativeSession{session_id:r.0,user_id:r.1}))
    }


    pub async fn live_browser_session(&self, session_id: uuid::Uuid, user_id: uuid::Uuid) -> Result<Option<LiveBrowserSession>> {
        sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, String, Vec<u8>)>(
            "SELECT session_id,user_id,auth_provider,csrf_hash FROM browser_sessions WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW()",
        ).bind(session_id).bind(user_id).fetch_optional(&self.pool).await.map_err(map_db_error)
         .map(|r| r.map(|r| LiveBrowserSession { session_id:r.0,user_id:r.1,auth_provider:r.2,csrf_hash:r.3 }))
    }

    pub async fn touch_browser_session(&self, session_id: uuid::Uuid, user_id: uuid::Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE browser_sessions SET last_seen_at=NOW() WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW()")
            .bind(session_id).bind(user_id).execute(&self.pool).await.map_err(map_db_error)?.rows_affected()==1)
    }

    pub async fn revoke_browser_session(&self, session_id: uuid::Uuid, user_id: uuid::Uuid) -> Result<()> {
        sqlx::query("UPDATE browser_sessions SET revoked_at=COALESCE(revoked_at,NOW()) WHERE session_id=$1 AND user_id=$2")
            .bind(session_id).bind(user_id).execute(&self.pool).await.map_err(map_db_error)?; Ok(())
    }

    pub async fn create_native_session(&self, user_id: uuid::Uuid) -> Result<uuid::Uuid> {
        let (id,) = sqlx::query_as::<_, (uuid::Uuid,)>(
            "INSERT INTO native_sessions(user_id,expires_at) VALUES($1,NOW()+INTERVAL '1 hour') RETURNING session_id",
        ).bind(user_id).fetch_one(&self.pool).await.map_err(map_db_error)?; Ok(id)
    }

    pub async fn live_native_session(&self, session_id: uuid::Uuid, user_id: uuid::Uuid) -> Result<Option<LiveNativeSession>> {
        sqlx::query_as::<_, (uuid::Uuid,uuid::Uuid)>(
            "SELECT session_id,user_id FROM native_sessions WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW()",
        ).bind(session_id).bind(user_id).fetch_optional(&self.pool).await.map_err(map_db_error)
         .map(|r| r.map(|r| LiveNativeSession{session_id:r.0,user_id:r.1}))
    }

    pub async fn touch_native_session(&self, session_id: uuid::Uuid, user_id: uuid::Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE native_sessions SET last_seen_at=NOW() WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW()")
            .bind(session_id).bind(user_id).execute(&self.pool).await.map_err(map_db_error)?.rows_affected()==1)
    }

    pub async fn revoke_native_session(&self, session_id: uuid::Uuid, user_id: uuid::Uuid) -> Result<()> {
        sqlx::query("UPDATE native_sessions SET revoked_at=COALESCE(revoked_at,NOW()) WHERE session_id=$1 AND user_id=$2")
            .bind(session_id).bind(user_id).execute(&self.pool).await.map_err(map_db_error)?; Ok(())
    }

    pub async fn revoke_all_sessions(&self, user_id: uuid::Uuid) -> Result<()> {
        let mut tx=self.pool.begin().await.map_err(map_db_error)?;
        sqlx::query("UPDATE browser_sessions SET revoked_at=COALESCE(revoked_at,NOW()) WHERE user_id=$1").bind(user_id).execute(&mut *tx).await.map_err(map_db_error)?;
        sqlx::query("UPDATE native_sessions SET revoked_at=COALESCE(revoked_at,NOW()) WHERE user_id=$1").bind(user_id).execute(&mut *tx).await.map_err(map_db_error)?;
        sqlx::query("UPDATE users SET token_version=token_version+1 WHERE id=$1").bind(user_id).execute(&mut *tx).await.map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?; Ok(())
    }

    pub async fn revoke_provider_browser_sessions(&self, user_id: uuid::Uuid, provider: &str) -> Result<()> {
        sqlx::query("UPDATE browser_sessions SET revoked_at=COALESCE(revoked_at,NOW()) WHERE user_id=$1 AND auth_provider=$2")
            .bind(user_id).bind(provider).execute(&self.pool).await.map_err(map_db_error)?; Ok(())
    }

    pub async fn create_oauth_login_state(&self, state: &str, browser_nonce: &str, provider: &str, return_to: &str, verifier: Option<&str>) -> Result<()> {
        sqlx::query("INSERT INTO oauth_login_states(state_hash,browser_nonce_hash,provider,return_to,code_verifier) VALUES($1,$2,$3,$4,$5)")
            .bind(token_hash(state)).bind(token_hash(browser_nonce)).bind(provider).bind(return_to).bind(verifier)
            .execute(&self.pool).await.map_err(map_db_error)?; Ok(())
    }

    pub async fn consume_oauth_login_state(&self, state: &str, browser_nonce: &str, provider: &str) -> Result<Option<OAuthLoginState>> {
        let mut tx=self.pool.begin().await.map_err(map_db_error)?;
        let row=sqlx::query_as::<_,(String,String,Option<String>,Vec<u8>)>(
            "SELECT provider,return_to,code_verifier,browser_nonce_hash FROM oauth_login_states WHERE state_hash=$1 AND consumed_at IS NULL AND expires_at>NOW() FOR UPDATE",
        ).bind(token_hash(state)).fetch_optional(&mut *tx).await.map_err(map_db_error)?;
        let Some((stored_provider,return_to,code_verifier,nonce_hash))=row else { tx.rollback().await.map_err(map_db_error)?; return Ok(None) };
        if stored_provider!=provider || nonce_hash!=token_hash(browser_nonce) { tx.rollback().await.map_err(map_db_error)?; return Ok(None) }
        sqlx::query("UPDATE oauth_login_states SET consumed_at=NOW() WHERE state_hash=$1").bind(token_hash(state)).execute(&mut *tx).await.map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?;
        Ok(Some(OAuthLoginState{provider:stored_provider,return_to,code_verifier}))
    }

    pub async fn create_provider_proof(&self, provider_uid:&str, display_name:&str) -> Result<String> {
        let nonce=opaque_token();
        sqlx::query("INSERT INTO provider_sessions(nonce_hash,provider,provider_uid,display_name,expires_at) VALUES($1,'discord',$2,$3,NOW()+INTERVAL '5 minutes')")
            .bind(token_hash(&nonce)).bind(provider_uid).bind(display_name).execute(&self.pool).await.map_err(map_db_error)?;
        Ok(nonce)
    }

    pub async fn create_browser_link_intent(
        &self,
        user_id: uuid::Uuid,
        session_id: uuid::Uuid,
        proof: &str,
    ) -> Result<std::result::Result<(uuid::Uuid, String), LinkIntentError>> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        let live_session = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM browser_sessions WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW())",
        )
        .bind(session_id)
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_db_error)?;
        if !live_session {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err(LinkIntentError::SessionMismatch));
        }
        let has_steam = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE user_id=$1 AND provider='steam')",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_db_error)?;
        if !has_steam {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err(LinkIntentError::SteamRequired));
        }
        let proof_hash = token_hash(proof);
        let row = sqlx::query_as::<_, (String, bool, bool)>(
            "SELECT display_name,consumed_at IS NULL,expires_at>NOW() FROM provider_sessions WHERE nonce_hash=$1 AND provider='discord' FOR UPDATE",
        )
        .bind(&proof_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_db_error)?;
        let Some((name, unconsumed, live)) = row else {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err(LinkIntentError::Expired));
        };
        if !unconsumed {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err(LinkIntentError::Replayed));
        }
        if !live {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err(LinkIntentError::Expired));
        }
        let (id,) = sqlx::query_as::<_, (uuid::Uuid,)>(
            "INSERT INTO link_intents(user_id,kind,provider_session_hash,browser_session_id) VALUES($1,'browser',$2,$3) RETURNING id",
        )
        .bind(user_id)
        .bind(proof_hash)
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?;
        Ok(Ok((id, name)))
    }

    pub async fn create_oauth_handoff(&self, native_session_id: uuid::Uuid) -> Result<String> {
        let state = opaque_token();
        sqlx::query(
            "INSERT INTO oauth_handoffs(state_hash,provider,native_session_id) VALUES($1,'discord',$2)",
        )
        .bind(token_hash(&state))
        .bind(native_session_id)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(state)
    }

    pub async fn peek_oauth_handoff(&self, state: &str) -> Result<Option<uuid::Uuid>> {
        sqlx::query_scalar(
            "SELECT h.native_session_id FROM oauth_handoffs h JOIN native_sessions ns ON ns.session_id=h.native_session_id WHERE h.state_hash=$1 AND h.provider='discord' AND h.consumed_at IS NULL AND h.expires_at>NOW() AND ns.revoked_at IS NULL AND ns.expires_at>NOW()",
        )
        .bind(token_hash(state))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)
    }

    pub async fn complete_native_handoff(
        &self,
        state: &str,
        provider_uid: &str,
        display_name: &str,
    ) -> Result<Option<uuid::Uuid>> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        let state_hash = token_hash(state);
        let row = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid)>(
            "SELECT h.native_session_id,ns.user_id FROM oauth_handoffs h JOIN native_sessions ns ON ns.session_id=h.native_session_id WHERE h.state_hash=$1 AND h.provider='discord' AND h.consumed_at IS NULL AND h.expires_at>NOW() AND ns.revoked_at IS NULL AND ns.expires_at>NOW() FOR UPDATE OF h,ns",
        )
        .bind(&state_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_db_error)?;
        let Some((session_id, user_id)) = row else {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(None);
        };
        let proof = opaque_token();
        let proof_hash = token_hash(&proof);
        sqlx::query(
            "INSERT INTO provider_sessions(nonce_hash,provider,provider_uid,display_name,expires_at) VALUES($1,'discord',$2,$3,NOW()+INTERVAL '5 minutes')",
        )
        .bind(&proof_hash)
        .bind(provider_uid)
        .bind(display_name)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        let intent_id = sqlx::query_scalar(
            "INSERT INTO link_intents(user_id,kind,provider_session_hash,native_session_id) VALUES($1,'native',$2,$3) RETURNING id",
        )
        .bind(user_id)
        .bind(proof_hash)
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query("UPDATE oauth_handoffs SET consumed_at=NOW() WHERE state_hash=$1")
            .bind(state_hash)
            .execute(&mut *tx)
            .await
            .map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?;
        Ok(Some(intent_id))
    }


    pub async fn confirm_discord_link(
        &self,
        id: uuid::Uuid,
        user_id: uuid::Uuid,
        kind: &str,
        session_id: uuid::Uuid,
    ) -> Result<std::result::Result<(), &'static str>> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        let intent = sqlx::query_as::<_, (String, Option<uuid::Uuid>, Option<uuid::Uuid>, Vec<u8>, String, String, bool, bool, bool)>(
            "SELECT li.kind,li.browser_session_id,li.native_session_id,li.provider_session_hash,ps.provider_uid,li.status,li.expires_at>NOW(),ps.expires_at>NOW(),ps.consumed_at IS NULL FROM link_intents li JOIN provider_sessions ps ON ps.nonce_hash=li.provider_session_hash AND ps.provider='discord' WHERE li.id=$1 AND li.user_id=$2 FOR UPDATE OF li,ps",
        )
        .bind(id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_db_error)?;
        let Some((stored_kind, bsid, nsid, proof_hash, provider_uid, status, intent_live, proof_live, proof_unconsumed)) = intent else {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("session_mismatch"));
        };
        if status != "pending" || !proof_unconsumed {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("replayed"));
        }
        if !intent_live || !proof_live {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("expired"));
        }
        if stored_kind != kind
            || (kind == "browser" && bsid != Some(session_id))
            || (kind == "native" && nsid != Some(session_id))
        {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("session_mismatch"));
        }
        let session_live = match kind {
            "browser" => sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM browser_sessions WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW())"),
            "native" => sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM native_sessions WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW())"),
            _ => {
                tx.rollback().await.map_err(map_db_error)?;
                return Ok(Err("session_mismatch"));
            }
        }
        .bind(session_id)
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_db_error)?;
        if !session_live {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("session_mismatch"));
        }
        let steam = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE user_id=$1 AND provider='steam')",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_db_error)?;
        if !steam {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("steam_required"));
        }
        let owner = sqlx::query_as::<_, (uuid::Uuid,)>("SELECT user_id FROM accounts WHERE provider='discord' AND provider_uid=$1 FOR UPDATE")
            .bind(&provider_uid).fetch_optional(&mut *tx).await.map_err(map_db_error)?;
        let existing = sqlx::query_as::<_, (String,)>("SELECT provider_uid FROM accounts WHERE user_id=$1 AND provider='discord' FOR UPDATE")
            .bind(user_id).fetch_optional(&mut *tx).await.map_err(map_db_error)?;
        if owner.is_some_and(|r| r.0 != user_id)
            || existing.as_ref().is_some_and(|r| r.0 != provider_uid)
        {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(Err("ownership_conflict"));
        }
        if owner.is_none() {
            sqlx::query("SAVEPOINT discord_link_insert").execute(&mut *tx).await.map_err(map_db_error)?;
            let inserted = sqlx::query("INSERT INTO accounts(provider,provider_uid,user_id,last_login_at,linked_at) VALUES('discord',$1,$2,NOW(),NOW())")
                .bind(&provider_uid).bind(user_id).execute(&mut *tx).await;
            if inserted.is_err() {
                sqlx::query("ROLLBACK TO SAVEPOINT discord_link_insert").execute(&mut *tx).await.map_err(map_db_error)?;
                let canonical = sqlx::query_as::<_, (uuid::Uuid,)>("SELECT user_id FROM accounts WHERE provider='discord' AND provider_uid=$1 FOR UPDATE")
                    .bind(&provider_uid).fetch_optional(&mut *tx).await.map_err(map_db_error)?;
                let current = sqlx::query_as::<_, (String,)>("SELECT provider_uid FROM accounts WHERE user_id=$1 AND provider='discord' FOR UPDATE")
                    .bind(user_id).fetch_optional(&mut *tx).await.map_err(map_db_error)?;
                if canonical.is_none_or(|r| r.0 != user_id)
                    || current.is_none_or(|r| r.0 != provider_uid)
                {
                    tx.rollback().await.map_err(map_db_error)?;
                    return Ok(Err("ownership_conflict"));
                }
            }
        }
        sqlx::query("UPDATE provider_sessions SET consumed_at=NOW() WHERE nonce_hash=$1 AND consumed_at IS NULL")
            .bind(proof_hash).execute(&mut *tx).await.map_err(map_db_error)?;
        sqlx::query("UPDATE link_intents SET status='committed' WHERE id=$1 AND status='pending'")
            .bind(id).execute(&mut *tx).await.map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?;
        Ok(Ok(()))
    }

    pub async fn revoke_discord_link(&self,user_id:uuid::Uuid)->Result<()> {
        let mut tx=self.pool.begin().await.map_err(map_db_error)?;
        sqlx::query("DELETE FROM accounts WHERE user_id=$1 AND provider='discord'").bind(user_id).execute(&mut *tx).await.map_err(map_db_error)?;
        sqlx::query("UPDATE browser_sessions SET revoked_at=COALESCE(revoked_at,NOW()) WHERE user_id=$1 AND auth_provider='discord'").bind(user_id).execute(&mut *tx).await.map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?; Ok(())
    }
}
