use crate::websocket::{self, Message};
use chrono::{Duration, Local};
use imap::Session;
use native_tls::TlsStream;
use serde::Deserialize;
use std::net::TcpStream;
use tracing::{error, warn};

type IMAPSession = Session<TlsStream<TcpStream>>;

#[derive(Clone, Debug, Deserialize)]
pub struct UnblockIMAPUser {
    domain: String,
    username: String,
    password: String,
}

impl Default for UnblockIMAPUser {
    fn default() -> Self {
        Self {
            domain: "".into(),
            username: "".into(),
            password: "".into(),
        }
    }
}

impl UnblockIMAPUser {
    pub fn new(domain: String, username: String, password: String) -> Self {
        Self {
            domain,
            username,
            password,
        }
    }

    fn imap_login(&self) -> Result<IMAPSession, Box<dyn std::error::Error>> {
        let tls = native_tls::TlsConnector::builder().build()?;
        let client = imap::connect((self.domain.as_str(), 993), &self.domain, &tls)?;

        Ok(client
            .login(&self.username, &self.password)
            .map_err(|(err, _)| err)?)
    }

    pub(crate) fn query_block_status(
        &self,
        senders: Vec<String>,
        inbound_tx: crossbeam_channel::Sender<websocket::Message>,
    ) {
        let timer = Local::now();
        let mut session: Option<IMAPSession> = None;

        loop {
            if Local::now().gt(&(timer + Duration::try_minutes(5).unwrap())) {
                if let Some(s) = session.as_mut() {
                    s.logout().unwrap_or_else(|e| {
                        warn!(msg = "IMAP logout failed", err = format!("{e}"))
                    });
                }
                session = None;
            }

            let _session = match session.as_mut() {
                Some(s) => s,
                None => match self.imap_login() {
                    Ok(s) => session.insert(s),
                    Err(err) => {
                        error!(msg = "imap login failed", err = format!("{err}"));
                        continue;
                    }
                },
            };

            for sender in senders.iter() {
                let res = match _session
                    .search(format!("HEADER BODY \"550\" HEADER FROM \"{}\"", sender))
                {
                    Ok(r) => r,
                    Err(err) => {
                        error!(msg = "IMAP search failed", err = format!("{err}"));
                        continue;
                    }
                };

                let query = res
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<String>>()
                    .join(" ");

                // Flag the read emails and delete them
                if let Err(err) = _session.store(query, "+FLAGS (\\Deleted)") {
                    error!(msg = "failed to flag emails", err = format!("{err}"));
                    continue;
                }

                if let Err(err) = _session.expunge() {
                    error!(msg = "failed to delete emails", err = format!("{err}"));
                    continue;
                }

                if !res.is_empty() {
                    let msg =
                        match Message::local_block("".into(), "".into(), sender.clone(), res.len())
                        {
                            Ok(m) => m,
                            Err(e) => {
                                error!(msg = "message creation err", err = format!("{e}"));
                                continue;
                            }
                        };

                    inbound_tx.send(msg).unwrap_or_else(|err| {
                        error!(
                            msg = "inbound block message send err",
                            err = format!("{err}")
                        )
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env::var, fs};

    use super::UnblockIMAPUser;

    #[test]
    fn test_imap() -> Result<(), Box<dyn std::error::Error>> {
        let test_data: UnblockIMAPUser =
            serde_json::from_str(&fs::read_to_string(var("USER_FILE")?)?)?;

        let mut session = test_data.imap_login()?;

        session.select("INBOX")?;

        let res = session.search(var("QUERY")?)?;

        println!("Got email matches: {res:?}");

        let query = res
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>()
            .join(" ");

        session.store(query, "+FLAGS (\\Deleted)")?;
        session.expunge()?;

        // be nice to the server and log out
        session.logout()?;

        Ok(())
    }
}
