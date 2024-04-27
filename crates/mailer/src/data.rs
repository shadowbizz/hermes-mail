use handlebars::{Handlebars, TemplateError};
use lettre::message::Mailboxes;
use lettre::transport::smtp::authentication::Mechanism;
use serde::de::DeserializeOwned;
use serde::Serializer;
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("for file: '{file}'; err: {err}")]
    IOError { file: PathBuf, err: io::Error },
    #[error("for source: '{src}'; err: {err}")]
    TemplateError { src: String, err: TemplateError },
    #[error("expected: key=value pairs for variables; got: {data}")]
    TemplateVariableParseError { data: String },
}

#[derive(Debug, Default, PartialEq)]
pub struct TemplateVariables(pub HashMap<String, String>);

impl FromStr for TemplateVariables {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            s.split(';')
                .map(|s| {
                    if let Some(pos) = s.find('=') {
                        let (key, val) = s.split_at(pos);
                        Ok((key.to_string(), val[1..val.len()].to_string()))
                    } else {
                        Err(Error::TemplateVariableParseError {
                            data: s.to_string(),
                        })
                    }
                })
                .collect::<Result<HashMap<String, String>, Error>>()?,
        ))
    }
}

impl Serialize for TemplateVariables {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(
            &self
                .0
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<String>>()
                .join(";"),
        )
    }
}

impl<'de> Deserialize<'de> for TemplateVariables {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(D::Error::custom)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Receiver {
    pub email: String,
    pub cc: Option<Mailboxes>,
    pub bcc: Option<Mailboxes>,
    pub sender: String,
    pub variables: Option<TemplateVariables>,
}

impl Default for Receiver {
    fn default() -> Self {
        Self {
            email: "".into(),
            sender: "".into(),
            cc: None,
            bcc: None,
            variables: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sender {
    pub email: String,
    pub secret: String,
    pub host: String,
    pub auth: Mechanism,
    pub subject: String,
    pub read_receipt: Option<String>,
    pub plain: PathBuf,
    pub html: Option<PathBuf>,
    #[serde(skip_serializing, skip_deserializing)]
    pub templates: Option<Handlebars<'static>>,
}

impl Default for Sender {
    fn default() -> Self {
        Self {
            email: "".into(),
            secret: "".into(),
            subject: "".into(),
            host: "".into(),
            auth: Mechanism::Plain,
            read_receipt: None,
            plain: PathBuf::new(),
            html: None,
            templates: None,
        }
    }
}

impl Sender {
    pub fn init_templates(&mut self) -> Result<(), Error> {
        let templates = self.templates.insert(Handlebars::new());
        templates
            .register_template_string("subject", &self.subject)
            .map_err(|err| Error::TemplateError {
                src: self.subject.clone(),
                err,
            })?;
        templates
            .register_template_file("plain", &self.plain)
            .map_err(|err| Error::TemplateError {
                src: self.plain.to_str().unwrap_or("plaintext file").into(),
                err,
            })?;

        if let Some(html) = self.html.as_ref() {
            let ext = html.extension().unwrap_or(OsStr::new("")).to_str().unwrap();
            match ext {
                "md" => templates
                    .register_template_string(
                        "html",
                        markdown::file_to_html(html).map_err(|err| Error::IOError {
                            file: html.clone(),
                            err,
                        })?,
                    )
                    .map_err(|err| Error::TemplateError {
                        src: self.plain.to_str().unwrap_or("plaintext file").into(),
                        err,
                    })?,

                "html" | "" | &_ => {
                    templates
                        .register_template_file("html", html)
                        .map_err(|err| Error::TemplateError {
                            src: self.plain.to_str().unwrap_or("plaintext file").into(),
                            err,
                        })?
                }
            }
        }

        Ok(())
    }
}

impl PartialEq for Sender {
    fn eq(&self, other: &Self) -> bool {
        if self.email != other.email {
            return false;
        }

        if self.secret != other.secret {
            return false;
        }

        if self.host != other.host {
            return false;
        }

        if self.auth != other.auth {
            return false;
        }

        if self.subject != other.subject {
            return false;
        }

        if self.plain != other.plain {
            return false;
        }

        if self.html != other.html {
            return false;
        }

        true
    }
}

pub fn read_input<D>(file: &PathBuf) -> Result<Vec<Arc<D>>, csv::Error>
where
    D: DeserializeOwned,
{
    let mut reader = csv::Reader::from_path(file)?;
    reader
        .deserialize()
        .map(|rec| match rec {
            Ok(r) => Ok(Arc::new(r)),
            Err(e) => Err(e),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_read_input() -> Result<(), csv::Error> {
        let senders = read_input::<Sender>(&"../../examples/senders.example.csv".parse().unwrap())?;

        assert_eq!(senders.len(), 10);
        assert_eq!(
            senders[2],
            Sender {
                email: "alexander@mail.com".into(),
                secret: "Password123".into(),
                host: "smtp.mail.com".into(),
                auth: Mechanism::Login,
                subject: "This is a test email for you {{name}}".into(),
                read_receipt: None,
                plain: "/home/sheke/Code/hermes/examples/text_message.example.txt"
                    .parse()
                    .unwrap(),
                html: Some(
                    "/home/sheke/Code/hermes/examples/html_message.example.html"
                        .parse()
                        .unwrap()
                ),
                templates: None
            }
            .into()
        );

        let receivers =
            read_input::<Receiver>(&"../../examples/receivers.example.csv".parse().unwrap())?;

        assert_eq!(receivers.len(), 11);
        assert_eq!(
            receivers[5],
            Receiver {
                email: "tom@example.com".into(),
                cc: Some(Mailboxes::from_str("mark@example.com,sarah@example.com").unwrap()),
                bcc: Some(Mailboxes::from_str("emma@example.com,john@example.com").unwrap()),
                sender: "sarah.wilson@example.com".into(),
                variables: Some(TemplateVariables(HashMap::from([
                    ("name".to_string(), "Tom".to_string()),
                    ("location".to_string(), "Berlin".to_string())
                ])))
            }
            .into()
        );

        Ok(())
    }
}
