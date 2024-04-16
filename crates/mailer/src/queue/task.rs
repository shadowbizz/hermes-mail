use crate::data::{Receiver, Sender, TemplateVariables};
use handlebars::RenderError;
use lettre::{
    address::AddressError,
    message::{
        header::{HeaderName, HeaderValue},
        MultiPart,
    },
    transport::smtp::{self, authentication::Credentials},
    Message, SmtpTransport, Transport,
};
use std::{sync::Arc, thread, thread::JoinHandle};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("could not build transport for task: {task:#?}; error: {err}")]
    TransportError { task: Task, err: smtp::Error },
    #[error("could not parse 'to'/'from' email for task: {task:#?}; error: {err}")]
    AddressError { task: Task, err: AddressError },
    #[error("could not render message for: {task:#?}; error: {err}")]
    RenderError { task: Task, err: RenderError },
    #[error("could build email message for: {task:#?}; error: {err}")]
    MessageBuildError {
        task: Task,
        err: lettre::error::Error,
    },
    #[error("send error for: {task:#?}; error: {err}")]
    SendError { task: Task, err: smtp::Error },
}

#[derive(Debug, Clone)]
pub struct Task {
    pub sender: Arc<Sender>,
    pub receiver: Arc<Receiver>,
}

pub type Result = core::result::Result<Task, Error>;

impl Task {
    pub(super) fn new(sender: Arc<Sender>, receiver: Arc<Receiver>) -> Self {
        Task { sender, receiver }
    }

    fn send(self) -> Result {
        let (sender, receiver, empty) = (
            self.clone().sender.clone(),
            self.clone().receiver.clone(),
            TemplateVariables::default(),
        );
        let transport = create_transport(sender.clone()).map_err(|err| Error::TransportError {
            task: self.clone().clone(),
            err,
        })?;

        let templates = sender.templates.as_ref().unwrap();
        let variables = receiver.variables.as_ref().unwrap_or(&empty);

        let mut builder = Message::builder()
            .from(sender.email.parse().map_err(|err| Error::AddressError {
                task: self.clone().clone(),
                err,
            })?)
            .to(receiver.email.parse().map_err(|err| Error::AddressError {
                task: self.clone(),
                err,
            })?)
            .subject(
                templates
                    .render("subject", &sender.subject)
                    .map_err(|err| Error::RenderError {
                        task: self.clone(),
                        err,
                    })?,
            );

        if let Some(cc) = receiver.cc.as_ref() {
            for mailbox in cc.iter() {
                builder = builder.cc(mailbox.to_owned());
            }
        }

        if let Some(bcc) = receiver.bcc.as_ref() {
            for mailbox in bcc.iter() {
                builder = builder.bcc(mailbox.to_owned());
            }
        }

        let plain = templates
            .render("text", variables)
            .map_err(|err| Error::RenderError {
                task: self.clone(),
                err,
            })?;
        let mut msg = if templates.has_template("html") {
            let html = templates
                .render("html", &variables)
                .map_err(|err| Error::RenderError {
                    task: self.clone(),
                    err,
                })?;
            builder
                .multipart(MultiPart::alternative_plain_html(plain, html))
                .map_err(|err| Error::MessageBuildError {
                    task: self.clone(),
                    err,
                })?
        } else {
            builder
                .body(plain)
                .map_err(|err| Error::MessageBuildError {
                    task: self.clone(),
                    err,
                })?
        };

        if let Some(read_receipts) = sender.read_receipt.as_ref() {
            set_header(
                &mut msg,
                "Disposition-Notification-To",
                read_receipts.clone(),
            );
            set_header(&mut msg, "Return-Receipt-To", read_receipts.clone());
        }
        set_header(&mut msg, "Reply-To", sender.email.clone());

        match transport.send(&msg) {
            Ok(_) => Ok(self),
            Err(err) => Err(Error::SendError { task: self, err }),
        }
    }

    pub(super) fn spawn(self) -> JoinHandle<Result> {
        thread::spawn(move || self.send())
    }
}

fn create_transport(sender: Arc<Sender>) -> core::result::Result<SmtpTransport, smtp::Error> {
    Ok(SmtpTransport::relay(&sender.host)?
        .credentials(Credentials::new(
            sender.email.clone(),
            sender.secret.clone(),
        ))
        .authentication(vec![sender.auth])
        .build())
}

fn set_header(msg: &mut Message, name: &'static str, value: String) {
    msg.headers_mut().insert_raw(HeaderValue::new(
        HeaderName::new_from_ascii_str(name),
        value,
    ))
}
